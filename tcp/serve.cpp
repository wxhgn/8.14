#include <iostream>
#include <cstring>
#include<future>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <signal.h>
#include <chrono>
#include <cassert>
#include <functional>
#include "buffer.h"
#include"minilog.h"
#include"TimerQueue.h"
#include <mysql/mysql.h>
#include "EventLoop.h"
#include"Channel.h"
#include"EpollPoller.h"
#include "EventLoopThreadPool.h"

using namespace std;

// ===================== 宏配置 =====================
#define IDLE_TIMEOUT_SEC 60
#define MAX_EVENTS   1024
#define PORT         8888
#define THREAD_NUM   4       // subReactor数量
#define MAX_MSG_LEN  (64 * 1024)
#define MAX_SEND_BUF_SIZE   (256*1024)
#define SEND_BUF_SOFT_LIMIT (64*1024)   // 高水位
#define SEND_BUF_LOW_LIMIT  (16*1024)   // 低水位
#define CONN_CACHE_MAX_CNT  500         // 单连接暂停时缓存消息上限

atomic<uint64_t> g_total_recv_mgs{0};
atomic<int>      g_online_conn{0};
atomic<int>      g_business_queue_size{0};
chrono::steady_clock::time_point g_last_stat_print;
const int QUEUE_MAX_SIZE = 200;
 EventLoop* g_mainLoop = nullptr;
void broadcast(int fd, const std::string& msg);
class Channel;
class ConnEvent;

double calc_qps(uint64_t old_cnt, uint64_t new_cnt, int ms) {
    if (ms <= 0) return 0.0;
    return (new_cnt - old_cnt) * 1000.0 / ms;
}

// MySQL 配置
const char* DB_HOST = "10.50.80.12";
const char* DB_USER = "remote";
const char* DB_PWD  = "xjh123321";
const char* DB_NAME = "chat_db";
unsigned int DB_PORT = 3306;

// ==================== IoContext 【改成线程安全，去掉主线程断言】 ====================
using TimeoutItem = pair<chrono::steady_clock::time_point, int>;
class IoContext {
private:
    priority_queue<TimeoutItem, vector<TimeoutItem>, greater<TimeoutItem>> timeout_heap;
    unordered_map<int, string>        online_clients;
    unordered_map<int, int>           fd_to_uid;
    unordered_map<int, uint64_t>      fd_to_session;
    uint64_t session_counter = 0;
    unordered_map<int, chrono::steady_clock::time_point> fd_last_active;
    unordered_map<int, string>        fd_to_name;
    unordered_set<int>                online_fd_set;
    std::mutex mtx_;
    
public:
    void update_fd_active(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto now = chrono::steady_clock::now();
        timeout_heap.emplace(now + chrono::seconds(IDLE_TIMEOUT_SEC), fd);
        fd_last_active[fd] = now;
    }

    bool fd_alive(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        return online_clients.find(fd) != online_clients.end();
    }

    void clean_fd(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        online_clients.erase(fd);
        fd_to_uid.erase(fd);
        fd_to_session.erase(fd);
        fd_to_name.erase(fd);
        fd_last_active.erase(fd);
        online_fd_set.erase(fd);
        g_online_conn.fetch_sub(1);
    }

    uint64_t add_new_conn(int fd, const string& ip) {
        std::lock_guard<std::mutex> lock(mtx_);
        online_clients[fd] = ip;
        uint64_t sid = ++session_counter;
        fd_to_session[fd] = sid;
        g_online_conn.fetch_add(1);
        return sid;
    }

    uint64_t get_session(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = fd_to_session.find(fd);
        if (it == fd_to_session.end()) return 0;
        return it->second;
    }

    string get_ip(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = online_clients.find(fd);
        if (it == online_clients.end()) return "";
        return it->second;
    }

    void set_uid_and_name(int fd, int uid, const string& name) {
        std::lock_guard<std::mutex> lock(mtx_);
        fd_to_name[fd] = name;
        fd_to_uid[fd]  = uid;
        online_fd_set.insert(fd);
    }

    int get_uid(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = fd_to_uid.find(fd);
        if (it == fd_to_uid.end()) return -1;
        return it->second;
    }

    string get_username(int fd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = fd_to_name.find(fd);
        if (it == fd_to_name.end()) return "";
        return it->second;
    }

    vector<int> get_all_fd() {
        std::lock_guard<std::mutex> lock(mtx_);
        vector<int> res;
        res.reserve(online_fd_set.size());
        for (auto& p : online_fd_set) res.push_back(p);
        return res;
    }//登录了的
   
    vector<int> find_same_uid_fd(int skip_fd, int target_uid) {
        std::lock_guard<std::mutex> lock(mtx_);
        vector<int> to_kick;
        for (auto& p : fd_to_uid) {
            int fd = p.first, uid = p.second;
            if (fd != skip_fd && uid == target_uid) to_kick.push_back(fd);
        }
        return to_kick;
    }

    vector<int> handle_timeout() {
        std::lock_guard<std::mutex> lock(mtx_);
        vector<int> need_close;
        unordered_set<int> seen;
        auto now = chrono::steady_clock::now();
        while (!timeout_heap.empty()) {
            auto top_item = timeout_heap.top();
            auto expire_time = top_item.first;
            int  fd          = top_item.second;
            if (expire_time > now) break;
            timeout_heap.pop();
            auto it = fd_last_active.find(fd);
            if (it == fd_last_active.end()) continue;
            auto real_time = it->second;
            if (chrono::duration_cast<chrono::seconds>(now - real_time).count()
                >= IDLE_TIMEOUT_SEC) {
                if (seen.insert(fd).second) {
                    LOG_I("idle timeout close fd=%d", fd);
                    need_close.push_back(fd);
                }
            }
        }
        return need_close;
    }

    vector<int> get_all_online_fd() {
        std::lock_guard<std::mutex> lock(mtx_);
        vector<int> fds;
        for (auto& pair : online_clients) fds.push_back(pair.first);
        return fds;
    }
     int find_fds_by_uid(int target_uid){
        lock_guard<mutex>lock(mtx_);
        vector<int>res;
        for(auto&p:fd_to_uid){
            if(p.second==target_uid)return p.first;
        }
        return -1;
    }
    vector<pair<int,string>>get_online_users(){
        lock_guard<mutex>lock(mtx_);
        vector<pair<int,string>>res;
        for(auto&p:fd_to_uid){
            int fd=p.first;
            int uid=p.second;
            auto it=fd_to_name.find(fd);
            string name=(it!=fd_to_name.end())?it->second:"";
            res.push_back({uid,name});
        }
        return res;
    }
};

IoContext g_io_ctx;

// ==================== muduo风格回调类型定义 ====================
using ConnectionCallback  = function<void(const shared_ptr<ConnEvent>&)>;
using MessageCallback     = function<void(const shared_ptr<ConnEvent>&, const string&)>;
using WriteCompleteCallback = function<void(const shared_ptr<ConnEvent>&)>;
using CloseCallback       = function<void(const shared_ptr<ConnEvent>&)>;
using HighWaterMarkCallback = function<void(const shared_ptr<ConnEvent>&, size_t)>;
using LowWaterMarkCallback  = function<void(const shared_ptr<ConnEvent>&)>;

// ==================== 前向声明 ====================
vector<char> pack_msg(const string& s);
void sent_msg(int fd, const string& msg, shared_ptr<ConnEvent> conn);

// ==================== ConnEvent 增加 ownerLoop_ 归属 ====================
class ConnEvent : public enable_shared_from_this<ConnEvent>
{
public:
    int fd_{-1};
    uint64_t session_id_{0};
    string ip_;
    Channel* channel_{nullptr};
    EventLoop* ownerLoop_{nullptr}; // 归属哪个subLoop
    
    Buffer recv_buf_;
    Buffer send_buf_;

    ConnectionCallback conn_cb_;
    MessageCallback msg_cb_;
    WriteCompleteCallback write_complete_cb_;
    CloseCallback close_cb_;
    HighWaterMarkCallback high_water_cb_;
    LowWaterMarkCallback  low_water_cb_;

    size_t high_water_mark_{SEND_BUF_SOFT_LIMIT};
    size_t low_water_mark_{SEND_BUF_LOW_LIMIT};

    bool pause_send_ = false;
    bool high_triggered_ = false;
    queue<string> cache_msg_queue_;

    ConnEvent(int fd, string ip, uint64_t sid, Channel* ch, EventLoop* loop)
        : fd_(fd),session_id_(sid),ip_(ip) ,channel_(ch), ownerLoop_(loop)
    {}

    void setConnectionCallback(ConnectionCallback cb) { conn_cb_ = move(cb); }
    void setMessageCallback(MessageCallback cb) { msg_cb_ = move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb) { write_complete_cb_ = move(cb); }
    void setCloseCallback(CloseCallback cb) { close_cb_ = move(cb); }
    void setHighWaterMarkCallback(HighWaterMarkCallback cb, size_t mark)
    {
        high_water_cb_ = move(cb);
        high_water_mark_ = mark;
    }
    void setLowWaterMarkCallback(LowWaterMarkCallback cb, size_t mark)
    {
        low_water_cb_ = move(cb);
        low_water_mark_ = mark;
    }
    
    void pauseSend()
    {
        pause_send_ = true;
        LOG_W("fd=%d pauseSend set", fd_);
    }

    void resumeSend()
    {
        pause_send_ = false;
        LOG_I("fd=%d resumeSend set", fd_);
        flushCacheMsg();
    }

    bool isPauseSend() const { return pause_send_; }

    void flushCacheMsg()
    {
        Buffer& send_buf = send_buf_;
        while (!cache_msg_queue_.empty())
        {
            const string& msg = cache_msg_queue_.front();
            auto pack = pack_msg(msg);
            if (send_buf.readableBytes() + pack.size() > MAX_SEND_BUF_SIZE)
            {
                LOG_W("fd=%d flush cache, send_buf full, stop flush", fd_);
                break;
            }
            send_buf.append(pack);
            cache_msg_queue_.pop();
        }
        checkWaterMark();
        if (send_buf.readableBytes() > 0)
        {
            channel_->enableReading();
            channel_->enableWriting();
            ownerLoop_->updateChannel(channel_);
        }
    }

    void checkWaterMark()
    {
        Buffer& buf = send_buf_;
        size_t len = buf.readableBytes();
        if (len >= high_water_mark_ && !high_triggered_)
        {
            high_triggered_ = true;
            if (high_water_cb_) high_water_cb_(shared_from_this(), len);
        }
        else if (len <= low_water_mark_ && high_triggered_)
        {
            high_triggered_ = false;
            if (low_water_cb_) low_water_cb_(shared_from_this());
        }
    }

    void handleRead()
    {
         if (ownerLoop_->isQuit()) return; 
        if(!g_io_ctx.fd_alive(fd_)){
            return;
        }

        int savedErrno = 0;
        Buffer& recv_buf = recv_buf_;

        ssize_t n = recv_buf.readFd(fd_, &savedErrno);
        if (n <= 0)
        {
            if (n == 0)
            {
                LOG_I("客户端下线 fd=%d ip=%s", fd_, g_io_ctx.get_ip(fd_).c_str());
                if (close_cb_) close_cb_(shared_from_this());
                return;
            }
            else
            {
                if (savedErrno != EAGAIN && savedErrno != EWOULDBLOCK)
                {
                    LOG_I("客户端异常下线 fd=%d errno=%d", fd_, savedErrno);
                    if (close_cb_) close_cb_(shared_from_this());
                }
                return;
            }
        }

        g_io_ctx.update_fd_active(fd_);
        bool need_close = false;

        while (recv_buf.readableBytes() >= 4)
        {
             if (ownerLoop_->isQuit()) break;   
            uint32_t net_len;
            ::memcpy(&net_len, recv_buf.peek(), 4);
            uint32_t body_len = ntohl(net_len);

            if (body_len > MAX_MSG_LEN)
            {
                LOG_W("body_len 超限 fd=%d", fd_);
                need_close = true;
                break;
            }
            if (recv_buf.readableBytes() < 4 + body_len)
            {
                break;
            }
            recv_buf.retrieve(4);
            string msg(recv_buf.peek(), body_len);
            recv_buf.retrieve(body_len);

            g_total_recv_mgs.fetch_add(1);
            if (msg_cb_)
            {
                msg_cb_(shared_from_this(), msg);
            }
        }

        if (need_close)
        {
            if (close_cb_) close_cb_(shared_from_this());
        }
    }
 
    void handleWrite()
    {
         if (ownerLoop_->isQuit()) return; 
        if(!g_io_ctx.fd_alive(fd_)){
            return;
        }

        Buffer& buf = send_buf_;
        if (buf.readableBytes() == 0)
        {
            channel_->enableReading();
            channel_->disableWriting();
            ownerLoop_->updateChannel(channel_);
            return;
        }

        bool closed = false;
        while (buf.readableBytes() > 0)
        {
                if (ownerLoop_->isQuit()) break;   // ★ 检查退出标志
            int ret = ::send(fd_, buf.peek(), buf.readableBytes(), MSG_NOSIGNAL);
            if (ret < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                LOG_E("send error fd=%d", fd_);
                if (close_cb_) close_cb_(shared_from_this());
                closed = true;
                break;
            }
            if (ret == 0) break;
            buf.retrieve(ret);
        }
        if (closed) return;

        checkWaterMark();

        if (buf.readableBytes() == 0)
        {
            channel_->enableReading();
            channel_->disableWriting();
            ownerLoop_->updateChannel(channel_);
            if (write_complete_cb_)
            {
                write_complete_cb_(shared_from_this());
            }
        }
    }
   
    void send(const string& msg);
};

// 每个loop自己维护连接map，这里简化：全局map（面试够用）
unordered_map<int, shared_ptr<ConnEvent>> g_fd_conn_map;
unordered_map<int, shared_ptr<Channel>> g_fd_channel_map;
std::mutex g_conn_map_mtx;
class BlacklistManager{
    private:
      mutex mtx_;
      unordered_map<int,unordered_set<int>>blacklist_;
      public:
      void load(int owner_uid,vector<int>&blocked_uids){
        lock_guard<mutex>lock(mtx_);
        auto&s=blacklist_[owner_uid];
        s.clear();
        for(int b:blocked_uids)s.insert(b);
      }
      void clear(int owner_uid){
        lock_guard<mutex>lock(mtx_);
        blacklist_.erase(owner_uid);
          }
      bool isBlocked(int owner_uid,int target_uid){
        lock_guard<mutex>lock(mtx_);
        auto it=blacklist_.find(owner_uid);
        if(it==blacklist_.end())return false;
        return it->second.count(target_uid)>0;
      }
      void add(int owner_uid,int blocked_uid){
        lock_guard<mutex>lock(mtx_);
        blacklist_[owner_uid].insert(blocked_uid);
      }
      void remove(int owner_uid,int blocked_uid){
        lock_guard<mutex>lock(mtx_);
        auto it=blacklist_.find(owner_uid);
      if(it!=blacklist_.end())
        it->second.erase(blocked_uid);
      }
      vector<int>getList(int owner_uid){
        lock_guard<mutex>lock(mtx_);
        vector<int>res;
        auto it=blacklist_.find(owner_uid);
        if(it!=blacklist_.end()){
            for(int b:it->second){
                res.push_back(b);
            }
        }
return res;
      }
};
BlacklistManager g_blacklist;
// ==================== ReplyTask ====================
struct ReplyTask {
    int         fd         = -1;
    uint64_t session_id = 0;
    string      reply_msg;
    int         uid        = -1;
    string      username;
};

// ==================== SafeQueue ====================
atomic<bool> g_pool_running{true};

template<typename T>
class SafeQueue {
private:
    queue<T> q;
    mutex mtx;
    condition_variable cv;
    int max_size;
public:
    explicit SafeQueue(int max_sz) : max_size(max_sz) {}

    bool push(T&&val) {
        {
            lock_guard<mutex> lock(mtx);
            if ((int)q.size() >= max_size) return false;
            q.emplace(std::forward<T>(val));
        }
        cv.notify_one();
        return true;
    }

    bool pop(T& out) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this] { return !q.empty() || !g_pool_running.load(); });
        if (q.empty()) return false;
        out = move(q.front());
        q.pop();
        return true;
    }

    bool try_pop(T& out) {
        lock_guard<mutex> lock(mtx);
        if (q.empty()) return false;
        out = move(q.front());
        q.pop();
        return true;
    }

    size_t size() {
        lock_guard<mutex> lock(mtx);
        return q.size();
    }

    void notify_all() { cv.notify_all(); }
};

// ==================== Message 基类 ====================
class Message {
public:
    int         fd         = -1;
    uint64_t session_id = 0;
    string      ip;

    virtual void handle(MYSQL* conn) = 0;
    virtual ~Message() = default;
};
class BlacklistMsg:public Message{
    public:
    int owner_uid=-1;
    int target_uid=-1;
    string op;
    void handle(MYSQL*conn)override;
};
class LoginMsg : public Message {
public:
    string username;
    string password;
    void handle(MYSQL* conn) override;
};

class ChatMsg : public Message {
public:
    int     uid = -1;
    string username;
    string content;
    void handle(MYSQL* conn) override;
};

class LogoutMsg : public Message {
public:
    int uid = -1;
    void handle(MYSQL* conn) override;
};

class PrivateChatMsg:public Message{
    public:
    int sender_uid=-1;
    string sender_name;
    int target_uid=-1;
    string content;
    void handle(MYSQL*conn)override;
};

SafeQueue<unique_ptr<Message>> business_queue{QUEUE_MAX_SIZE};
vector<thread>                 g_thread_pool;

// ==================== DB 函数声明 ====================
int  db_login(MYSQL* conn, const string& req);
void db_insert_msg(MYSQL* conn, int sender_id,int target_id,int msg_type, const string& msg,int is_read);
void db_insert_online_log(MYSQL* conn, int user_id,
                          const string& ip, const string& event);
struct OfflineMsg{
    int sender_uid;
    string sender_name;
    string content;
    string send_time;
};                       
vector<OfflineMsg> db_load_offline_msgs(MYSQL* conn, int target_uid);
void db_mark_msgs_read(MYSQL* conn, int target_uid);
void broadcast(int fd, const std::string& msg);
vector<int>db_load_blacklist(MYSQL*conn,int owner_uid);
bool db_add_blacklist(MYSQL*conn,int owner_uid,int blocked_uid);
bool db_remove_blacklist(MYSQL*conn,int owner_uid,int blocked_uid);
// 【重点改造：不再全局wakeup，拿到conn，交给它所属loop runInLoop】
void reply_dispatch(ReplyTask rt)
{
    if (rt.fd == -1) {
        broadcast(-1, rt.reply_msg);
        return;
    }
    uint64_t cur_sid = g_io_ctx.get_session(rt.fd);
    if (cur_sid == 0 || cur_sid != rt.session_id) {
        LOG_W("drop stale reply fd=%d (fd reused)", rt.fd);
        return;
    }
    if (!g_io_ctx.fd_alive(rt.fd)) return;

    std::lock_guard<std::mutex> lock(g_conn_map_mtx);
    auto it_conn = g_fd_conn_map.find(rt.fd);
    if (it_conn == g_fd_conn_map.end() || !it_conn->second) return;
    auto conn = it_conn->second;

    // 登录成功踢同账号其他设备
    if (rt.uid != -1) {
        int new_fd  = rt.fd;
        int new_uid = rt.uid;
        vector<int> to_kick = g_io_ctx.find_same_uid_fd(new_fd, new_uid);
        for (int kick_fd : to_kick) {
            LOG_E("kick old fd=%d", kick_fd);
            auto it = g_fd_conn_map.find(kick_fd);
            if (it != g_fd_conn_map.end() && it->second) {
                auto kick_conn=it->second;
                kick_conn->ownerLoop_->runInLoop([kick_conn](){
                if (kick_conn->close_cb_) kick_conn->close_cb_(kick_conn);
                });
            }
        }
        g_io_ctx.set_uid_and_name(new_fd, new_uid, rt.username);
    }

    // 投递到这个连接归属的subLoop线程执行send
    conn->ownerLoop_->runInLoop([conn, msg=rt.reply_msg](){
        conn->send(msg);
    });
}

// worker调用，直接dispatch
bool reply_push(ReplyTask&& rt)
{
    reply_dispatch(std::move(rt));
    return true;
}

// ==================== handle 实现 ====================
void LoginMsg::handle(MYSQL* conn) {
    int uid = db_login(conn, username + ":" + password);

    ReplyTask rt;
    rt.fd         = this->fd;
    rt.session_id = this->session_id;
    rt.username   = this->username;

    if (uid != -1) {
        rt.uid        = uid;
        rt.reply_msg = string("login ok uid") + to_string(uid);
        db_insert_online_log(conn, uid, this->ip, "login");
       auto blocked=db_load_blacklist(conn,uid);
        g_blacklist.load(uid,blocked);
        ReplyTask broadcast_online;
        broadcast_online.fd         = -1;
        broadcast_online.reply_msg = "2|" + this->username + "|"+"online";
        reply_push(move(broadcast_online));
        LOG_I("login ok fd=%d,uid=%d",rt.fd,uid);
        auto offline_msgs=db_load_offline_msgs(conn,uid);
        for(auto&m:offline_msgs){
            ReplyTask off_rt;
            off_rt.fd=this->fd;
            off_rt.session_id=this->session_id;
            off_rt.reply_msg="6|"+ to_string(m.sender_uid) + "|" +
                                m.sender_name + "|" + m.content;
            reply_push(move(off_rt));
        }
         db_mark_msgs_read(conn, uid);
    } else {
        rt.reply_msg = "login fail";
    }
    reply_push(move(rt));
}

void ChatMsg::handle(MYSQL* conn) {
    if (uid == -1) return;
    db_insert_msg(conn, uid, 0, 1, content,1);

    ReplyTask brd;
    brd.fd        = -1;
    brd.reply_msg = "1|" + username + "|" + content;
    reply_push(move(brd));
}

void LogoutMsg::handle(MYSQL* conn) {
    if (uid <= 0) return;
    db_insert_online_log(conn, uid, ip, "logout");
}

void PrivateChatMsg::handle(MYSQL*conn){
    if(sender_uid==-1||target_uid==-1){
        return;
    }
    // ★ 检查：target 是否拉黑了 sender
    if (g_blacklist.isBlocked(target_uid, sender_uid)) {
        ReplyTask rt;
        rt.fd         = fd;
        rt.session_id = session_id;
        rt.reply_msg  = "7|sys|you are blocked by this user";
        reply_push(move(rt));
        return;   // ★ 不落库，不推送
    }
 int tfd  = g_io_ctx.find_fds_by_uid(target_uid);
 int is_read=(tfd!=-1)?1:0;
    db_insert_msg(conn,sender_uid,target_uid,2,"[private to "+to_string(target_uid)+"]"+content,is_read);
   
    if(tfd==-1){
        ReplyTask rt;
        rt.fd=fd;
        rt.session_id=session_id;
        rt.reply_msg="7|sys|user"+to_string(target_uid)+"offline,saved";
        reply_push(move(rt));
        return;
    }
    ReplyTask rt;
    rt.fd=tfd;
    rt.session_id=g_io_ctx.get_session(tfd);
    rt.reply_msg="6|"+to_string(sender_uid)+"|"+sender_name+"|"+content;
    reply_push(move(rt));

    ReplyTask ack;
    ack.fd=fd;
    ack.session_id=session_id;
    ack.reply_msg="7|sys|send to "+to_string(target_uid);
    reply_push(move(ack));
}
void BlacklistMsg::handle(MYSQL*conn){
    if(owner_uid==-1||target_uid==-1)return;
    bool ok=false;
    if(op=="add"){
        ok=db_add_blacklist(conn,owner_uid,target_uid);
        if(ok)g_blacklist.add(owner_uid,target_uid);
    }else if(op=="remove"){
        ok=db_remove_blacklist(conn,owner_uid,target_uid);
        if(ok)g_blacklist.remove(owner_uid,target_uid);
    }
     ReplyTask rt;
    rt.fd         = fd;
    rt.session_id = session_id;
    rt.reply_msg  = ok ? ("11|ok|" + op + " " + to_string(target_uid))
                       : ("11|fail|" + op + " failed");
    reply_push(move(rt));
}
// ==================== MySQL 连接池 ====================
class MySQLConnPool {
private:
    queue<MYSQL*> m_conn_queue;
    mutex m_mtx;
    condition_variable m_cv;
    string m_host, m_user, m_pwd, m_db;
    unsigned int m_port;
    int m_max_conn;
    int m_cur_conn;

    MYSQL* create_new_conn() {
        MYSQL* conn = mysql_init(nullptr);
        if (!conn) { LOG_E("mysql_init fail"); return nullptr; }
          unsigned int timeout_sec = 5;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout_sec);
    mysql_options(conn, MYSQL_OPT_READ_TIMEOUT,    &timeout_sec);
    mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT,   &timeout_sec);
        if (!mysql_real_connect(conn, m_host.c_str(), m_user.c_str(),
                                m_pwd.c_str(), m_db.c_str(),
                                m_port, nullptr, 0)) {
            LOG_E("mysql_connect fail: %s", mysql_error(conn));
            mysql_close(conn);
            return nullptr;
        }
        mysql_set_character_set(conn, "utf8mb4");
        return conn;
    }

    MySQLConnPool(const MySQLConnPool&) = delete;
    MySQLConnPool& operator=(const MySQLConnPool&) = delete;

public:
    MySQLConnPool(string host, string user, string pwd, string db,
                  unsigned int port, int max_conn)
        : m_host(host), m_user(user), m_pwd(pwd), m_db(db),
          m_port(port), m_max_conn(max_conn), m_cur_conn(0)
    {
        if (max_conn <= 0) return;
        for (int i = 0; i < max_conn / 2; i++) {
            MYSQL* c = create_new_conn();
            if (c) { m_conn_queue.push(c); m_cur_conn++; }
        }
    }

    MYSQL* get_conn(int timeout_ms = 5000) {
        unique_lock<mutex> lock(m_mtx);
        auto deadline = chrono::steady_clock::now()
                        + chrono::milliseconds(timeout_ms);
        while (true) {
            if (!m_conn_queue.empty()) {
                MYSQL* conn = m_conn_queue.front();
                m_conn_queue.pop();
                lock.unlock();
                if (mysql_query(conn,"SELECT 1")!= 0) {
                    mysql_close(conn);
                    lock.lock();
                     m_cur_conn--;
                    m_cv.notify_one();
                   
                    continue;
                }
                MYSQL_RES*res=mysql_store_result(conn);
                if(res)mysql_free_result(res);
                return conn;
            }

            if (m_cur_conn < m_max_conn) {
                lock.unlock();
                MYSQL* new_conn = create_new_conn();
                if (new_conn) {
                    lock.lock();
                    m_cur_conn++;
                    return new_conn;
                }
                lock.lock();
            }

            if (m_cv.wait_until(lock, deadline) == cv_status::timeout) {
                LOG_E("get mysql conn timeout");
                return nullptr;
            }
        }
    }

    void return_conn(MYSQL* conn) {
        if (!conn) return;
        lock_guard<mutex> lock(m_mtx);
        m_conn_queue.push(conn);
        m_cv.notify_one();
    }

    ~MySQLConnPool() {
        lock_guard<mutex> lock(m_mtx);
        while (!m_conn_queue.empty()) {
            MYSQL* c = m_conn_queue.front();
            m_conn_queue.pop();
            mysql_close(c);
        }
    }
};

unique_ptr<MySQLConnPool> g_pool;

// ==================== 工具函数 ====================
void broadcast(int sender_fd, const string& msg);

vector<string> split(const string& s, char sep) {
    vector<string>res;
    size_t pos=0,prev=0;
    while(pos<s.size()){
        if(s[pos]==sep){
            res.push_back(s.substr(prev,pos-prev));
            prev=pos+1;
        }
        pos++;
    }
    res.push_back(s.substr(prev));
    return res;
}

void sig_handler(int sig) {
    (void)sig;
    g_pool_running = false;
      if (g_mainLoop) g_mainLoop->quit();
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

vector<char> pack_msg(const string& s) {
    vector<char> out;
    uint32_t bodylen = (uint32_t)s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

void clean_fd(int fd, Channel* ch) {
    g_io_ctx.clean_fd(fd);
    {
        std::lock_guard<std::mutex> lock(g_conn_map_mtx);
        g_fd_conn_map.erase(fd);
        g_fd_channel_map.erase(fd);
    }
    if(ch)
    {
        ch->ownerLoop()->removeChannel(ch);
    }
    close(fd);
}

void sent_msg(int fd, const string& msg, shared_ptr<ConnEvent> conn) {
    if (!g_io_ctx.fd_alive(fd)) return;

    if (conn->isPauseSend()) {
        if (conn->cache_msg_queue_.size() >= CONN_CACHE_MAX_CNT) {
            LOG_W("fd=%d cache queue overflow, drop msg", fd);
            return;
        }
        conn->cache_msg_queue_.push(msg);
        LOG_W("fd=%d paused, msg cache to queue, cache size=%zu",
              fd, conn->cache_msg_queue_.size());
        return;
    }

    auto pack = pack_msg(msg);
    Buffer& buf = conn->send_buf_;

    if (buf.readableBytes() + pack.size() > MAX_SEND_BUF_SIZE) {
        LOG_E("fd=%d send buf overflow, drop msg (no close)", fd);
        return;
    }

    bool need_mod = (buf.readableBytes() == 0);
    buf.append(pack);

    conn->checkWaterMark();

    if (need_mod) {
        conn->channel_->enableReading();
        conn->channel_->enableWriting();
        conn->ownerLoop_->updateChannel(conn->channel_);
    }
}

void ConnEvent::send(const string& msg)
{
    sent_msg(fd_, msg, shared_from_this());
}

void broadcast(int sender_fd, const string& msg) {
    vector<int> targets = g_io_ctx.get_all_fd();
    for (int fd : targets) {
        if (fd == sender_fd) continue;
        std::lock_guard<std::mutex> lock(g_conn_map_mtx);
        auto it = g_fd_conn_map.find(fd);
        if (it != g_fd_conn_map.end() && it->second) {
            auto c = it->second;
            c->ownerLoop_->runInLoop([c, msg](){
                c->send(msg);
            });
        }
    }
}

void clean_fd_with_log(int fd, Channel* ch) {
    int uid = g_io_ctx.get_uid(fd);
    string username = g_io_ctx.get_username(fd);

    if (uid != -1) {
        auto task = make_unique<LogoutMsg>();
        task->fd         = fd;
        task->session_id = g_io_ctx.get_session(fd);
        task->ip         = g_io_ctx.get_ip(fd);
        task->uid        = uid;

        bool push_ok = business_queue.push(move(task));
        if (push_ok) {
            g_business_queue_size = (int)business_queue.size();
        }

        string logout_body = "3|" + username + "|user logout";
        ReplyTask brd;
        brd.fd = -1;
        brd.reply_msg = logout_body;
        reply_push(move(brd));
    }
    clean_fd(fd, ch);
}

// ==================== 回调实现 ====================
void onConnection(const shared_ptr<ConnEvent>& conn)
{
    LOG_I("新客户端接入 fd=%d session=%lu ip=%s",
          conn->fd_, (unsigned long)conn->session_id_, conn->ip_.c_str());
}

void onMessage(const shared_ptr<ConnEvent>& conn, const string& pkg)
{
    int fd = conn->fd_;
    auto parts = split(pkg, '|');
    if (parts.empty()) return;
   
    int msg_type = atoi(parts[0].c_str());

    if (msg_type == 0) { // 登录
         if(parts.size()<3){
        conn->send("1|sys|bad format");
        return;
    }
        string user = parts[1];
        string pwd  = parts[2];

        auto task = make_unique<LoginMsg>();
        task->fd         = fd;
        task->username   = user;
        task->password   = pwd;
        task->session_id = g_io_ctx.get_session(fd);
        task->ip         = g_io_ctx.get_ip(fd);

        bool ok = business_queue.push(move(task));
        if (!ok) {
            LOG_W("business queue full fd=%d", fd);
            conn->send("1|sys|server busy");
        }
        g_business_queue_size = (int)business_queue.size();
    }
    else if (msg_type == 1) { // 聊天
         if(parts.size()<3){
        conn->send("1|sys|bad format");
        return;
    }
        int uid = g_io_ctx.get_uid(fd);
        if (uid != -1) {
            auto task = make_unique<ChatMsg>();
            task->username   = parts[1];
            task->fd         = fd;
            task->uid        = uid;
            task->session_id = g_io_ctx.get_session(fd);
            task->content    = parts[2];
            bool ok = business_queue.push(move(task));
            if (!ok) {
                LOG_W("business queue full fd=%d", fd);
                conn->send("1|sys|server busy");
            }
            g_business_queue_size = (int)business_queue.size();
        } else {
            conn->send("1|sys|please login first");
        }
    }
    else if (msg_type == 4) { // 心跳
        LOG_I("recv ping fd=%d", fd);
        g_io_ctx.update_fd_active(fd);
        conn->send("5|pong");
    }else if(msg_type==6){
         if(parts.size()<3){
        conn->send("1|sys|bad format");
        return;
    }
        int uid=g_io_ctx.get_uid(fd);
        if(uid==-1){
            conn->send("7|sys|please login first");
            return;
        }
        int target_uid=atoi(parts[1].c_str());
        if(target_uid<=0){
            conn->send("7|sys|bad target_uid");
            return;
        }
        if(target_uid==uid){
            conn->send("7|sys|cannot chat with yourself");
            return;
        }
        auto task=make_unique<PrivateChatMsg>();
         task->fd          = fd;
    task->session_id  = g_io_ctx.get_session(fd);
    task->ip          = g_io_ctx.get_ip(fd);
    task->sender_uid  = uid;
    task->sender_name = g_io_ctx.get_username(fd);
    task->target_uid  = target_uid;
    task->content     = parts[2];

    bool ok = business_queue.push(move(task));
    if(!ok){
        LOG_W("business queue full fd=%d",fd);
        conn->send("7|sys|server busy");
    }
       g_business_queue_size=(int)business_queue.size();
    }else if(msg_type==8){
      int uid=g_io_ctx.get_uid(fd);
      if(uid==-1){
        conn->send("8|sys|please login first");
        return;
      }
      auto users=g_io_ctx.get_online_users();
      string body="8|";
      for(size_t i=0;i<users.size();i++){
        if(i>0)body+=";";
        body+="uid="+to_string(users[i].first)+","+users[i].second;
      }
      conn->send(body);
    }else if(msg_type==11){
        int uid=g_io_ctx.get_uid(fd);
        if(uid==-1){
            conn->send("11|fail|please login first");
            return;
        }
        if(parts.size()<2){
            conn->send("11|fail|format error");
            return;
        }
        string op=parts[1];
        if(op=="add"||op=="remove"){
            if(parts.size()<3){
                conn->send("11|fail|bad target_uid.");
                return;
            }
            int target_uid=atoi(parts[2].c_str());
            if(target_uid<=0||target_uid==uid){
                conn->send("11|fail|bad target_uid");
                return;
            }
           auto task = make_unique<BlacklistMsg>();
        task->fd          = fd;
        task->session_id  = g_io_ctx.get_session(fd);
        task->ip          = g_io_ctx.get_ip(fd);
        task->owner_uid   = uid;
        task->target_uid  = target_uid;
        task->op          = op;

        bool ok = business_queue.push(move(task));
        if (!ok) {
            conn->send("11|fail|server busy");
        }
    }else if(op=="list"){
        auto list=g_blacklist.getList(uid);
        string body="11|list|";
        for(size_t i=0;i<list.size();i++){
            if(i>0)body+=",";
            body+=to_string(list[i]);
        }
        conn->send(body);
        }else{
            conn->send("11|fail|unknown op");
        }
    }else {
        conn->send("unknown msg_type");
    }
}

void onWriteComplete(const shared_ptr<ConnEvent>& conn)
{
    (void)conn;
}

void onClose(const shared_ptr<ConnEvent>& conn)
{
    LOG_I("连接关闭回调 fd=%d", conn->fd_);
    clean_fd_with_log(conn->fd_, conn->channel_);
}

void onHighWaterMark(const shared_ptr<ConnEvent>& conn, size_t len)
{
    LOG_W("HighWaterMark 告警 fd=%d send_buf=%zu bytes", conn->fd_, len);
    conn->pauseSend();
}

void onLowWaterMark(const shared_ptr<ConnEvent>& conn)
{
    LOG_I("LowWaterMark fd=%d send_buf down, resume send", conn->fd_);
    conn->resumeSend();
}

// ==================== DB 操作实现 ====================
int db_login(MYSQL* conn, const string& req) {
    size_t pos = req.find(':');
    if (pos == string::npos) return -1;
    string username = req.substr(0, pos);
    string password = req.substr(pos + 1);

    vector<char> user_esc(username.size() * 2 + 1, 0);
    vector<char> pwd_esc (password.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, user_esc.data(), username.c_str(), username.size());
    mysql_real_escape_string(conn, pwd_esc.data(),  password.c_str(), password.size());

    vector<char> sql(username.size() * 2 + password.size() * 2 + 128, 0);
    snprintf(sql.data(), sql.size(),
             "SELECT id FROM user WHERE username='%s' AND password='%s'",
             user_esc.data(), pwd_esc.data());

    if (mysql_query(conn, sql.data()) != 0) {
        LOG_E("query error: %s", mysql_error(conn));
        return -1;
    }

    MYSQL_RES* res = mysql_store_result(conn);
    if (!res) return -1;

    int uid = -1;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row) uid = atoi(row[0]);
    mysql_free_result(res);
    return uid;
}

void db_insert_msg(MYSQL* conn, int sender_id,int target_id,int msg_type,const string& msg,int is_read  ) {
    vector<char> esc(msg.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, esc.data(), msg.c_str(), msg.size());
  
    vector<char> sql(msg.size() * 2 + 256, 0);
    snprintf(sql.data(), sql.size(),
            "INSERT INTO chat_msg(sender_id,target_id,msg_type,msg,is_read) "
             "VALUES(%d,%d,%d,'%s',%d)",
             sender_id, target_id, msg_type, esc.data(), is_read);
    if (mysql_query(conn, sql.data()) != 0) {
        LOG_E("insert msg error: %s", mysql_error(conn));
    }
}

void db_insert_online_log(MYSQL* conn, int user_id,
                          const string& ip, const string& event) {
    vector<char> ip_esc(ip.size() * 2 + 1, 0);
    vector<char> ev_esc(event.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, ip_esc.data(), ip.c_str(), ip.size());
    mysql_real_escape_string(conn, ev_esc.data(), event.c_str(), event.size());

    vector<char> sql(ip.size() * 2 + event.size() * 2 + 128, 0);
    snprintf(sql.data(), sql.size(),
             "INSERT INTO online_log(user_id,ip,event) VALUES(%d,'%s','%s')",
             user_id, ip_esc.data(), ev_esc.data());
    if (mysql_query(conn, sql.data()) != 0) {
        LOG_E("insert log error: %s", mysql_error(conn));
    }
}


vector<OfflineMsg>db_load_offline_msgs(MYSQL*conn,int target_uid){
    vector<OfflineMsg>res;
    char sql[1024];
     snprintf(sql, sizeof(sql),
             "SELECT c.sender_id, u.username, c.msg, c.send_time "
             "FROM chat_msg c "
             "LEFT JOIN user u ON c.sender_id = u.id "
             "WHERE c.target_id=%d AND c.msg_type=2 AND c.is_read=0 "
             "ORDER BY c.send_time ASC "
             "LIMIT 1000",
             target_uid);
             if(mysql_query(conn,sql)!=0){
                LOG_E("load offline msgs error: %s",mysql_error(conn));
                return res;
             }
             MYSQL_RES*result=mysql_store_result(conn);
             if(!result)return res;
             MYSQL_ROW row;
             while((row=mysql_fetch_row(result))){
                OfflineMsg m;
                  m.sender_uid  = atoi(row[0]);
        m.sender_name = row[1] ? row[1] : ("uid" + to_string(m.sender_uid));
        m.content     = row[2] ? row[2] : "";
        m.send_time   = row[3] ? row[3] : "";
        res.push_back(m);
    }
    mysql_free_result(result);
    return res;
}
void db_mark_msgs_read(MYSQL* conn, int target_uid) {
    char sql[256];
    snprintf(sql, sizeof(sql),
             "UPDATE chat_msg SET is_read=1 "
             "WHERE target_id=%d AND msg_type=2 AND is_read=0",
             target_uid);
    if (mysql_query(conn, sql) != 0) {
        LOG_E("mark read error: %s", mysql_error(conn));
    }
}
vector<int>db_load_blacklist(MYSQL*conn,int owner_uid){
    vector<int>res;
    char sql[256];
    snprintf(sql,sizeof(sql),
    "SELECT blocked_uid FROM user_blocklist where owner_uid=%d",owner_uid);
    if(mysql_query(conn,sql)!=0){
        LOG_E("load blacklist error: %s",mysql_error(conn));
        return res;
    }
    MYSQL_RES*result=mysql_store_result(conn);
    if(!result)return res;
    MYSQL_ROW row;
    while((row=mysql_fetch_row(result))){
        res.push_back(atoi(row[0]));
    }
    mysql_free_result(result);
    return res;
}
bool db_add_blacklist(MYSQL*conn,int owner_uid,int blocked_uid){
    char sql[256];
    snprintf(sql,sizeof(sql),
       "insert ignore into user_blacklist(owner_uid,blocked_uid)"
       "values(%d,%d)",
       owner_uid,blocked_uid   
    );
    if(mysql_query(conn,sql)!=0){
        LOG_E("addblacklist error : %s",mysql_error(conn));
        return false;
    }
    return true;
}
bool db_remove_blacklist(MYSQL*conn,int owner_uid,int blocked_uid){
    char sql[256];
    snprintf(sql,sizeof(sql),
"delete from user_blacklist"
"where owner_uid=%d and blocked_uid=%d",
  owner_uid,blocked_uid);
  if(mysql_query(conn,sql)!=0){
    LOG_E("remove blacklist error:%s",mysql_error(conn));
    return false;
  }
  return true;
}
// ==================== worker ====================
void worker() {
    while (g_pool_running.load()) {
        unique_ptr<Message> msg;
        if (!business_queue.pop(msg)) {
            if (!g_pool_running.load()) break;
            continue;
        }
        g_business_queue_size = (int)business_queue.size();

        MYSQL* conn = g_pool->get_conn(5000);
        if (conn == nullptr) {
            ReplyTask rt;
            rt.fd = msg->fd;
            rt.session_id = msg->session_id;
            rt.reply_msg = "1|sys|db busy";
            reply_push(move(rt));
            continue;
        }
        
        msg->handle(conn);
        g_pool->return_conn(conn);
    }
}
#ifndef UNIT_TEST
// ==================== main：MainLoop，只负责accept！ ====================
int main()
{
    struct sigaction act_int{};
    act_int.sa_handler = sig_handler;
    sigemptyset(&act_int.sa_mask);
    act_int.sa_flags = 0;
    sigaction(SIGINT, &act_int, nullptr);

    struct sigaction act_pipe{};
    act_pipe.sa_handler = SIG_IGN;
    sigemptyset(&act_pipe.sa_mask);
    act_pipe.sa_flags = 0;
    sigaction(SIGPIPE, &act_pipe, nullptr);

    g_pool = make_unique<MySQLConnPool>(DB_HOST, DB_USER, DB_PWD,
                                        DB_NAME, DB_PORT, 8);

    // MainLoop：主Reactor
    EventLoop mainLoop;
    EventLoopThreadPool threadPool(&mainLoop, THREAD_NUM);
   
    g_mainLoop = &mainLoop;
    threadPool.start();
    
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) { perror("socket"); return -1; }
    set_nonblock(listenfd);

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serv_addr{};
    serv_addr.sin_family      = AF_INET;
    serv_addr.sin_port        = htons(PORT);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind"); close(listenfd); return -1;
    }
    if (listen(listenfd, 5) < 0) {
        perror("listen"); close(listenfd); return -1;
    }

    Channel listenCh(&mainLoop,listenfd);
    listenCh.enableReading();
    mainLoop.addChannel(&listenCh);

    // accept回调：MainLoop里面，拿到fd，分配给subLoop
    listenCh.setReadCallback([&](){
        while (true) {
            sockaddr_in cli_addr;
            socklen_t cli_len = sizeof(cli_addr);
            int connfd = accept4(listenfd, (sockaddr*)&cli_addr,
                                 &cli_len,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (connfd == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                 if (errno == EINTR) break;
                perror("accept");
                break;
            }
            string ip = inet_ntoa(cli_addr.sin_addr);
            uint64_t sid = g_io_ctx.add_new_conn(connfd, ip);
            g_io_ctx.update_fd_active(connfd);

            // 轮询拿到subLoop
            EventLoop* subLoop = threadPool.getNextLoop();
            // 所有连接创建逻辑，投递到subLoop线程执行
            subLoop->runInLoop([subLoop, connfd, ip, sid](){
                auto clientCh = make_shared<Channel>(subLoop,connfd);
                clientCh->enableReading();
                subLoop->addChannel(clientCh.get());
                auto conn = make_shared<ConnEvent>(connfd, ip, sid, clientCh.get(), subLoop);
                clientCh->setReadCallback([conn](){ conn->handleRead(); });
                clientCh->setWriteCallback([conn](){ conn->handleWrite(); });
                clientCh->setCloseCallback([conn](){
                    if (conn->close_cb_) conn->close_cb_(conn);
                });
                conn->setConnectionCallback(onConnection);
                conn->setMessageCallback(onMessage);
                conn->setWriteCompleteCallback(onWriteComplete);
                conn->setCloseCallback(onClose);
                conn->setHighWaterMarkCallback(onHighWaterMark, SEND_BUF_SOFT_LIMIT);
                conn->setLowWaterMarkCallback(onLowWaterMark, SEND_BUF_LOW_LIMIT);

                {
                    std::lock_guard<std::mutex> lock(g_conn_map_mtx);
                    g_fd_conn_map[connfd] = conn;
                    g_fd_channel_map[connfd] = clientCh;
                }
                if (conn->conn_cb_) conn->conn_cb_(conn);
            });
        }
    });

    // 启动业务worker线程池（数据库、消息处理）
    for (int i = 0; i < THREAD_NUM; i++) {
        g_thread_pool.emplace_back(worker);
    }

    // MainLoop跑起来，只负责accept
    mainLoop.loop();
    LOG_I("===mainLoop exited===");
    // 优雅退出
    g_pool_running = false;
    business_queue.notify_all();
    LOG_I("===notify_all done ===");
    threadPool.quitAll();
    LOG_I("=== quitAll done ====");
    // ★ join 加超时
std::future<void> join_future = std::async(std::launch::async, [&]() {
    for (auto& t : g_thread_pool) {
        if (t.joinable()) t.join();
    }
    LOG_I("===all workers joined ===");
});

if (join_future.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) {
    LOG_W("worker join timeout, force exit");
    quick_exit(0);   // 强制退出
}
  LOG_I("=== join success ===");
    close(listenfd);
    LOG_I("=== listenfd closed ===");
    g_pool.reset();
    LOG_I("=== g_pool reset ===");
    return 0;
}
#endif