#include <iostream>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <errno.h>
#include <unordered_map>
#include <vector>
#include <string>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <mysql/mysql.h>
#include "minilog.h"
#include<atomic>
#include<signal.h>
using namespace std;

#define MAX_EVENTS   1024
#define PORT         8888
#define MAX_MSG_LEN  (64 * 1024)
#define THREAD_NUM   4

const char* DB_HOST = "10.50.80.12";
const char* DB_USER = "remote";
const char* DB_PWD  = "xjh123321";
const char* DB_NAME = "chat_db";
unsigned int DB_PORT = 3306;

// ============ 只在主线程读写的全局表 ============
unordered_map<int, vector<char>> fd_recv_buf;
unordered_map<int, vector<char>> fd_send_buf;
unordered_map<int, string>       online_clients;
unordered_map<int, int>          fd_to_uid;

// ============ 任务结构 ============
struct BusinessTask {
    int fd  = -1;
    int uid = -1;
    string req_msg;   // 登录: "user:pwd"；普通消息: content
    string ip;
    string event;     // "login"/"logout"
    enum TaskType {
        TASK_LOGIN,
        TASK_INSERT_MSG,
        TASK_ONLINE_LOG
    } type;
};

struct ReplyTask {
    int fd = -1;
    string reply_msg;
    int uid = -1;     // -1 表示非登录任务
};

// ============ 全局运行标志 + 线程安全队列 ============
atomic<bool> g_pool_running = true;
template<typename T>
class SafeQueue {
private:
    queue<T> q;
    mutex mtx;
    condition_variable cv;
public:
    void push(T val) {
        {
            lock_guard<mutex> lock(mtx);
            q.push(move(val));
        }
        cv.notify_one();
    }

    // 阻塞等待（worker 用）
    bool pop(T& out) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this]{ return !q.empty() || !g_pool_running; });
        if(q.empty()) return false;
        out = move(q.front());
        q.pop();
        return true;
    }

    // 非阻塞（主线程用，处理 reply_queue）
    bool try_pop(T& out) {
        lock_guard<mutex> lock(mtx);
        if(q.empty()) return false;
        out = move(q.front());
        q.pop();
        return true;
    }

    void notify_all() {
        cv.notify_all();
    }
};
void sig_handler(int sig){
    (void)sig;
    g_pool_running=false;
}

SafeQueue<BusinessTask> business_queue;
SafeQueue<ReplyTask>    reply_queue;
vector<thread>          g_thread_pool;

// ============ 工具函数 ============
int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

vector<char> pack_mag(const string& s)
{
    vector<char> out;
    uint32_t bodylen = (uint32_t)s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

void clean_fd(int fd, int epfd)
{
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    fd_recv_buf.erase(fd);
    fd_send_buf.erase(fd);
    online_clients.erase(fd);
    fd_to_uid.erase(fd);
}

// 清理前先取 uid/ip，丢一条 logout 日志进线程池
void clean_fd_with_log(int fd, int epfd)
{
    auto it = fd_to_uid.find(fd);
    if(it != fd_to_uid.end())
    {
        BusinessTask task;
        task.type  = BusinessTask::TASK_ONLINE_LOG;
        task.uid   = it->second;
        task.ip    = online_clients.count(fd) ? online_clients[fd] : "";
        task.event = "logout";
        business_queue.push(move(task));
    }
    clean_fd(fd, epfd);
}

void sent_msg(int fd, const string& msg, int epfd)
{
    if(online_clients.find(fd) == online_clients.end()) return;

    auto pack = pack_mag(msg);
    fd_send_buf[fd].insert(fd_send_buf[fd].end(), pack.begin(), pack.end());

    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events  = EPOLLIN | EPOLLET | EPOLLOUT;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// ============ DB 操作（每个 worker 独立 conn） ============
int db_login(MYSQL* conn, const string& req)
{
    size_t pos = req.find(':');
    if(pos == string::npos) return -1;
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

    if(mysql_query(conn, sql.data()) != 0)
    {
        cerr << "[DB] query error:" << mysql_error(conn) << endl;
        return -1;
    }

    MYSQL_RES* res = mysql_store_result(conn);
    if(res == nullptr) return -1;

    int uid = -1;
    MYSQL_ROW row = mysql_fetch_row(res);
    if(row != nullptr) uid = atoi(row[0]);
    mysql_free_result(res);
    return uid;
}

void db_insert_msg(MYSQL* conn, int sender_id, const string& msg)
{
    vector<char> esc(msg.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, esc.data(), msg.c_str(), msg.size());

    vector<char> sql(msg.size() * 2 + 256, 0);
    snprintf(sql.data(), sql.size(),
             "INSERT INTO chat_msg(sender_id,msg) VALUES(%d,'%s')",
             sender_id, esc.data());
    mysql_query(conn, sql.data());
}

void db_insert_online_log(MYSQL* conn, int user_id, const string& ip, const string& event)
{
    vector<char> ip_esc(ip.size() * 2 + 1, 0);
    vector<char> ev_esc(event.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, ip_esc.data(), ip.c_str(), ip.size());
    mysql_real_escape_string(conn, ev_esc.data(), event.c_str(), event.size());

    vector<char> sql(ip.size() * 2 + event.size() * 2 + 128, 0);
    snprintf(sql.data(), sql.size(),
             "INSERT INTO online_log(user_id,ip,event) VALUES(%d,'%s','%s')",
             user_id, ip_esc.data(), ev_esc.data());
    mysql_query(conn, sql.data());
}

// ============ worker 线程 ============
void worker()
{
    MYSQL* conn = mysql_init(nullptr);
    if(conn == nullptr) {
        cerr << "[WORKER] mysql init fail" << endl;
        return;
    }
    if(mysql_real_connect(conn, DB_HOST, DB_USER, DB_PWD,
                          DB_NAME, DB_PORT, nullptr, 0) == nullptr)
    {
        cerr << "[WORKER] mysql connect fail:" << mysql_error(conn) << endl;
        mysql_close(conn);
        return;
    }
    mysql_set_character_set(conn, "utf8mb4");

    while(g_pool_running)
    {
        BusinessTask task;
        if(!business_queue.pop(task)) {
            if(!g_pool_running) break;
            continue;
        }

        if(task.type == BusinessTask::TASK_LOGIN)
        {
            int uid = db_login(conn, task.req_msg);

            ReplyTask rt;
            rt.fd = task.fd;
            if(uid != -1)
            {
                rt.uid       = uid;
                rt.reply_msg = "login ok uid:" + to_string(uid);
                db_insert_online_log(conn, uid, task.ip, "login");
            }
            else
            {
                rt.reply_msg = "login fail";
            }
            reply_queue.push(move(rt));
        }
        else if(task.type == BusinessTask::TASK_INSERT_MSG)
        {
            db_insert_msg(conn, task.uid, task.req_msg);
        }
        else if(task.type == BusinessTask::TASK_ONLINE_LOG)
        {
            db_insert_online_log(conn, task.uid, task.ip, task.event);
        }
    }

    mysql_close(conn);
}

// ============ 广播 ============
void broadcast(int sender_fd, const string& msg, int epfd)
{
    vector<int> targets;
    targets.reserve(fd_to_uid.size());
    for(auto& p : fd_to_uid)
    {
        if(p.first != sender_fd) targets.push_back(p.first);
    }
    for(int fd : targets)
    {
        sent_msg(fd, msg, epfd);
    }
}

int main()
{
    signal(SIGINT,sig_handler);
    Minilog::getInstance().setFile("multi1.log");
    Minilog::getInstance().setMinLevel(Level::INFO);

    // 启动线程池（MySQL 连接在 worker 内部建立）
    for(int i = 0; i < THREAD_NUM; i++)
    {
        g_thread_pool.emplace_back(worker);
    }

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0) { perror("socket"); return -1; }
    set_nonblock(listenfd);

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serv_addr{};
    serv_addr.sin_family      = AF_INET;
    serv_addr.sin_port        = htons(PORT);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if(bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    { perror("bind"); close(listenfd); return -1; }

    if(listen(listenfd, 5) < 0)
    { perror("listen"); close(listenfd); return -1; }

    int epfd = epoll_create(MAX_EVENTS);
    if(epfd < 0) { perror("epoll_create"); close(listenfd); return -1; }

    struct epoll_event ev;
    ev.data.fd = listenfd;
    ev.events  = EPOLLIN | EPOLLET;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[MAX_EVENTS];

    while(true)
    {
        // 超时 10ms，保证主线程能及时处理 reply_queue
        int nready = epoll_wait(epfd, events, MAX_EVENTS, 10);
        if(nready < 0)
        {
            if(errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        // ========= 处理 worker 返回的回复任务 =========
        ReplyTask rt;
        while(reply_queue.try_pop(rt))
        {
            // 关键：登录成功，由主线程写 fd <-> uid
            if(rt.uid != -1)
            {
                fd_to_uid[rt.fd] = rt.uid;
            }
            if(online_clients.count(rt.fd))
            {
                sent_msg(rt.fd, rt.reply_msg, epfd);
            }
        }

        for(int i = 0; i < nready; i++)
        {
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            // -------- 新连接 --------
            if(fd == listenfd)
            {
                if(revents & EPOLLIN)
                {
                    while(true)
                    {
                        sockaddr_in cli_addr;
                        socklen_t cli_len = sizeof(cli_addr);
                        int connfd = accept4(listenfd, (sockaddr*)&cli_addr, &cli_len,SOCK_NONBLOCK|SOCK_CLOEXEC);
                        if(connfd == -1)
                        {
                            if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                            perror("accept");
                            break;
                        }
                        set_nonblock(connfd);

                        fd_recv_buf[connfd] = vector<char>();
                        fd_send_buf[connfd] = vector<char>();
                        online_clients[connfd] = inet_ntoa(cli_addr.sin_addr);

                        cout << "[LOG] 新客户端接入 fd=" << connfd
                             << " ip=" << online_clients[connfd] << endl;

                        struct epoll_event conn_ev;
                        conn_ev.data.fd = connfd;
                        conn_ev.events  = EPOLLIN | EPOLLET;
                        epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &conn_ev);
                    }
                }
                continue;
            }

            // -------- 读事件 --------
            if(revents & EPOLLIN)
            {
                if(online_clients.find(fd) == online_clients.end())
                    continue;

                char tem[1024];
                int n = 0;
                while((n = recv(fd, tem, sizeof(tem), 0)) > 0)
                {
                    fd_recv_buf[fd].insert(fd_recv_buf[fd].end(), tem, tem + n);
                }

                if(n == 0)
                {
                    cout << "[LOG] 客户端下线 fd=" << fd
                         << " ip=" << online_clients[fd] << endl;
                    clean_fd_with_log(fd, epfd);
                    continue;
                }

                if(n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                {
                    perror("recv");
                    cout << "[LOG] 客户端异常下线 fd=" << fd << endl;
                    clean_fd_with_log(fd, epfd);
                    continue;
                }

                vector<char>& buf = fd_recv_buf[fd];
                bool need_close = false;
                while(true)
                {
                    if(buf.size() < 4) break;

                    uint32_t net_len;
                    memcpy(&net_len, buf.data(), 4);
                    uint32_t body_len = ntohl(net_len);

                    if(body_len > MAX_MSG_LEN)
                    {
                        cerr << "[WARN] body_len 超限 fd=" << fd << endl;
                        need_close = true;
                        break;
                    }

                    if(buf.size() < 4 + body_len) break;

                    string msg(buf.data() + 4, buf.data() + 4 + body_len);
                    buf.erase(buf.begin(), buf.begin() + 4 + body_len);

                    cout << "recv msg from fd=" << fd << ":" << msg << endl;

                    // ---- 登录：丢线程池 ----
                    if(msg.size() >= 6 && msg.substr(0, 6) == "login:")
                    {
                        size_t pos1 = msg.find(':', 6);
                        if(pos1 == string::npos) continue;

                        string user = msg.substr(6, pos1 - 6);
                        string pwd  = msg.substr(pos1 + 1);

                        BusinessTask task;
                        task.type    = BusinessTask::TASK_LOGIN;
                        task.fd      = fd;
                        task.req_msg = user + ":" + pwd;
                        task.ip      = online_clients[fd];
                        business_queue.push(move(task));
                    }
                    else
                    {
                        // ---- 普通消息 ----
                        auto it = fd_to_uid.find(fd);
                        if(it != fd_to_uid.end())
                        {
                            int uid = it->second;

                            BusinessTask task;
                            task.type    = BusinessTask::TASK_INSERT_MSG;
                            task.fd      = fd;
                            task.uid     = uid;
                            task.req_msg = msg;
                            business_queue.push(move(task));

                            broadcast(fd, msg, epfd);
                        }
                        else
                        {
                            sent_msg(fd, "please login first", epfd);
                        }
                    }
                }

                if(need_close)
                {
                    clean_fd_with_log(fd, epfd);
                    continue;
                }
            }

            // -------- 写事件 --------
            if(revents & EPOLLOUT)
            {
                if(online_clients.find(fd) == online_clients.end())
                    continue;

                auto& buf = fd_send_buf[fd];
                if(buf.empty())
                {
                    struct epoll_event ev_out;
                    ev_out.data.fd = fd;
                    ev_out.events  = EPOLLIN | EPOLLET;
                    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev_out);
                    continue;
                }
while(!buf.empty()){
                int ret = send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);
                if(ret < 0)
                {
                    if(errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;
                    cout << "send error fd=" << fd << endl;
                    clean_fd_with_log(fd, epfd);
                    continue;
                }

                buf.erase(buf.begin(), buf.begin() + ret);
            }
                if(buf.empty())
                {
                    struct epoll_event ev_out;
                    ev_out.data.fd = fd;
                    ev_out.events  = EPOLLIN | EPOLLET;
                    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev_out);
                }
            }
        }
    }

    // ========= 收尾 =========
    g_pool_running = false;
    business_queue.notify_all();
    reply_queue.notify_all();
    for(auto& t : g_thread_pool) t.join();
vector<int>ans;
    for(auto& pair : online_clients)
    {
       ans.push_back(pair.first);
    }
    for(int i=0;i<ans.size();i++){
        clean_fd(ans[i],epfd);
    }
    close(listenfd);
    close(epfd);
    return 0;
}