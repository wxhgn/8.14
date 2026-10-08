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

#include <mysql/mysql.h>

using namespace std;

// ===================== 简化日志（替换 minilog.h） =====================
// 若你项目已有 minilog.h，删除下面这段，改为 #include "minilog.h"
enum class Level { DEBUG, INFO, WARN, ERROR };
namespace Minilog {
class Logger {
public:
    static Logger& getInstance() { static Logger l; return l; }
    void setFile(const string&) {}
    void setMinLevel(Level) {}
};
}
#define LOG_I(fmt, ...) do{ printf("[INFO ] " fmt "\n", ##__VA_ARGS__); }while(0)
#define LOG_W(fmt, ...) do{ printf("[WARN ] " fmt "\n", ##__VA_ARGS__); }while(0)
#define LOG_E(fmt, ...) do{ printf("[ERROR] " fmt "\n", ##__VA_ARGS__); }while(0)
// ======================================================================


// ===================== 宏配置 =====================
#define IDLE_TIMEOUT_SEC 60
#define MAX_EVENTS   1024
#define PORT         8888
#define THREAD_NUM   4
#define MAX_MSG_LEN  (64 * 1024)
#define MAX_SEND_BUF_SIZE (256*1024)
#define SEND_BUF_SOFT_LIMIT (64*1024)

atomic<uint64_t> g_total_recv_mgs{0};
atomic<int>      g_online_conn{0};
atomic<int>      g_business_queue_size{0};
chrono::steady_clock::time_point g_last_stat_print;
const int QUEUE_MAX_SIZE = 200;

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

std::thread::id g_main_thread_id;

inline void assert_is_main_thread() {
#ifndef NDEBUG
    assert(std::this_thread::get_id() == g_main_thread_id
           && "FATAL: 非主线程访问 IoContext！");
#endif
}


// ==================== IoContext ====================
using TimeoutItem = pair<chrono::steady_clock::time_point, int>;

class IoContext {
private:
    priority_queue<TimeoutItem, vector<TimeoutItem>, greater<TimeoutItem>> timeout_heap;
    unordered_map<int, vector<char>> fd_recv_buf;
    unordered_map<int, vector<char>> fd_send_buf;
    unordered_map<int, string>       online_clients;
    unordered_map<int, int>          fd_to_uid;
    unordered_map<int, uint64_t>     fd_to_session;
    uint64_t session_counter = 0;
    unordered_map<int, chrono::steady_clock::time_point> fd_last_active;
    unordered_map<int, string>       fd_to_name;
    unordered_set<int>               online_fd_set;

public:
    void update_fd_active(int fd) {
        assert_is_main_thread();
        auto now = chrono::steady_clock::now();
        timeout_heap.emplace(now + chrono::seconds(IDLE_TIMEOUT_SEC), fd);
        fd_last_active[fd] = now;
    }

    bool fd_alive(int fd) {
        assert_is_main_thread();
        return online_clients.find(fd) != online_clients.end();
    }

    void clean_fd(int fd) {
        assert_is_main_thread();
        fd_recv_buf.erase(fd);
        fd_send_buf.erase(fd);
        online_clients.erase(fd);
        fd_to_uid.erase(fd);
        fd_to_session.erase(fd);
        fd_to_name.erase(fd);
        fd_last_active.erase(fd);
        online_fd_set.erase(fd);
        g_online_conn.fetch_sub(1);
    }

    uint64_t add_new_conn(int fd, const string& ip) {
        assert_is_main_thread();
        fd_recv_buf[fd] = vector<char>();
        fd_send_buf[fd] = vector<char>();
        online_clients[fd] = ip;
        uint64_t sid = ++session_counter;
        fd_to_session[fd] = sid;
        g_online_conn.fetch_add(1);
        return sid;
    }

    uint64_t get_session(int fd) {
        assert_is_main_thread();
        auto it = fd_to_session.find(fd);
        if (it == fd_to_session.end()) return 0;
        return it->second;
    }

    string get_ip(int fd) {
        assert_is_main_thread();
        auto it = online_clients.find(fd);
        if (it == online_clients.end()) return "";
        return it->second;
    }

    void set_uid_and_name(int fd, int uid, const string& name) {
        assert_is_main_thread();
        fd_to_name[fd] = name;
        fd_to_uid[fd]  = uid;
        online_fd_set.insert(fd);
    }

    int get_uid(int fd) {
        assert_is_main_thread();
        auto it = fd_to_uid.find(fd);
        if (it == fd_to_uid.end()) return -1;
        return it->second;
    }

    string get_username(int fd) {
        assert_is_main_thread();
        auto it = fd_to_name.find(fd);
        if (it == fd_to_name.end()) return "";
        return it->second;
    }

    vector<int> get_all_fd() {
        assert_is_main_thread();
        vector<int> res;
        res.reserve(online_fd_set.size());
        for (auto& p : online_fd_set) res.push_back(p);
        return res;
    }

    vector<int> find_same_uid_fd(int skip_fd, int target_uid) {
        assert_is_main_thread();
        vector<int> to_kick;
        for (auto& p : fd_to_uid) {
            int fd = p.first, uid = p.second;
            if (fd != skip_fd && uid == target_uid) to_kick.push_back(fd);
        }
        return to_kick;
    }

    vector<char>& get_recv_buf(int fd) {
        assert_is_main_thread();
        return fd_recv_buf[fd];
    }

    vector<char>& get_send_buf(int fd) {
        assert_is_main_thread();
        return fd_send_buf[fd];
    }

    vector<int> handle_timeout() {
        assert_is_main_thread();
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
        assert_is_main_thread();
        vector<int> fds;
        for (auto& pair : online_clients) fds.push_back(pair.first);
        return fds;
    }
};

IoContext g_io_ctx;


// ==================== ReplyTask（必须先定义） ====================
struct ReplyTask {
    int      fd         = -1;
    uint64_t session_id = 0;
    string   reply_msg;
    int      uid        = -1;
    string   username;
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

    bool push(T val) {
        {
            lock_guard<mutex> lock(mtx);
            if ((int)q.size() >= max_size) return false;
            q.push(move(val));
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


// ==================== Message 基类（提前定义） ====================
class Message {
public:
    int      fd         = -1;
    uint64_t session_id = 0;
    string   ip;

    virtual void handle(MYSQL* conn) = 0;
    virtual ~Message() = default;
};


// ==================== 派生类声明 ====================
class LoginMsg : public Message {
public:
    string username;
    string password;
    void handle(MYSQL* conn) override;
};

class ChatMsg : public Message {
public:
    int    uid = -1;
    string username;
    string content;
    void handle(MYSQL* conn) override;
};

class LogoutMsg : public Message {
public:
    int uid = -1;
    void handle(MYSQL* conn) override;
};


// ==================== 全局队列（类型已齐） ====================
SafeQueue<unique_ptr<Message>> business_queue{QUEUE_MAX_SIZE};
SafeQueue<ReplyTask>           reply_queue{QUEUE_MAX_SIZE};
vector<thread>                 g_thread_pool;


// ==================== DB 函数声明（handle 实现里要用） ====================
int  db_login(MYSQL* conn, const string& req);
void db_insert_msg(MYSQL* conn, int sender_id, const string& msg);
void db_insert_online_log(MYSQL* conn, int user_id,
                          const string& ip, const string& event);


// ==================== handle 实现（此时队列、DB 声明都可见） ====================
void LoginMsg::handle(MYSQL* conn) {
    int uid = db_login(conn, username + ":" + password);

    ReplyTask rt;
    rt.fd         = this->fd;
    rt.session_id = this->session_id;
    rt.username   = this->username;

    if (uid != -1) {
        rt.uid       = uid;
        rt.reply_msg = string("login ok uid") + to_string(uid);
        db_insert_online_log(conn, uid, this->ip, "login");

        ReplyTask broadcast_online;
        broadcast_online.fd        = -1;
        broadcast_online.reply_msg = "2|" + this->username + "|";
        reply_queue.push(move(broadcast_online));
    } else {
        rt.reply_msg = "login fail";
    }
    reply_queue.push(move(rt));
}

void ChatMsg::handle(MYSQL* conn) {
    if (uid == -1) return;
    db_insert_msg(conn, uid, content);

    ReplyTask brd;
    brd.fd        = -1;
    brd.reply_msg = "1|" + username + "|" + content;
    reply_queue.push(move(brd));
}

void LogoutMsg::handle(MYSQL* conn) {
    if (uid <= 0) return;
    db_insert_online_log(conn, uid, ip, "logout");
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
                if (mysql_ping(conn) != 0) {
                    mysql_close(conn);
                    lock.lock();
                    m_cur_conn--;
                    continue;
                }
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
void broadcast(int sender_fd, const string& msg, int epfd);

vector<string> split(const string& s, char sep) {
    vector<string> tokens;
    stringstream ss(s);
    string tmp;
    while (getline(ss, tmp, sep)) {
        if (!tmp.empty()) tokens.push_back(tmp);
    }
    return tokens;
}

void sig_handler(int sig) {
    (void)sig;
    g_pool_running = false;
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

vector<char> pack_msg(const string& s) {
    assert_is_main_thread();
    vector<char> out;
    uint32_t bodylen = (uint32_t)s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

void clean_fd(int fd, int epfd) {
    assert_is_main_thread();
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    g_io_ctx.clean_fd(fd);
}

void sent_msg(int fd, const string& msg, int epfd) {
    assert_is_main_thread();
    if (!g_io_ctx.fd_alive(fd)) return;

    auto pack = pack_msg(msg);
    auto& buf = g_io_ctx.get_send_buf(fd);

    if (buf.size() + pack.size() > MAX_SEND_BUF_SIZE) {
        LOG_E("fd send buf overflow, close fd=%d", fd);
        // 注意：此处不直接清理，避免与调用方冲突；仅丢弃本次
        return;
    }
    if (buf.size() + pack.size() >= SEND_BUF_SOFT_LIMIT) {
        LOG_W("fd send_buf reach soft limit fd=%d", fd);
    }
    bool need_mod = buf.empty();
    buf.insert(buf.end(), pack.begin(), pack.end());
    if (need_mod) {
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events  = EPOLLIN | EPOLLET | EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
}

void broadcast(int sender_fd, const string& msg, int epfd) {
    vector<int> targets = g_io_ctx.get_all_fd();
    for (int fd : targets) {
        if (fd == sender_fd) continue;
        sent_msg(fd, msg, epfd);
    }
}

void clean_fd_with_log(int fd, int epfd) {
    assert_is_main_thread();
    int uid = g_io_ctx.get_uid(fd);
    string username = g_io_ctx.get_username(fd);

    if (uid != -1) {
        auto task = make_unique<LogoutMsg>();   // ✅ 补上 >
        task->fd         = fd;
        task->session_id = g_io_ctx.get_session(fd);
        task->ip         = g_io_ctx.get_ip(fd);
        task->uid        = uid;                 // ✅ 补上 uid

        bool push_ok = business_queue.push(move(task));
        if (push_ok) {
            g_business_queue_size = (int)business_queue.size();
        }

        string logout_body = "3|" + username + "|user logout";
        broadcast(-1, logout_body, epfd);
    }
    clean_fd(fd, epfd);
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

void db_insert_msg(MYSQL* conn, int sender_id, const string& msg) {
    vector<char> esc(msg.size() * 2 + 1, 0);
    mysql_real_escape_string(conn, esc.data(), msg.c_str(), msg.size());

    vector<char> sql(msg.size() * 2 + 256, 0);
    snprintf(sql.data(), sql.size(),
             "INSERT INTO chat_msg(sender_id,msg) VALUES(%d,'%s')",
             sender_id, esc.data());
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


// ==================== worker ====================
void worker() {
    while (g_pool_running.load()) {
        unique_ptr<Message> msg;                    // ✅ 正确类型
        if (!business_queue.pop(msg)) {
            if (!g_pool_running.load()) break;
            continue;
        }
        g_business_queue_size = (int)business_queue.size();

        MYSQL* conn = g_pool->get_conn(5000);
        if (conn == nullptr) {
            LOG_E("get conn fail, drop task");
            continue;
        }

        msg->handle(conn);                          // ✅ 多态调用

        g_pool->return_conn(conn);
    }
}


// ==================== main ====================
int main()
{
    signal(SIGINT, sig_handler);
    signal(SIGPIPE, SIG_IGN);
    g_main_thread_id = std::this_thread::get_id();

    // 若使用 minilog.h，请取消注释：
    // Minilog::getInstance().setFile("multi1.log");
    // Minilog::getInstance().setMinLevel(Level::DEBUG);

    g_pool = make_unique<MySQLConnPool>(DB_HOST, DB_USER, DB_PWD,
                                        DB_NAME, DB_PORT, 8);

    // ---- 监听 socket ----
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

    int epfd = epoll_create(MAX_EVENTS);
    if (epfd < 0) { perror("epoll_create"); close(listenfd); return -1; }

    struct epoll_event ev;
    ev.data.fd = listenfd;
    ev.events  = EPOLLIN | EPOLLET;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    // 启动 worker 线程池
    for (int i = 0; i < THREAD_NUM; i++) {
        g_thread_pool.emplace_back(worker);
    }

    struct epoll_event events[MAX_EVENTS];
    g_last_stat_print = chrono::steady_clock::now();
    uint64_t last_msg_cnt = 0;

    // ============ epoll 主循环 ============
    while (g_pool_running.load())
    {
        int nready = epoll_wait(epfd, events, MAX_EVENTS, 10);

        auto now = chrono::steady_clock::now();
        auto dur = chrono::duration_cast<chrono::milliseconds>(now - g_last_stat_print);
        if (dur.count() >= 10000) {
            uint64_t curr_msg = g_total_recv_mgs.load();
            double qps = calc_qps(last_msg_cnt, curr_msg, (int)dur.count());
            int online = g_online_conn.load();
            int q_len  = (int)business_queue.size();
            LOG_I("[MONITOR] online_conn=%d, msg_qps=%.2f, business_queue_len=%d, queue_max=%d",
                  online, qps, q_len, QUEUE_MAX_SIZE);
            last_msg_cnt = curr_msg;
            g_last_stat_print = now;
        }

        // 1. 空闲超时
        vector<int> timeout_fds = g_io_ctx.handle_timeout();
        for (int fd : timeout_fds) {
            clean_fd_with_log(fd, epfd);
        }

        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        // 2. 消费 worker 返回的 ReplyTask
        ReplyTask rt;
        while (reply_queue.try_pop(rt))
        {
            if (rt.fd == -1) {
                broadcast(-1, rt.reply_msg, epfd);
                continue;
            }
            uint64_t cur_sid = g_io_ctx.get_session(rt.fd);
            if (cur_sid == 0 || cur_sid != rt.session_id) {
                LOG_W("drop stale reply fd=%d (fd reused)", rt.fd);
                continue;
            }
            if (!g_io_ctx.fd_alive(rt.fd)) continue;

            if (rt.uid != -1) {
                int new_fd  = rt.fd;
                int new_uid = rt.uid;
                vector<int> to_kick = g_io_ctx.find_same_uid_fd(new_fd, new_uid);
                for (int kick_fd : to_kick) {
                    LOG_E("kick old fd=%d", kick_fd);
                    clean_fd_with_log(kick_fd, epfd);
                }
                g_io_ctx.set_uid_and_name(new_fd, new_uid, rt.username);
            }
            sent_msg(rt.fd, rt.reply_msg, epfd);
        }

        // 3. 处理 epoll 就绪事件
        for (int i = 0; i < nready; i++)
        {
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            // -------- 新连接 --------
            if (fd == listenfd) {
                if (revents & EPOLLIN) {
                    while (true) {
                        sockaddr_in cli_addr;
                        socklen_t cli_len = sizeof(cli_addr);
                        int connfd = accept4(listenfd, (sockaddr*)&cli_addr,
                                             &cli_len,
                                             SOCK_NONBLOCK | SOCK_CLOEXEC);
                        if (connfd == -1) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            perror("accept");
                            break;
                        }
                        string ip = inet_ntoa(cli_addr.sin_addr);
                        uint64_t sid = g_io_ctx.add_new_conn(connfd, ip);
                        g_io_ctx.update_fd_active(connfd);

                        LOG_I("新客户端接入 fd=%d session=%lu ip=%s",
                              connfd, (unsigned long)sid, ip.c_str());

                        struct epoll_event conn_ev;
                        conn_ev.data.fd = connfd;
                        conn_ev.events  = EPOLLIN | EPOLLET;
                        epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &conn_ev);
                    }
                }
                continue;
            }

            // -------- 读事件 --------
            if (revents & EPOLLIN) {
                if (!g_io_ctx.fd_alive(fd)) continue;

                char tem[1024];
                int n = 0;
                auto& recv_buf = g_io_ctx.get_recv_buf(fd);
                while ((n = recv(fd, tem, sizeof(tem), 0)) > 0) {
                    recv_buf.insert(recv_buf.end(), tem, tem + n);
                }
                if (n == 0) {
                    LOG_I("客户端下线 fd=%d ip=%s",
                          fd, g_io_ctx.get_ip(fd).c_str());
                    clean_fd_with_log(fd, epfd);
                    continue;
                }
                if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("recv");
                    LOG_I("客户端异常下线 fd=%d", fd);
                    clean_fd_with_log(fd, epfd);
                    continue;
                }
                g_io_ctx.update_fd_active(fd);

                bool need_close = false;
                // 拆包
                while (true) {
                    if (recv_buf.size() < 4) break;

                    uint32_t net_len;
                    memcpy(&net_len, recv_buf.data(), 4);
                    uint32_t body_len = ntohl(net_len);

                    if (body_len > MAX_MSG_LEN) {
                        LOG_W("body_len 超限 fd=%d", fd);
                        need_close = true;
                        break;
                    }
                    if (recv_buf.size() < 4 + body_len) break;

                    string msg(recv_buf.data() + 4,
                               recv_buf.data() + 4 + body_len);
                    recv_buf.erase(recv_buf.begin(),
                                   recv_buf.begin() + 4 + body_len);

                    g_total_recv_mgs.fetch_add(1);

                    auto parts = split(msg, '|');
                    if (parts.empty()) continue;
                    int msg_type = atoi(parts[0].c_str());

                    if (msg_type == 0) {              // 登录
                        if (parts.size() < 3) continue;
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
                            sent_msg(fd, "1|sys|server busy", epfd);
                        }
                        g_business_queue_size = (int)business_queue.size();
                    }
                    else if (msg_type == 1) {         // 聊天
                        int uid = g_io_ctx.get_uid(fd);
                        if (uid != -1) {
                            auto task = make_unique<ChatMsg>();
                            task->username   = parts[1];
                            task->fd         = fd;
                            task->uid        = uid;
                            task->session_id = g_io_ctx.get_session(fd);
                            task->content    = parts[2];    // ✅ content
                            bool ok = business_queue.push(move(task));
                            if (!ok) {
                                LOG_W("business queue full fd=%d", fd);
                                sent_msg(fd, "1|sys|server busy", epfd);
                            }
                            g_business_queue_size = (int)business_queue.size();
                        } else {
                            sent_msg(fd, "1|sys|please login first", epfd);
                        }
                    }
                    else if (msg_type == 4) {         // 心跳
                        LOG_I("recv ping fd=%d", fd);
                        g_io_ctx.update_fd_active(fd);
                        sent_msg(fd, "5|pong", epfd);
                    }
                    else {
                        sent_msg(fd, "unknown msg_type", epfd);
                    }
                }

                if (need_close) {
                    clean_fd_with_log(fd, epfd);
                    continue;
                }
            }

            // -------- 写事件 --------
            if (revents & EPOLLOUT) {
                if (!g_io_ctx.fd_alive(fd)) continue;

                auto& buf = g_io_ctx.get_send_buf(fd);
                if (buf.empty()) {
                    struct epoll_event ev_out;
                    ev_out.data.fd = fd;
                    ev_out.events  = EPOLLIN | EPOLLET;
                    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev_out);
                    continue;
                }

                bool closed = false;
                while (!buf.empty()) {
                    int ret = send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);
                    if (ret < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        LOG_E("send error fd=%d", fd);
                        clean_fd_with_log(fd, epfd);
                        closed = true;
                        break;
                    }
                    if (ret == 0) break;
                    buf.erase(buf.begin(), buf.begin() + ret);
                }
                if (closed) continue;

                if (buf.empty()) {
                    struct epoll_event ev_out;
                    ev_out.data.fd = fd;
                    ev_out.events  = EPOLLIN | EPOLLET;
                    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev_out);
                }
            }
        }
    }

    // ============ 优雅退出 ============
    g_pool_running = false;
    business_queue.notify_all();
    reply_queue.notify_all();
    for (auto& t : g_thread_pool) {
        if (t.joinable()) t.join();
    }

    vector<int> fds = g_io_ctx.get_all_online_fd();
    for (int fd : fds) clean_fd(fd, epfd);

    close(listenfd);
    close(epfd);
    g_pool.reset();
    return 0;
}