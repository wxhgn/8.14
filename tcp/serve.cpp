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
#include <mysql/mysql.h>
#include "minilog.h"

using namespace std;

#define MAX_EVENTS   1024
#define PORT         8888
#define MAX_MSG_LEN  (64 * 1024)   // 单个消息体最大长度

// 数据库连接参数
const char* DB_HOST = "10.50.80.12";
const char* DB_USER = "remote";
const char* DB_PWD  = "xjh123321";
const char* DB_NAME = "chat_db";
unsigned int DB_PORT = 3306;

MYSQL* mysql_conn = nullptr;

unordered_map<int, vector<char>> fd_recv_buf;   // fd -> 接收缓冲
unordered_map<int, vector<char>> fd_send_buf;   // fd -> 发送缓冲
unordered_map<int, string>       online_clients; // fd -> ip
unordered_map<int, int>          fd_to_uid;      // fd -> uid

// 设置非阻塞
int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 打包：[4字节大端长度][body]
vector<char> pack_mag(const string& s)
{
    vector<char> out;
    uint32_t bodylen = (uint32_t)s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

// 清理一个连接（从 epoll 移除、关闭、清理映射）
void clean_fd(int fd, int epfd)
{
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    fd_recv_buf.erase(fd);
    fd_send_buf.erase(fd);
    online_clients.erase(fd);
    fd_to_uid.erase(fd);
}

// 把消息放进 fd 的发送缓冲，并注册 EPOLLOUT
void sent_msg(int fd, const string& msg, int epfd)
{
    // 已经掉线的 fd 就别发了
    if(online_clients.find(fd) == online_clients.end()) return;

    auto pack = pack_mag(msg);
    fd_send_buf[fd].insert(fd_send_buf[fd].end(), pack.begin(), pack.end());

    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events  = EPOLLIN | EPOLLET | EPOLLOUT;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// 初始化 mysql
bool mysql_init_db()
{
    mysql_conn = mysql_init(nullptr);
    if(mysql_conn == nullptr)
    {
        std::cerr << "mysql init fail" << std::endl;
        return false;
    }
    // 连接时直接指定数据库
    if(mysql_real_connect(mysql_conn, DB_HOST, DB_USER, DB_PWD,
                          DB_NAME, DB_PORT, nullptr, 0) == nullptr)
    {
        std::cerr << "mysql connect fail:" << mysql_error(mysql_conn) << std::endl;
        return false;
    }
    mysql_set_character_set(mysql_conn, "utf8mb4");
    return true;
}

// 登录
int db_login(const string& username, const string& password)
{
    char user_esc[256] = {0};
    char pwd_esc[256]  = {0};
    mysql_real_escape_string(mysql_conn, user_esc, username.c_str(), username.size());
    mysql_real_escape_string(mysql_conn, pwd_esc,  password.c_str(), password.size());

    char sql[1024] = {0};
    snprintf(sql, sizeof(sql),
             "SELECT id FROM user WHERE username='%s' AND password='%s'",
             user_esc, pwd_esc);

    if(mysql_query(mysql_conn, sql) != 0)
    {
        std::cerr << "[DB] query error:" << mysql_error(mysql_conn) << std::endl;
        return -1;
    }

    MYSQL_RES* res = mysql_store_result(mysql_conn);
    if(res == nullptr)
    {
        std::cerr << "[DB] store_result error:" << mysql_error(mysql_conn) << std::endl;
        return -1;
    }

    int uid = -1;
    MYSQL_ROW row = mysql_fetch_row(res);
    if(row != nullptr)
    {
        uid = atoi(row[0]);
    }
    mysql_free_result(res);
    return uid;
}

// 插入聊天消息
void db_insert_msg(int sender_id, const string& msg)
{
    // 转义长度最多是原来的 2 倍 + 1
    vector<char> esc(msg.size() * 2 + 1, 0);
    mysql_real_escape_string(mysql_conn, esc.data(), msg.c_str(), msg.size());

    vector<char> sql(msg.size() * 2 + 256, 0);
    snprintf(sql.data(), sql.size(),
             "INSERT INTO chat_msg(sender_id,msg) VALUES(%d,'%s')",
             sender_id, esc.data());
    mysql_query(mysql_conn, sql.data());
}

// 插入上下线日志
void db_insert_online_log(int user_id, const string& ip, const string& event)
{
    char ip_esc[128]    = {0};
    char event_esc[64]  = {0};
    mysql_real_escape_string(mysql_conn, ip_esc,    ip.c_str(),    ip.size());
    mysql_real_escape_string(mysql_conn, event_esc, event.c_str(), event.size());

    char sql[512] = {0};
    snprintf(sql, sizeof(sql),
             "INSERT INTO online_log(user_id,ip,event) VALUES(%d,'%s','%s')",
             user_id, ip_esc, event_esc);
    mysql_query(mysql_conn, sql);
}

// 广播：只发给已登录用户
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
    Minilog::getInstance().setFile("multi1.log");
    Minilog::getInstance().setMinLevel(Level::INFO);

    if(!mysql_init_db())
    {
        return -1;
    }

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0)
    {
        perror("socket");
        return -1;
    }
    set_nonblock(listenfd);

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serv_addr{};
    serv_addr.sin_family      = AF_INET;
    serv_addr.sin_port        = htons(PORT);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if(bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("bind");
        close(listenfd);
        return -1;
    }

    if(listen(listenfd, 5) < 0)
    {
        perror("listen");
        close(listenfd);
        return -1;
    }

    int epfd = epoll_create(MAX_EVENTS);
    if(epfd < 0)
    {
        perror("epoll_create");
        close(listenfd);
        return -1;
    }

    struct epoll_event ev;
    ev.data.fd = listenfd;
    ev.events  = EPOLLIN | EPOLLET;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[MAX_EVENTS];

    while(true)
    {
        int nready = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if(nready < 0)
        {
            if(errno == EINTR) continue;
            perror("epoll_wait");
            break;
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
                        int connfd = accept(listenfd, (sockaddr*)&cli_addr, &cli_len);
                        if(connfd == -1)
                        {
                            if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                            perror("accept");
                            break;
                        }
                        set_nonblock(connfd);

                        fd_recv_buf[connfd] = vector<char>();
                        fd_send_buf[connfd] = vector<char>();

                        string client_ip = inet_ntoa(cli_addr.sin_addr);
                        online_clients[connfd] = client_ip;

                        cout << "[LOG] 新客户端接入 fd=" << connfd
                             << " ip=" << client_ip << endl;

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
                // 如果已经被 clean 过（比如同一轮里先被 EPOLLOUT 清理），跳过
                if(online_clients.find(fd) == online_clients.end())
                    continue;

                char tem[1024];
                int n = 0;
                while((n = recv(fd, tem, sizeof(tem), 0)) > 0)
                {
                    fd_recv_buf[fd].insert(fd_recv_buf[fd].end(), tem, tem + n);
                }

                // 对端关闭
                if(n == 0)
                {
                    cout << "[LOG] 客户端下线 fd=" << fd
                         << " ip=" << online_clients[fd] << endl;
                    auto it = fd_to_uid.find(fd);
                    if(it != fd_to_uid.end())
                    {
                        db_insert_online_log(it->second, online_clients[fd], "logout");
                    }
                    clean_fd(fd, epfd);
                    continue;
                }

                // 真正的错误
                if(n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                {
                    perror("recv");
                    cout << "[LOG] 客户端异常下线 fd=" << fd << endl;
                    auto it = fd_to_uid.find(fd);
                    if(it != fd_to_uid.end())
                    {
                        db_insert_online_log(it->second, online_clients[fd], "logout");
                    }
                    clean_fd(fd, epfd);
                    continue;
                }

                // 走到这里说明是 EAGAIN，数据收完了，开始解析
                vector<char>& buf = fd_recv_buf[fd];

                bool need_close = false;
                while(true)
                {
                    if(buf.size() < 4) break;

                    uint32_t net_len;
                    memcpy(&net_len, buf.data(), 4);
                    uint32_t body_len = ntohl(net_len);

                    // 长度上限，防止恶意包撑爆内存
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

                    // ---- 登录 ----
                    if(msg.substr(0, 6) == "login:")
                    {
                        size_t pos1 = msg.find(':', 6);
                        if(pos1 == string::npos) continue;

                        string user = msg.substr(6, pos1 - 6);
                        string pwd  = msg.substr(pos1 + 1);

                        int uid = db_login(user, pwd);
                        if(uid != -1)
                        {
                            fd_to_uid[fd] = uid;
                            string resp = "login ok uid:" + to_string(uid);
                            sent_msg(fd, resp, epfd);
                            db_insert_online_log(uid, online_clients[fd], "login");
                            cout << "[DB] 用户登录成功 uid=" << uid << endl;
                        }
                        else
                        {
                            sent_msg(fd, "login fail", epfd);
                        }
                    }
                    else
                    {
                        // ---- 普通消息 ----
                        auto it = fd_to_uid.find(fd);
                        if(it != fd_to_uid.end())
                        {
                            int uid = it->second;
                            db_insert_msg(uid, msg);
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
                    auto it = fd_to_uid.find(fd);
                    if(it != fd_to_uid.end())
                    {
                        db_insert_online_log(it->second, online_clients[fd], "logout");
                    }
                    clean_fd(fd, epfd);
                    continue;
                }
            }

            // -------- 写事件 --------
            if(revents & EPOLLOUT)
            {
                // fd 可能在 EPOLLIN 那一段已经被清掉了
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

                int ret = send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);
                if(ret < 0)
                {
                    if(errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        continue;   // 关键：不要 break，不要丢其他事件
                    }
                    cout << "send error fd=" << fd << endl;
                    auto it = fd_to_uid.find(fd);
                    if(it != fd_to_uid.end())
                    {
                        db_insert_online_log(it->second, online_clients[fd], "logout");
                    }
                    clean_fd(fd, epfd);
                    continue;
                }

                buf.erase(buf.begin(), buf.begin() + ret);

                // 发完了，去掉 EPOLLOUT
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

    // 收尾
    for(auto& pair : online_clients)
    {
        close(pair.first);
    }
    mysql_close(mysql_conn);
    close(listenfd);
    close(epfd);
    return 0;
}