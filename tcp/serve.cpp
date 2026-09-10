#include<iostream>
#include <cstring>
 #include <unistd.h>
 #include <fcntl.h>
 #include <sys/socket.h>
 #include <arpa/inet.h>
 #include <sys/epoll.h>
 #include <errno.h>
 #include <unordered_map>
 #include <mysql/mysql.h>
 #define MAX_EVENTS 1024
 #define PORT 8888
 // 数据库连接参数
 const char* DB_HOST = "10.50.80.12";
 const char* DB_USER = "remote";
 const char* DB_PWD = "xjh123321"; // 修改为你自己的mysql密码
 const char* DB_NAME = "chat_db";
 unsigned int DB_PORT = 3306;
 MYSQL* mysql_conn = nullptr;
 // 工具函数：设置非阻塞
 int set_nonblock(int fd)
 {
     int flags = fcntl(fd, F_GETFL, 0);
     return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
 }
 // 自动建表函数
 bool mysql_create_table(MYSQL* conn)
 {
     // 1. 创建数据库
     const char* sql_create_db = "CREATE DATABASE IF NOT EXISTS chat_db";
     if(mysql_query(conn, sql_create_db) != 0)
     {
         std::cerr << "[DB] create db fail: " << mysql_error(conn) << std::endl;
         return false;
     }
     // 使用chat_db数据库
     if(mysql_select_db(conn, "chat_db") != 0)
     {
         std::cerr << "[DB] select db fail: " << mysql_error(conn) << std::endl;
         return false;
     }
     // 创建user表
     const char* sql_user = R"(
     CREATE TABLE IF NOT EXISTS user(
         id INT PRIMARY KEY AUTO_INCREMENT,
         username VARCHAR(32) NOT NULL UNIQUE,
         password VARCHAR(32) NOT NULL,
         create_time DATETIME DEFAULT NOW()
     )
     )";
     if(mysql_query(conn, sql_user) != 0)
     {
         std::cerr << "[DB] create user table fail: " << mysql_error(conn) << std::endl;
         return false;
     }
     // 创建chat_msg表
     const char* sql_msg = R"(
     CREATE TABLE IF NOT EXISTS chat_msg(
         id INT PRIMARY KEY AUTO_INCREMENT,
         sender_id INT NOT NULL,
         msg TEXT NOT NULL,
         send_time DATETIME DEFAULT NOW(),
         FOREIGN KEY(sender_id) REFERENCES user(id)
     )
     )";
     if(mysql_query(conn, sql_msg) != 0)
     {
         std::cerr << "[DB] create chat_msg table fail: " << mysql_error(conn) << std::endl;
         return false;
     }
     // 创建online_log表
     const char* sql_log = R"(
     CREATE TABLE IF NOT EXISTS online_log(
         id INT PRIMARY KEY AUTO_INCREMENT,
         user_id INT NOT NULL,
         ip VARCHAR(32),
         event ENUM('login','logout'),
         time DATETIME DEFAULT NOW(),
         FOREIGN KEY(user_id) REFERENCES user(id)
     )
     )";
     if(mysql_query(conn, sql_log) != 0)
     {
         std::cerr << "[DB] create online_log table fail: " << mysql_error(conn) << std::endl;
         return false;
     }
     // 插入测试用户，如果已经存在会报错，所以用INSERT IGNORE
     const char* sql_insert_test = "INSERT IGNORE INTO user(username,password) VALUES('user1','123456'),('user2','123456')";
     mysql_query(conn, sql_insert_test);
     std::cout << "[DB] 数据库&表初始化完成" << std::endl;
     return true;
 }
 // 初始化mysql连接
 bool mysql_init_db()
 {
     mysql_conn = mysql_init(nullptr);
     if(mysql_conn == nullptr)
     {
         std::cerr << "mysql init fail:" << mysql_error(mysql_conn) << std::endl;
         return false;
     }
     // 连接mysql服务，不指定数据库
     if(mysql_real_connect(mysql_conn, DB_HOST, DB_USER, DB_PWD, nullptr, DB_PORT, nullptr, 0) == nullptr)
     {
         std::cerr << "mysql connect fail:" << mysql_error(mysql_conn) << std::endl;
         return false;
     }
     mysql_set_character_set(mysql_conn, "utf8mb4");
     // 执行自动建表
     if(!mysql_create_table(mysql_conn))
     {
         return false;
     }
     std::cout << "[DB] mysql连接成功" << std::endl;
     return true;
 }

int db_login(const std::string& username, const std::string& password)
 {
     char sql[1024] = {0};
     snprintf(sql, sizeof(sql), "SELECT id FROM user WHERE username='%s' AND password='%s'",
              username.c_str(), password.c_str());
     if(mysql_query(mysql_conn, sql) != 0)
     {
         std::cerr << "[DB] query error:" << mysql_error(mysql_conn) << std::endl;
         return -1;
     }
     MYSQL_RES* res = mysql_store_result(mysql_conn);
     if(res == nullptr) return -1;
     MYSQL_ROW row = mysql_fetch_row(res);
     int uid = -1;
     if(row != nullptr)
     {
         uid = atoi(row[0]);
     }
     mysql_free_result(res);
     return uid;
 }
 // 插入聊天消息到chat_msg
 void db_insert_msg(int sender_id, const std::string& msg)
 {
     char sql[2048] = {0};
     char escape_msg[2048] = {0};
     mysql_real_escape_string(mysql_conn, escape_msg, msg.c_str(), msg.size());
     snprintf(sql, sizeof(sql), "INSERT INTO chat_msg(sender_id,msg) VALUES(%d,'%s')", sender_id, escape_msg);
     mysql_query(mysql_conn, sql);
 }
 // 插入上下线日志 online_log表
 void db_insert_online_log(int user_id, const std::string& ip, const std::string& event)
 {
     char sql[1024] = {0};
     snprintf(sql, sizeof(sql), "INSERT INTO online_log(user_id,ip,event) VALUES(%d,'%s','%s')",
              user_id, ip.c_str(), event.c_str());
     mysql_query(mysql_conn, sql);
 }
 // 广播函数
 void broadcast(int sender_fd, const std::string& msg, std::unordered_map<int, std::string>& online_clients, int epfd)
 {
     for(auto& pair : online_clients)
     {
         int fd = pair.first;
         if(fd == sender_fd) continue;
         ssize_t ret = send(fd, msg.c_str(), msg.size(), MSG_NOSIGNAL);
         if(ret < 0)
         {
             std::cout << "[LOG] 发送广播失败 fd=" << fd << std::endl;
             epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
             close(fd);
             online_clients.erase(fd);
         }
     }
 }
 // 内存映射
 std::unordered_map<int, std::string> online_clients;
 std::unordered_map<int, int> fd_to_uid;
 int main()
 {
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
     serv_addr.sin_family = AF_INET;
     serv_addr.sin_port = htons(PORT);
     serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
     bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr));
     listen(listenfd, 5);
     int epfd = epoll_create(MAX_EVENTS);
     struct epoll_event ev;
     ev.data.fd = listenfd;
     ev.events = EPOLLIN | EPOLLET;
     epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);
     struct epoll_event events[MAX_EVENTS];
     while(true)
     {
         int nready = epoll_wait(epfd, events, MAX_EVENTS, -1);
         if(nready < 0)
         {
             perror("epoll_wait");
             break;
         }
         for(int i = 0; i < nready; i++)
         {
             int fd = events[i].data.fd;
             if(fd == listenfd)
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
                     std::string client_ip = inet_ntoa(cli_addr.sin_addr);
                     std::cout << "[LOG] 新客户端接入 fd=" << connfd << " ip=" << client_ip << std::endl;
                     online_clients[connfd] = client_ip;
                   struct epoll_event conn_ev;
                     conn_ev.data.fd = connfd;
                     conn_ev.events = EPOLLIN | EPOLLET;
                     epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &conn_ev);
                 }
             }
             else
             {
                 char buf[1024] = {0};
                 while(true)
                 {
                     int n = recv(fd, buf, sizeof(buf)-1, 0);
                     if(n > 0)
                     {
                         std::string msg(buf, n);
                         std::cout << "[LOG] fd=" << fd << "收到消息: " << msg << std::endl;
                         if(msg.substr(0,6) == "login:")
                         {
                             size_t pos1 = msg.find(':', 6);
                             if(pos1 != std::string::npos)
                             {
                                 std::string user = msg.substr(6, pos1-6);
                                 std::string pwd = msg.substr(pos1+1);
                                 int uid = db_login(user, pwd);
                                 if(uid != -1)
                                 {
                                     fd_to_uid[fd] = uid;
                                     std::string resp = "login ok uid:" + std::to_string(uid);
                                     send(fd, resp.c_str(), resp.size(), MSG_NOSIGNAL);
                                     db_insert_online_log(uid, online_clients[fd], "login");
                                     std::cout << "[DB] 用户登录成功 uid=" << uid << std::endl;
                                 }
                                 else
                                 {
                                     send(fd, "login fail", 10, MSG_NOSIGNAL);
                                 }
                             }
                         }
                         else
                         {
                             auto it = fd_to_uid.find(fd);
                             if(it != fd_to_uid.end())
                             {
                                 int uid = it->second;
                                 db_insert_msg(uid, msg);
                                 broadcast(fd, msg, online_clients, epfd);
                             }
                             else
                             {
                                 send(fd, "please login first", 18, MSG_NOSIGNAL);
                             }
                         }
                         memset(buf,0,sizeof(buf));
                     }
                     else if(n == 0)
                     {
                         std::cout << "[LOG] 客户端下线 fd=" << fd << " ip=" << online_clients[fd] << std::endl;
                         auto it = fd_to_uid.find(fd);
                         if(it != fd_to_uid.end())
                         {
                             int uid = it->second;
                             db_insert_online_log(uid, online_clients[fd], "logout");
                             fd_to_uid.erase(it);
                         }
                         epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                         close(fd);
                         online_clients.erase(fd);
                         break;
                     }
                     else
                     {
                         if(errno == EAGAIN || errno == EWOULDBLOCK)
                         {
                             break;
                         }
                         perror("recv");
                         std::cout << "[LOG] 客户端异常下线 fd=" << fd << std::endl;
                         auto it = fd_to_uid.find(fd);
                         if(it != fd_to_uid.end())
                         {
                             int uid = it->second;
                             db_insert_online_log(uid, online_clients[fd], "logout");
                             fd_to_uid.erase(it);
                         }
                         epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                         close(fd);
                         online_clients.erase(fd);
                         break;
                     }
                 }
             }
         }
     }
     for(auto& pair : online_clients)
     {
         close(pair.first);
     }
     mysql_close(mysql_conn);
     close(listenfd);
     close(epfd);
     return 0;
 }