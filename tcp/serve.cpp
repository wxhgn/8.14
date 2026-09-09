#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <errno.h>

#define MAX_EVENTS 1024
#define PORT 8888

// 工具函数：把fd设置成非阻塞
int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main()
{
    // 1. 创建监听socket listenfd
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0)
    {
        perror("socket");
        return -1;
    }
    set_nonblock(listenfd); // 监听fd设置非阻塞

    // 端口复用，避免重启服务器端口被TIME_WAIT占用
    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
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

    // 2. 创建epoll实例
    int epfd = epoll_create(MAX_EVENTS);
    if(epfd < 0)
    {
        perror("epoll_create");
        close(listenfd);
        return -1;
    }

    struct epoll_event ev;
    ev.data.fd = listenfd;
    // ET边缘触发 EPOLLET
    ev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[MAX_EVENTS];

    // 主循环
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
                // 情况1：listenfd就绪，有新客户端连接
                while(true)
                {
                    sockaddr_in cli_addr;
                    socklen_t cli_len = sizeof(cli_addr);
                    int connfd = accept(listenfd, (sockaddr*)&cli_addr, &cli_len);
                    if(connfd == -1)
                    {
                        // EAGAIN 内核连接队列空了，退出循环
                        if(errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            break;
                        }
                        perror("accept");
                        break;
                    }
                    set_nonblock(connfd); // 新客户端fd设为非阻塞
                    std::cout << "新客户端接入: " << inet_ntoa(cli_addr.sin_addr) << std::endl;

                    // 注册connfd到epoll，ET模式监听可读
                    struct epoll_event conn_ev;
                    conn_ev.data.fd = connfd;
                    conn_ev.events = EPOLLIN | EPOLLET;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &conn_ev);
                }
            }
            else
            {
                // 情况2：客户端connfd就绪，收到消息
                char buf[1024] = {0};
                while(true)
                {
                    int n = recv(fd, buf, sizeof(buf)-1, 0);
                    if(n > 0)
                    {
                        std::cout << "收到消息: " << buf << std::endl;
                        send(fd, buf, n, 0); // echo回显，把消息发回去
                        memset(buf, 0, sizeof(buf));
                    }
                    else if(n == 0)
                    {
                        // n=0：客户端关闭连接
                        std::cout << "客户端断开: " << fd << std::endl;
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        break;
                    }
                    else
                    {
                        // n<0
                        if(errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            // 缓冲区全部读完，退出循环
                            break;
                        }
                        perror("recv");
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        break;
                    }
                }
            }
        }
    }
    close(listenfd);
    close(epfd);
    return 0;
}