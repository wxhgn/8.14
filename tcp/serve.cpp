#include <iostream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

// 设置fd为非阻塞
int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if(flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main()
{
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblock(listenfd); //监听fd也设置非阻塞

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8888);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    bind(listenfd, (sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(listenfd, 5);

    fd_set readfds;
    std::vector<int> client_fds;

    while(true)
    {
        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);

        int maxfd = listenfd;
        for(int fd : client_fds)
        {
            FD_SET(fd, &readfds);
            if(fd > maxfd) maxfd = fd;
        }

        // select阻塞等待事件
        int ret = select(maxfd + 1, &readfds, nullptr, nullptr, nullptr);
        if(ret < 0)
        {
            perror("select");
            break;
        }

        // 1. listenfd就绪：新连接到来
        if(FD_ISSET(listenfd, &readfds))
        {
            // 非阻塞accept，有可能没有连接（多连接同时到达）
            while(true)
            {
                int connfd = accept(listenfd, nullptr, nullptr);
                if(connfd == -1)
                {
                    // EAGAIN 代表没有更多新连接了，退出循环
                    if(errno == EAGAIN || errno == EWOULDBLOCK)
                        break;
                    perror("accept");
                    break;
                }
                set_nonblock(connfd); // 客户端fd设置非阻塞！！
                client_fds.push_back(connfd);
                std::cout << "new client: " << connfd << std::endl;
            }
        }

        // 2.处理客户端可读事件
        for(size_t i = 0; i < client_fds.size(); )
        {
            int fd = client_fds[i];
            if(FD_ISSET(fd, &readfds))
            {
                char buf[1024]{0};
                // 非阻塞recv，while循环读完缓冲区全部数据
                while(true)
                {
                    int n = recv(fd, buf, sizeof(buf)-1, 0);
                    if(n > 0)
                    {
                        std::cout << "recv: " << buf << std::endl;
                        send(fd, buf, n, 0); // echo回显
                        memset(buf,0,sizeof(buf));
                    }
                    else if(n == 0)
                    {
                        // 客户端关闭连接
                        close(fd);
                        client_fds.erase(client_fds.begin()+i);
                        goto next_client; //跳出两层循环
                    }
                    else // n < 0
                    {
                        if(errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            // 缓冲区读完了，没有更多数据，正常退出
                            break;
                        }
                        // 真正出错
                        close(fd);
                        client_fds.erase(client_fds.begin()+i);
                        goto next_client;
                    }
                }
                i++;
            }
            else
            {
                i++;
            }
next_client:;
        }
    }
    close(listenfd);
    return 0;
}