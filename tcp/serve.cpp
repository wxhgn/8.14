#include <iostream>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstring>

// 函数声明
void sigChildHandler(int sig);
int createListenSocket(int port);
void handleClient(int connfd);


int main()
{
    int port = 8888;
    signal(SIGCHLD, sigChildHandler);

    int listenfd = createListenSocket(port);
    if (listenfd < 0)
    {
        std::cerr << "创建监听socket失败\n";
        return -1;
    }
    std::cout << "服务端启动，监听端口 " << port << std::endl;

    while (true)
    {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int connfd = accept(listenfd,
                            reinterpret_cast<struct sockaddr*>(&cli_addr),
                            &cli_len);
        if (connfd < 0)
        {
            perror("accept");
            continue;
        }

        std::cout << "新客户端接入 IP:"
                  << inet_ntoa(cli_addr.sin_addr)
                  << " port:" << ntohs(cli_addr.sin_port) << std::endl;

        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork");
            close(connfd);
            continue;
        }
        else if (pid == 0)
        {
            // 子进程：处理客户端通信
            close(listenfd);
            handleClient(connfd);
            _exit(0);
        }
        else
        {
            // 父进程：继续等待新连接，关闭connfd
            close(connfd);
        }
    }

    close(listenfd);
    return 0;
}


// SIGCHLD信号回调，非阻塞回收僵尸进程
void sigChildHandler(int sig)
{
    (void)sig;
    while (waitpid(-1, nullptr, WNOHANG) > 0)
    {
        // 循环收割所有已经退出的子进程
    }
}


// 创建监听socket：socket -> setsockopt -> bind -> listen
int createListenSocket(int port)
{
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0)
    {
        perror("socket");
        return -1;
    }

    // 端口复用，解决重启服务TIME_WAIT占用端口
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_addr.s_addr = INADDR_ANY;
    srv_addr.sin_port = htons(port);

    if (bind(lfd, reinterpret_cast<struct sockaddr*>(&srv_addr), sizeof(srv_addr)) < 0)
    {
        perror("bind");
        close(lfd);
        return -1;
    }

    if (listen(lfd, 128) < 0)
    {
        perror("listen");
        close(lfd);
        return -1;
    }
    return lfd;
}


// 客户端业务：读取数据，原样回显（echo回射服务器）
void handleClient(int connfd)
{
    char buf[1024];
    ssize_t n;
    while ((n = read(connfd, buf, sizeof(buf)-1)) > 0)
    {
        buf[n] = '\0';
        std::cout << "收到客户端：" << buf << std::endl;
        write(connfd, buf, n);
    }
    std::cout << "客户端连接断开\n";
    close(connfd);
}