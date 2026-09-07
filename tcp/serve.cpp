#include <iostream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include"socket_fd.h"

#include"minilog.h"
#include <pthread.h>
using namespace std;

// 保存所有在线客户端的fd
vector<int> client_fds;
// 互斥锁：访问client_fds必须上锁
pthread_mutex_t mutex1 ;

// 线程工作函数：专门服务一个客户端
void* handleClient(void* arg)
{
    int connfd = *(int*)arg;
    delete (int*)arg;   //释放堆上分配的fd

    char buf[1024] = {0};

    while(true)
    {
        memset(buf,0,sizeof(buf));
        int ret = recv(connfd, buf, sizeof(buf)-1, 0);
        if(ret <= 0)
        {
           perror("客户端断开");

            //上锁，从在线列表删除该fd
            pthread_mutex_lock(&mutex1);
            for(auto it = client_fds.begin(); it != client_fds.end(); )
            {
                if(*it == connfd)
                {
                    it = client_fds.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            pthread_mutex_unlock(&mutex1);

            close(connfd);
            break;
        }
LOG_I("kk");
        cerr << "收到消息:" << buf << endl;

        // =========消息广播：发给所有其他客户端=========
        pthread_mutex_lock(&mutex1);
        for(int fd : client_fds)
        {
            if(fd != connfd) //不给发送者自己发
            {
                send(fd, buf, strlen(buf), 0);
            }
        }
        pthread_mutex_unlock(&mutex1);
    }

    pthread_exit(nullptr);
}


int main()
{
      Minilog::getInstance().setFile("multi.log");
    
    Minilog::getInstance().setMinLevel(Level::DEBUG);

    int port = 8888;
    pthread_mutex_init(&mutex1,nullptr);
    socket_fd listenfd (socket(AF_INET, SOCK_STREAM, 0));
    if(!listenfd.isValid())
    {
        perror("socket");
        return -1;
    }

    //端口复用
    int opt = 1;
    setsockopt(listenfd.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr.s_addr = INADDR_ANY;

    if(::bind(listenfd.get(), (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1)
    {
        perror("bind");
       
        return -1;
    }

    if(::listen(listenfd.get(),10) == -1)
    {
        perror("listen");
        return -1;
    }
LOG_I("fj服务端启动,监听端口");
    cout<< "服务端启动，监听端口 " << port << endl;

    while(true)
    {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int connfd = accept(listenfd.get(), (struct sockaddr*)&cli_addr, &cli_len);
        socket_fd con(connfd);
        if(!con.isValid())
        {
            perror("accept");
            continue;
        }
LOG_I("fj新客户端接入");
        cerr << "新客户端接入：" << inet_ntoa(cli_addr.sin_addr)
             << ":" << ntohs(cli_addr.sin_port) << endl;

        // 把新客户端fd加入容器，上锁
        pthread_mutex_lock(&mutex1);
        client_fds.push_back(connfd);
        pthread_mutex_unlock(&mutex1);

        // 注意：pthread_create参数不能直接传&connfd！多个线程会竞争同一个局部变量！
        // 在堆上分配，避免局部变量生命周期问题
        int* p_fd = new int(con.release());
        pthread_t tid;
        pthread_create(&tid, nullptr, handleClient, p_fd);
        pthread_detach(tid); //分离线程：线程结束自动回收资源，不用pthread_join
    }

   
    pthread_mutex_destroy(&mutex1);
    return 0;
}