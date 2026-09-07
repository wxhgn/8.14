#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/select.h>

using namespace std;

int main()
{
    
    //1. 创建客户端socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(sockfd < 0)
    {
        perror("socket");
        return -1;
    }

    //2. 填写服务端地址
    struct sockaddr_in serv_addr;
    memset(&serv_addr,0,sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8888);
    //连接本机服务端
    inet_pton(AF_INET,"127.0.0.1",&serv_addr.sin_addr);

    //3. 发起连接
    if(connect(sockfd,(struct sockaddr*)&serv_addr,sizeof(serv_addr)) <0)
    {
        perror("connect");
        close(sockfd);
        return -1;
    }

    cout << "✅ 连接聊天室服务器成功，可以发消息了" << endl;

    char buf[1024] = {0};

    while(true)
    {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds); //监控：键盘输入(0号文件描述符)
        FD_SET(sockfd, &read_fds);       //监控：服务端socket，接收别人广播消息

        //select阻塞等待：键盘有输入 或者 socket收到数据，才往下走
        int ret = select(sockfd+1, &read_fds, nullptr, nullptr, nullptr);
        if(ret <0)
        {
            perror("select");
            break;
        }

        // =========情况1：键盘输入了文字，发送给服务器=========
        if(FD_ISSET(STDIN_FILENO, &read_fds))
        {
            memset(buf,0,sizeof(buf));
            cin.getline(buf,sizeof(buf));
            send(sockfd, buf, strlen(buf),0);
        }

        // =========情况2：服务器发来广播消息，打印出来=========
        if(FD_ISSET(sockfd, &read_fds))
        {
            memset(buf,0,sizeof(buf));
            int n = recv(sockfd, buf, sizeof(buf)-1,0);
            if(n <=0)
            {
                cout << "\n❌服务器断开连接，客户端退出" << endl;
                break;
            }
            cout << "\n[别人消息]：" << buf << endl;
        }
    }

    close(sockfd);
    return 0;
}