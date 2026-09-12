#include <iostream>
#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/select.h>
 #include <sys/epoll.h>
using namespace std;

vector<char> pack_mag(const string& s)
{
    vector<char> out;
    uint32_t bodylen = s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

int main()
{
     
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(sockfd < 0)
    {
        perror("socket");
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port   = htons(8888);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if(connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("connect");
        close(sockfd);
        return -1;
    }

   std::cout << "✅ 连接聊天室服务器成功，可以发消息了" << endl;

    char buf[1024] = {0};
    vector<char> recv_buf;
 
    while(true)
    {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds);
        FD_SET(sockfd, &read_fds);

        int ret = select(sockfd + 1, &read_fds, nullptr, nullptr, nullptr);
        if(ret < 0)
        {
            perror("select");
            break;
        }

        // =========情况1：键盘输入，打包发送=========
        if(FD_ISSET(STDIN_FILENO, &read_fds))
        {
            memset(buf, 0, sizeof(buf));
            cin.getline(buf, sizeof(buf));

            string msg(buf);              // 转成 string
            auto pack = pack_mag(msg);    // 打包 [4字节长度][body]
            send(sockfd, pack.data(), pack.size(), 0);
        }

        // =========情况2：接收服务端数据，按长度解包=========
        if(FD_ISSET(sockfd, &read_fds))
        {
            char tmp[1024];
            int n = recv(sockfd, tmp, sizeof(tmp), 0);
            if(n <= 0)
            {
                std::cout << "\n❌服务器断开连接,客户端退出" << endl;
                break;
            }

            // 把新收到的数据追加到缓冲区
            recv_buf.insert(recv_buf.end(), tmp, tmp + n);

            // 循环解析完整包：[4字节大端长度][body]
            while(true)
            {
                if(recv_buf.size() < 4) break;

                uint32_t net_len;
                memcpy(&net_len, recv_buf.data(), 4);
                uint32_t body_len = ntohl(net_len);

                if(recv_buf.size() < 4 + body_len) break;   // 包还没收全

                string msg(recv_buf.data() + 4, recv_buf.data() + 4 + body_len);
                recv_buf.erase(recv_buf.begin(), recv_buf.begin() + 4 + body_len);

                std::cout << "\n[服务器]:" << msg << endl;
            }
        }
    }

    close(sockfd);
    return 0;
}