#pragma
#include<unistd.h>
#include<sys/socket.h>
class socket_fd
{
private:
  int m_fd;
public:
explicit socket_fd(int fd=-1);
~socket_fd();
socket_fd(const socket_fd&)=delete;
socket_fd&operator=(const socket_fd&)=delete;
socket_fd(socket_fd&&other);
socket_fd&operator=(socket_fd&&other);
int get()const;
bool isValid()const;
void reset(int new_fd);
int release();
};
