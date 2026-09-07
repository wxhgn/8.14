#include"socket_fd.h"
socket_fd::socket_fd(int fd){
m_fd=fd;
}
socket_fd::~socket_fd(){
   if(m_fd>=0){
    ::close(m_fd);
   }
}
socket_fd::socket_fd(socket_fd&&other){
    m_fd=other.m_fd;
    other.m_fd=-1;
}
socket_fd&socket_fd::operator=(socket_fd&&other){
    if(this==&other)return *this;
    if(m_fd>=0){
        ::close(m_fd);
    }
m_fd=other.m_fd;
other.m_fd=-1;
return *this;
}
int socket_fd::get()const{
    return m_fd;
}
bool socket_fd::isValid()const{
    return m_fd>=0;
}
int socket_fd::release(){
    int tmp=m_fd;
    m_fd=-1;
    return tmp;
}
void socket_fd::reset(int new_fd){
    if(m_fd>=0){
        ::close(m_fd);
    }
    m_fd=new_fd;
}