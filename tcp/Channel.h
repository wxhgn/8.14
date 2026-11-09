#ifndef CHANNEL_H
#define CHANNEL_H

#include <sys/epoll.h>
#include <functional>

class Channel
{
public:
    using ReadCallback = std::function<void()>;
    using WriteCallback = std::function<void()>;
    using CloseCallback = std::function<void()>;
    using ErrorCallback = std::function<void()>;

    Channel(int fd) : fd_(fd), events_(0), revents_(0) {}

    int fd() const { return fd_; }
    uint32_t events() const { return events_; }
    void set_revents(uint32_t r) { revents_ = r; }

    void setReadCallback(ReadCallback cb) { read_cb_ = std::move(cb); }
    void setWriteCallback(WriteCallback cb) { write_cb_ = std::move(cb); }
    void setCloseCallback(CloseCallback cb) { close_cb_ = std::move(cb); }
    void setErrorCallback(ErrorCallback cb) { error_cb_ = std::move(cb); }

    void enableReading()  { events_ |= EPOLLIN; }
    void disableReading() { events_ &= ~EPOLLIN; }
    void enableWriting()  { events_ |= EPOLLOUT; }
    void disableWriting() { events_ &= ~EPOLLOUT; }

    bool isReading() const { return events_ & EPOLLIN; }
    bool isWriting() const { return events_ & EPOLLOUT; }

    void handleEvent()
    {
        if (revents_ & (EPOLLERR | EPOLLHUP)) {
            if (error_cb_) error_cb_();
            if (close_cb_) close_cb_();
            return;
        }
        if (revents_ & EPOLLIN)  { if (read_cb_)  read_cb_();  }
        if (revents_ & EPOLLOUT) { if (write_cb_) write_cb_(); }
    }

private:
    int fd_;
    uint32_t events_;
    uint32_t revents_;
    ReadCallback read_cb_;
    WriteCallback write_cb_;
    CloseCallback close_cb_;
    ErrorCallback error_cb_;
};

#endif