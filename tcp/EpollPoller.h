#ifndef EPOLLPOLLER_H
#define EPOLLPOLLER_H

#include "Channel.h"
#include <sys/epoll.h>
#include <unistd.h>
#include <cassert>
#include <vector>
#include <cerrno>     // ★ 加：errno、EINTR
#include <cstdio>  
#include <unordered_map>

#define MAX_EVENTS 1024

class EpollPoller
{
public:
    EpollPoller()
    {
        epfd_ = epoll_create1(0);
        assert(epfd_ >= 0);
    }
    ~EpollPoller() { close(epfd_); }

    void updateChannel(Channel* ch)
    {
        int fd = ch->fd();
        auto it = fd2chan_.find(fd);
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events = ch->events() | EPOLLET;
        if (it == fd2chan_.end()) {
            epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
        } else {
            epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
        }
    }
    void addChannel(Channel* ch)
    {
        int fd = ch->fd();
        struct epoll_event ev;
        ev.data.fd = fd;
        ev.events = ch->events() | EPOLLET;
        epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
    }
    void removeChannel(Channel* ch)
    {
        int fd = ch->fd();
        epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    }

    void poll(int timeoutMs, std::vector<Channel*>& activeChannels)
    { activeChannels.clear();
    struct epoll_event evs[MAX_EVENTS];
    int n = epoll_wait(epfd_, evs, MAX_EVENTS, timeoutMs);
    if (n < 0) {
        if (errno == EINTR) return;   // ★ 被信号打断，正常返回，让 loop 检查 running_
        perror("epoll_wait");
        return;
    }
        for (int i = 0; i < n; ++i)
        {
            int fd = evs[i].data.fd;
            uint32_t rev = evs[i].events;
            auto it = fd2chan_.find(fd);
            if (it != fd2chan_.end())
            {
                it->second->set_revents(rev);
                activeChannels.push_back(it->second);
            }
        }
    }
    void registerChannel(Channel* ch)
    {
        fd2chan_[ch->fd()] = ch;
    }
    void unregisterChannel(int fd)
    {
        fd2chan_.erase(fd);
    }

private:
    int epfd_;
    std::unordered_map<int, Channel*> fd2chan_;
};

#endif