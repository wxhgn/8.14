#ifndef TIMERQUEUE_H
#define TIMERQUEUE_H

#include "Timer.h"
#include <set>
#include<map>
#include <vector>
#include <sys/epoll.h>
#include <memory>

class EpollPoller;
class Channel;   // 前向声明

class TimerQueue
{
public:
    explicit TimerQueue(EpollPoller* poller);
    ~TimerQueue();

    void addTimer(TimerCallback cb, double delaySec, bool repeat);

private:
    void resetTimerfd(std::chrono::steady_clock::time_point expire);
    void handleRead();
    static std::chrono::steady_clock::time_point now();
    static struct timespec howMuchTimeFromNow(std::chrono::steady_clock::time_point expire);

    int timerfd_;
    EpollPoller* poller_;
    std::unique_ptr<Channel> timer_channel_;   // ★ 新增：绑定 timerfd_ 的 Channel

    using TimerPtr = std::shared_ptr<Timer>;
    std::multimap<std::chrono::steady_clock::time_point, TimerPtr> timers_;
   
};

#endif