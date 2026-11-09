
#include "TimerQueue.h"
#include "EpollPoller.h"     // ★ 新增，为了 Channel 和 EpollPoller
#include <sys/timerfd.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

// ... createTimerfd 不变 ...
namespace
{
int createTimerfd()
{
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd < 0)
    {
        perror("timerfd_create");
    }
    return fd;
}
}


TimerQueue::TimerQueue(EpollPoller* poller)
    : timerfd_(createTimerfd())
    , poller_(poller)
{
    // ★ 注册 timerfd 到 epoll
    timer_channel_ = std::make_unique<Channel>(timerfd_);
    timer_channel_->enableReading();
    timer_channel_->setReadCallback([this]() { handleRead(); });
    poller_->registerChannel(timer_channel_.get());
    poller_->addChannel(timer_channel_.get());
}

TimerQueue::~TimerQueue()
{
    if (timer_channel_)
    {
        poller_->removeChannel(timer_channel_.get());
        poller_->unregisterChannel(timerfd_);
    }
    close(timerfd_);
}

// ... 其他不变 ...
std::chrono::steady_clock::time_point TimerQueue::now()
{
    return std::chrono::steady_clock::now();
}

struct timespec TimerQueue::howMuchTimeFromNow(std::chrono::steady_clock::time_point expire)
{
    auto diff = expire - now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
    if (ns < 1000000) ns = 1000000;
    struct timespec ts;
    ts.tv_sec = ns / 1000000000L;
    ts.tv_nsec = ns % 1000000000L;
    return ts;
}

void TimerQueue::resetTimerfd(std::chrono::steady_clock::time_point expire)
{
    struct itimerspec newValue;
    struct itimerspec oldValue;
    memset(&newValue, 0, sizeof(newValue));
    newValue.it_value = howMuchTimeFromNow(expire);
    int ret = timerfd_settime(timerfd_, 0, &newValue, &oldValue);
    if (ret < 0)
    {
        perror("timerfd_settime");
    }
}

void TimerQueue::addTimer(TimerCallback cb, double delaySec, bool repeat)
{
    auto expire = now() + std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(delaySec));
    TimerPtr timer = std::make_shared<Timer>(std::move(cb), expire, repeat ? delaySec : 0.0);
    bool earliestChanged = timers_.empty() || expire < timers_.begin()->first;
    timers_.insert({expire, timer});
    if (earliestChanged)
    {
        resetTimerfd(expire);
    }
}
void TimerQueue::handleRead()
{
    uint64_t howmany;
    ssize_t n = read(timerfd_, &howmany, sizeof(howmany));
    if (n != sizeof(howmany))
    {
        perror("read timerfd");
        return;
    }

    auto cur = now();
    std::vector<TimerPtr> expired;
    auto it = timers_.begin();
    while (it != timers_.end() && it->first <= cur)
    {
        expired.push_back(it->second);
        it = timers_.erase(it);
    }

    for (auto& t : expired)
    {
        t->run();
        if (t->isRepeat())
        {
            auto newExpire = now() + std::chrono::duration_cast<
                                         std::chrono::steady_clock::duration>(
                                         std::chrono::duration<double>(t->getInterval()));
            timers_.emplace(newExpire, t);   // ★ 用 emplace 代替 insert({...})
        }
    }

    if (!timers_.empty())
    {
        resetTimerfd(timers_.begin()->first);
    }
}
