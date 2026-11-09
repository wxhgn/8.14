
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
//timespec 秒加纳秒
struct timespec TimerQueue::howMuchTimeFromNow(std::chrono::steady_clock::time_point expire)
{
    
    auto duration=expire.time_since_epoch();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    if(ns<0){
        ns=0;
    } else if (ns < 1000000) {
        ns = 1000000;
    }//兜底保护：如果剩余时间太短，强制改成1毫秒
    struct timespec ts;
    ts.tv_sec = ns / 1000000000L;
    ts.tv_nsec = ns % 1000000000L;
    return ts;
}

void TimerQueue::resetTimerfd(std::chrono::steady_clock::time_point expire)
{
    struct itimerspec newValue;
    //it_interval:唤醒之后，隔多久再自动重复触发；
    //我们这里置0，只触发一次，不是内核自动周期定时器
    memset(&newValue, 0, sizeof(newValue));//把it_interval置0，代表内核只唤醒一次，不会自动循环
    newValue.it_value = howMuchTimeFromNow(expire);//多久之后唤醒一次
    int ret = timerfd_settime(timerfd_,TFD_TIMER_ABSTIME, &newValue, nullptr);
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
