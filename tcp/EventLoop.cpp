#include "EventLoop.h"
#include "Channel.h"
#include "EpollPoller.h"
#include "TimerQueue.h"
#include "minilog.h"

#include <cassert>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sstream>

EventLoop::EventLoop()
    : running_(true)
    , threadId_(std::this_thread::get_id())
    , wakeup_fd_(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC))
{
    assert(wakeup_fd_ >= 0);

    poller_ = new EpollPoller();
    timerQueue_ = new TimerQueue(this, poller_);

    wakeup_channel_ = std::make_shared<Channel>(this, wakeup_fd_);
    wakeup_channel_->enableReading();
    wakeup_channel_->setReadCallback([this]() { handleWakeup(); });
    poller_->registerChannel(wakeup_channel_.get());
    poller_->addChannel(wakeup_channel_.get());

    timerQueue_->addTimer([this]() {
        std::ostringstream oss;
        oss << threadId_;
        LOG_I("[SubLoop Monitor] thread=%s", oss.str().c_str());
    }, 30.0, true);
}


EventLoop::~EventLoop() {
    running_ = false;
    close(wakeup_fd_);
    delete timerQueue_;   // ★ 先 delete timerQueue_（它用 poller_）
    delete poller_;       // ★ 再 delete poller_
}

bool EventLoop::isInLoopThread() const
{
    return std::this_thread::get_id() == threadId_;
}

void EventLoop::wakeup()
{
    uint64_t one = 1;
    write(wakeup_fd_, &one, sizeof one);
}

void EventLoop::addChannel(Channel* ch)
{
    poller_->registerChannel(ch);
    poller_->addChannel(ch);
}

void EventLoop::removeChannel(Channel* ch)
{
    poller_->removeChannel(ch);
    poller_->unregisterChannel(ch->fd());
}

void EventLoop::updateChannel(Channel* ch)
{
    poller_->updateChannel(ch);
}

void EventLoop::handleWakeup()
{
    uint64_t val = 0;
    read(wakeup_fd_, &val, sizeof val);

    std::vector<Functor> cbs;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cbs.swap(pendingFunctors_);
    }
    for (auto& cb : cbs) cb();
}

void EventLoop::runInLoop(Functor cb)
{
    if (isInLoopThread()) cb();
    else queueInLoop(std::move(cb));
}

void EventLoop::queueInLoop(Functor cb)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    wakeup();
}

void EventLoop::loop()
{
    while (running_)
    {
        std::vector<Channel*> activeChannels;
        poller_->poll(10, activeChannels);
        for (auto ch : activeChannels) ch->handleEvent();
 
    }
}
//你代码不把这一批事件处理完、不跑完 doPendingTasks，**不会回到 epoll_wait**。
/* void poll(int timeoutMs, std::vector<Channel*>& activeChannels)
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
        SA_RESTART 告诉内核"信号处理函数返回后，被中断的系统调用自动重启"。
        epoll_wait 被 SIGINT 打断后自动重启，不会返回 -1, EINTR，主循环感知不到信号。
        去掉后，epoll_wait 返回 -1，errno = EINTR，主循环能感知。*/

void EventLoop::quit()
{
    quit_=true;
    running_ = false;
    wakeup();
}