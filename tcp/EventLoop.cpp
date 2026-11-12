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
    }, 3.0, 3.0);
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

void EventLoop::quit()
{
    running_ = false;

    wakeup();
}