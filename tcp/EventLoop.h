#ifndef EVENTLOOP_H
#define EVENTLOOP_H

#include <functional>
#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include <cassert>
#include <sys/eventfd.h>
#include <unistd.h>

class Channel;
class EpollPoller;
class TimerQueue;

class EventLoop
{
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    void loop();
    void quit();

    bool isInLoopThread() const;
    void runInLoop(Functor cb);
    void queueInLoop(Functor cb);
    void addChannel(Channel*ch);
    void removeChannel(Channel*ch);
    void updateChannel(Channel*ch);

private:
    void wakeup();
    void handleWakeup();

    bool running_;
    std::thread::id threadId_;
    int wakeup_fd_;
    std::shared_ptr<Channel> wakeup_channel_;

    std::mutex mutex_;
    std::vector<Functor> pendingFunctors_;

    EpollPoller* poller_;
    TimerQueue* timerQueue_;
};

#endif
