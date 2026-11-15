#ifndef EVENTLOOPTHREADPOOL_H
#define EVENTLOOPTHREADPOOL_H

#include <vector>
#include <thread>
#include <mutex>
#include <memory>

class EventLoop;

class EventLoopThreadPool
{
public:
    EventLoopThreadPool(EventLoop* mainLoop, int numThreads);
    ~EventLoopThreadPool();

    void start();
    EventLoop* getNextLoop();
    void quitAll();
private:
    EventLoop* mainLoop_;
    int numThreads_;
    int next_;

    std::mutex mutex_;
    std::vector<std::unique_ptr<EventLoop>> subLoops_;   // ★ unique_ptr 管生命周期
    std::vector<EventLoop*> subLoopPtrs_;                // ★ 裸指针给 getNextLoop
    std::vector<std::thread> threads_;
};

#endif