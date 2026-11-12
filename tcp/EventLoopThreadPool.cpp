#include "EventLoopThreadPool.h"
#include "EventLoop.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* mainLoop, int numThreads)
    : mainLoop_(mainLoop), numThreads_(numThreads), next_(0) {}

void EventLoopThreadPool::start()
{
    for (int i = 0; i < numThreads_; ++i)
    {
        threads_.emplace_back([this]() {
            auto loop = std::make_unique<EventLoop>();
            EventLoop* raw = loop.get();
            {
                std::lock_guard<std::mutex> lk(mutex_);
                subLoops_.push_back(std::move(loop));
                subLoopPtrs_.push_back(raw);
            }
            raw->loop();      // 死循环，直到 quit()
            // 不 delete，unique_ptr 管
        });
    }
}

EventLoop* EventLoopThreadPool::getNextLoop()
{
    EventLoop* loop = mainLoop_;
    if (!subLoopPtrs_.empty())
    {
        loop = subLoopPtrs_[next_];
        next_ = (next_ + 1) % subLoopPtrs_.size();
    }
    return loop;
}

EventLoopThreadPool::~EventLoopThreadPool()
{
    // ① 先 quit 所有 subLoop（此时对象还活着）
    for (auto* loop : subLoopPtrs_)
        loop->quit();

    // ② 等线程全部结束
    for (auto& t : threads_)
        if (t.joinable())
            t.join();

    // ③ subLoops_ 析构，unique_ptr 自动 delete 每个 EventLoop
}