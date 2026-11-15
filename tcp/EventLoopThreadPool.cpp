#include "EventLoopThreadPool.h"
#include "EventLoop.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* mainLoop, int numThreads)
    : mainLoop_(mainLoop), numThreads_(numThreads), next_(0) {}

 void EventLoopThreadPool::quitAll() {
    for (auto* loop : subLoopPtrs_) loop->quit();
}
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
/*Ctrl+C
  → 内核调用 sig_handler
      → g_pool_running = false
      → g_mainLoop->quit()
          → quit_ = true
          → running_ = false
          → wakeup() 写 eventfd
  → epoll_wait 返回（被 eventfd 唤醒 或 被 SIGINT 打断返回 EINTR）
  → EpollPoller::poll 返回
  → EventLoop::loop 检查 running_ == false，退出
  → main 继续
      → g_pool_running = false
      → business_queue.notify_all()
      → g_thread_pool.join()   ← worker 处理完当前 msg 后退出
      → close(listenfd)
      → g_pool.reset()
      → return 0
          → threadPool 析构
              → for (loop : subLoopPtrs_) loop->quit()
                  → quit_ = true
                  → running_ = false
                  → wakeup()
              → subLoop 的 epoll_wait 返回，loop 退出
              → join 成功
          → subLoops_ 析构，unique_ptr delete EventLoop
          → mainLoop 析构
              → delete timerQueue_   ← 先
              → delete poller_       ← 后
          → return 0*/