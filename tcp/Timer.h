#ifndef TIMER_H
#define TIMER_H

#include <functional>
#include <chrono>

using TimerCallback = std::function<void()>;

class Timer
{
public:
    Timer(TimerCallback cb,
          std::chrono::steady_clock::time_point expireTime,
          double interval)
        : callback_(std::move(cb))
        , expireTime_(expireTime)
        , interval_(interval)
        , repeat_(interval > 0)
    {}

    std::chrono::steady_clock::time_point getExpireTime() const
    {
        return expireTime_;
    }

    bool isRepeat() const
    {
        return repeat_;
    }
double getInterval() const
{
    return interval_;
}
    void restart(std::chrono::steady_clock::time_point now)
    {
        // 修复：duration<double> 必须先 duration_cast 成 steady_clock::duration
        auto sec_dur = std::chrono::duration<double>(interval_);//把interval_装进浮点秒盒子，但是这个盒子不是steady家的盒子
        expireTime_ = now + std::chrono::duration_cast<
                                 std::chrono::steady_clock::duration>(sec_dur);//再转化成steady自己的盒子
    }

    void run() const
    {
        callback_();
    }

private:
    TimerCallback callback_;
    std::chrono::steady_clock::time_point expireTime_;
    double interval_;
    bool repeat_;
};

#endif