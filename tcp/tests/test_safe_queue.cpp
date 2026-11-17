#include <gtest/gtest.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>

using namespace std;

// 拷贝 SafeQueue，需要包含全局 g_pool_running
std::atomic<bool> g_pool_running{true};

template<typename T>
class SafeQueue {
private:
    queue<T> q;
    mutex mtx;
    condition_variable cv;
    int max_size;
public:
    explicit SafeQueue(int max_sz) : max_size(max_sz) {}
    bool push(T&& val) {
        {
            lock_guard<mutex> lock(mtx);
            if ((int)q.size() >= max_size) return false;
            q.emplace(std::forward<T>(val));
        }
        cv.notify_one();
        return true;
    }
    bool pop(T& out) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this] { return !q.empty() || !g_pool_running.load(); });
        if (q.empty()) return false;
        out = move(q.front());
        q.pop();
        return true;
    }
    size_t size() {
        lock_guard<mutex> lock(mtx);
        return q.size();
    }
};

TEST(SafeQueueTest, PushPop) {
    SafeQueue<int> q(10);
    q.push(1);
    q.push(2);
    int val;
    ASSERT_TRUE(q.pop(val));
    EXPECT_EQ(val, 1);
    ASSERT_TRUE(q.pop(val));
    EXPECT_EQ(val, 2);
}

TEST(SafeQueueTest, Full) {
    SafeQueue<int> q(2);
    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_FALSE(q.push(3));   // 满了
}

TEST(SafeQueueTest, BlockingPop) {
    SafeQueue<int> q(10);
    int val = 0;
    thread t([&]() {
        q.pop(val);   // 阻塞等待
    });
    this_thread::sleep_for(chrono::milliseconds(50));
    q.push(42);
    t.join();
    EXPECT_EQ(val, 42);
}