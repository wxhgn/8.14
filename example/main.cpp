#include <thread>
#include <vector>
#include"minilog.h"
void worker(int id) {
    for (int i = 0; i < 10; ++i) {
        LOG_I("Thread " + std::to_string(id) + " message " + std::to_string(i));
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

int main() {
    Minilog::getInstance().setFile("multi.log");
    
    Minilog::getInstance().setMinLevel(Level::INFO);

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) t.join();

    return 0;
}