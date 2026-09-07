#pragma once
#include <mutex>
#include <iostream>
#include <string>
#include <sstream>
#include <ctime>
#include <fstream>

enum class Level {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

class Minilog {
public:
    static Minilog& getInstance();

    void setMaxSize(size_t bytes);
    void setMinLevel(Level level);
    void setFile(const std::string& path);
    void print(Level level, const std::string& msg);

    Minilog(const Minilog&) = delete;
    Minilog& operator=(const Minilog&) = delete;

private:
    Minilog() = default;
    ~Minilog();

    void checkRoll();          // 调用者需持有锁
    std::string getTime() const;

    std::mutex mtx;
    std::string log_path;
    size_t roll_bytes = 5 * 1024 * 1024;
    size_t current_size = 0;   // 缓存文件大小
    std::ofstream ofs;
    Level minLevel = Level::DEBUG;
    bool enableFile = false;
};

#define LOG_D(msg) Minilog::getInstance().print(Level::DEBUG, msg)
#define LOG_I(msg) Minilog::getInstance().print(Level::INFO, msg)
#define LOG_W(msg) Minilog::getInstance().print(Level::WARN, msg)
#define LOG_E(msg) Minilog::getInstance().print(Level::ERROR, msg)