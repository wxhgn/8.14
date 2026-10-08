#pragma once
#include <mutex>
#include <iostream>
#include <string>
#include <sstream>
#include <ctime>
#include <fstream>
#include <cstdarg>
#include <cstdio>

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
    // 纯字符串版本
    void print(Level level, const std::string& msg);
    // printf可变参数格式化版本
    void printf(Level level, const char* fmt, ...);

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

// 可变参数宏，##__VA_ARGS__兼容无参数场景 LOG_I("hello")
#define LOG_D(fmt, ...) Minilog::getInstance().printf(Level::DEBUG, fmt, ##__VA_ARGS__)
#define LOG_I(fmt, ...) Minilog::getInstance().printf(Level::INFO, fmt, ##__VA_ARGS__)
#define LOG_W(fmt, ...) Minilog::getInstance().printf(Level::WARN, fmt, ##__VA_ARGS__)
#define LOG_E(fmt, ...) Minilog::getInstance().printf(Level::ERROR, fmt, ##__VA_ARGS__)