#include "minilog.h"
#include <cstdio>   // for rename

static std::string levelToString(Level level) {
    switch (level) {
        case Level::DEBUG: return "DEBUG";
        case Level::INFO:  return "INFO";
        case Level::WARN:  return "WARN";
        case Level::ERROR: return "ERROR";
        default:           return "UNKNOWN";
    }
}

Minilog::~Minilog() {
    std::lock_guard<std::mutex> lock(mtx);
    if (ofs.is_open()) ofs.close();
}

Minilog& Minilog::getInstance() {
    static Minilog instance;
    return instance;
}

void Minilog::setFile(const std::string& path) {
    std::lock_guard<std::mutex> lock(mtx);
    if (ofs.is_open()) ofs.close();
    log_path = path;
    ofs.open(log_path, std::ios::out | std::ios::app);
    enableFile = ofs.is_open();
    if (enableFile) {
        // 获取当前文件大小（若存在）
        std::ifstream tmp(log_path, std::ios::in | std::ios::ate);
        if (tmp.is_open()) {
            current_size = static_cast<size_t>(tmp.tellg());
        } else {
            current_size = 0;
        }
    }
}

void Minilog::setMaxSize(size_t bytes) {
    std::lock_guard<std::mutex> lock(mtx);
    roll_bytes = bytes;
}

void Minilog::setMinLevel(Level level) {
    std::lock_guard<std::mutex> lock(mtx);
    minLevel = level;
}

void Minilog::print(Level level, const std::string& msg) {
    if (level < minLevel) return;   // 快速路径，无需锁（但 minLevel 可能被修改，需加锁判断）

    std::lock_guard<std::mutex> lock(mtx);
    // 再次检查级别（防止在获取锁前 minLevel 被降低）
    if (level < minLevel) return;

    // 检查滚动
    checkRoll();

    std::stringstream ss;
    ss << "[" << getTime() << "] " << msg << " [" << levelToString(level) << "]";
    std::string log_str = ss.str();

    std::cout << log_str << std::endl;

    if (enableFile && ofs.is_open()) {
        ofs << log_str << '\n';
        ofs.flush();
        current_size += log_str.size() + 1;  // +1 for newline
    }
}

void Minilog::checkRoll() {
    // 必须持有锁
    if (!enableFile || !ofs.is_open() || roll_bytes == 0) return;

    if (current_size < roll_bytes) return;

    // 关闭当前文件
    ofs.close();

    // 寻找空闲备份编号
    size_t index = 1;
    while (true) {
        std::string candidate = log_path + "_" + std::to_string(index);
        std::ifstream test(candidate);
        if (test.is_open()) {
            ++index;
            test.close();
        } else {
            break;
        }
    }

    std::string backup = log_path + "_" + std::to_string(index);
    if (std::rename(log_path.c_str(), backup.c_str()) != 0) {
        // 重命名失败，尝试重新打开原始文件（可能丢失滚动）
        std::cerr << "Failed to rename log file for rollover\n";
        ofs.open(log_path, std::ios::out | std::ios::app);
        enableFile = ofs.is_open();
        current_size = 0; // 重置，避免无限滚动
        return;
    }

    // 重新打开新文件
    ofs.open(log_path, std::ios::out | std::ios::app);
    enableFile = ofs.is_open();
    current_size = 0;   // 新文件大小归零
}

std::string Minilog::getTime() const {
    std::time_t t = std::time(nullptr);
    // 使用局部缓冲区避免线程安全问题
    std::tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_buf);
    return std::string(buffer);
}