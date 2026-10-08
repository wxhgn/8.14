#include "minilog.h"

Minilog& Minilog::getInstance()
{
    static Minilog obj;
    return obj;
}

void Minilog::printf(Level level, const char* fmt, ...)
{
    if (level < minLevel)
        return;

    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    print(level, std::string(buf));
}

void Minilog::setMaxSize(size_t bytes)
{
    std::lock_guard<std::mutex> lock(mtx);
    roll_bytes = bytes;
}

void Minilog::setMinLevel(Level level)
{
    std::lock_guard<std::mutex> lock(mtx);
    minLevel = level;
}

void Minilog::setFile(const std::string& path)
{
    std::lock_guard<std::mutex> lock(mtx);
    if (ofs.is_open())
    {
        ofs.close();
    }
    log_path = path;
    ofs.open(log_path, std::ios::app);
    enableFile = ofs.is_open();
    current_size = 0;
}

std::string Minilog::getTime() const
{
    time_t now = time(nullptr);
    tm t = *localtime(&now);
    char tmp[32];
    strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", &t);
    return std::string(tmp);
}

void Minilog::checkRoll()
{
    // 日志文件滚动，超过roll_bytes就新建文件
    if (!enableFile) return;
    if (current_size < roll_bytes) return;

    ofs.close();
    std::string new_name = log_path + "." + getTime();
    rename(log_path.c_str(), new_name.c_str());
    ofs.open(log_path, std::ios::app);
    current_size = 0;
}

void Minilog::print(Level level, const std::string& msg)
{
    std::lock_guard<std::mutex> lock(mtx);
    if (level < minLevel) return;

    std::stringstream ss;
    ss << getTime();
    switch (level)
    {
        case Level::DEBUG: ss << " [DEBUG] "; break;
        case Level::INFO:  ss << " [INFO]  "; break;
        case Level::WARN:  ss << " [WARN]  "; break;
        case Level::ERROR: ss << " [ERROR] "; break;
    }
    ss << msg << "\n";
    std::string out = ss.str();

    // 控制台输出
    std::cout << out;

    // 文件输出
    if (enableFile && ofs.is_open())
    {
        ofs << out;
        ofs.flush();
        current_size += out.size();
        checkRoll();
    }
}

Minilog::~Minilog()
{
    std::lock_guard<std::mutex> lock(mtx);
    if (ofs.is_open())
    {
        ofs.flush();
        ofs.close();
    }
}