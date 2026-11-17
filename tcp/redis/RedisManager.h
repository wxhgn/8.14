#pragma once
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <sw/redis++/redis++.h>

class RedisManager {
private:
    std::unique_ptr<sw::redis::Redis> redis_;
    const int PRESENCE_TTL = 40;
public:
    RedisManager();
    void update_presence(int uid);
    void remove_presence(int uid);
    bool is_online(int uid);   // 注意这里是 bool
    void push_offline_msg(int target_uid, const std::string& msg);
    std::vector<std::string> pop_all_offline_msg(int target_uid);
};