#include "RedisManager.h"
#include "minilog.h"

RedisManager::RedisManager() {
    try {
        redis_ = std::make_unique<sw::redis::Redis>("tcp://127.0.0.1:6379");
        redis_->ping();
        LOG_I("Redis 连接成功！");
    } catch (const std::exception& e) {
        LOG_E("Redis 连接失败: %s", e.what());
    }
}

void RedisManager::update_presence(int uid) {
    if (!redis_) return;
    try {
        redis_->set("presence:" + std::to_string(uid), "1", std::chrono::seconds(PRESENCE_TTL));
    } catch (const std::exception& e) { 
        LOG_E("Redis update_presence error: %s", e.what()); 
    }
}

void RedisManager::remove_presence(int uid) {
    if (!redis_) return;
    try {
        redis_->del("presence:" + std::to_string(uid));
    } catch (const std::exception& e) { 
        LOG_E("Redis remove_presence error: %s", e.what()); 
    }
}

// 注意这里必须是 bool，和头文件保持一致
bool RedisManager::is_online(int uid) {
    if (!redis_) return false;
    try {
        return redis_->exists("presence:" + std::to_string(uid)) > 0;
    } catch (const std::exception& e) { 
        LOG_E("Redis is_online error: %s", e.what()); 
        return false; 
    }
}

void RedisManager::push_offline_msg(int target_uid, const std::string& msg) {
    if (!redis_) return;
    try {
        redis_->rpush("offline_msg:" + std::to_string(target_uid), msg);
    } catch (const std::exception& e) { 
        LOG_E("Redis push_offline error: %s", e.what()); 
    }
}

std::vector<std::string> RedisManager::pop_all_offline_msg(int target_uid) {
    std::vector<std::string> msgs;
    if (!redis_) return msgs;
    try {
        std::string key = "offline_msg:" + std::to_string(target_uid);
        redis_->lrange(key, 0, -1, std::back_inserter(msgs));
        redis_->del(key);
    } catch (const std::exception& e) { 
        LOG_E("Redis pop_offline error: %s", e.what()); 
    }
    return msgs;
}