#include <gtest/gtest.h>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <vector>

// 拷贝 BlacklistManager 到测试里（或者抽到 blacklist.h）
class BlacklistManager {
private:
    std::mutex mtx_;
    std::unordered_map<int, std::unordered_set<int>> blacklist_;
public:
    void load(int owner_uid, const std::vector<int>& blocked_uids) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto& s = blacklist_[owner_uid];
        s.clear();
        for (int b : blocked_uids) s.insert(b);
    }
    void clear(int owner_uid) {
        std::lock_guard<std::mutex> lock(mtx_);
        blacklist_.erase(owner_uid);
    }
    bool isBlocked(int owner_uid, int target_uid) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = blacklist_.find(owner_uid);
        if (it == blacklist_.end()) return false;
        return it->second.count(target_uid) > 0;
    }
    void add(int owner_uid, int blocked_uid) {
        std::lock_guard<std::mutex> lock(mtx_);
        blacklist_[owner_uid].insert(blocked_uid);
    }
    void remove(int owner_uid, int blocked_uid) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = blacklist_.find(owner_uid);
        if (it != blacklist_.end()) it->second.erase(blocked_uid);
    }
};

class BlacklistTest : public ::testing::Test {
protected:
    BlacklistManager mgr;
};

TEST_F(BlacklistTest, InitiallyEmpty) {
    EXPECT_FALSE(mgr.isBlocked(1, 2));
}

TEST_F(BlacklistTest, AddAndCheck) {
    mgr.add(1, 2);
    EXPECT_TRUE(mgr.isBlocked(1, 2));
    EXPECT_FALSE(mgr.isBlocked(2, 1));   // 单向
}

TEST_F(BlacklistTest, Remove) {
    mgr.add(1, 2);
    mgr.remove(1, 2);
    EXPECT_FALSE(mgr.isBlocked(1, 2));
}

TEST_F(BlacklistTest, Load) {
    mgr.load(1, {2, 3, 4});
    EXPECT_TRUE(mgr.isBlocked(1, 2));
    EXPECT_TRUE(mgr.isBlocked(1, 3));
    EXPECT_TRUE(mgr.isBlocked(1, 4));
    EXPECT_FALSE(mgr.isBlocked(1, 5));
}

TEST_F(BlacklistTest, Clear) {
    mgr.add(1, 2);
    mgr.clear(1);
    EXPECT_FALSE(mgr.isBlocked(1, 2));
}