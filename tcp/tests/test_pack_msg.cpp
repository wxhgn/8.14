#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <cstring>
#include <arpa/inet.h>

using namespace std;

// 拷贝 pack_msg
vector<char> pack_msg(const string& s) {
    vector<char> out;
    uint32_t bodylen = (uint32_t)s.size();
    uint32_t netlen  = htonl(bodylen);
    out.insert(out.end(), (char*)&netlen, (char*)&netlen + 4);
    out.insert(out.end(), s.begin(), s.end());
    return out;
}

TEST(PackMsgTest, Basic) {
    auto pkt = pack_msg("hello");
    ASSERT_EQ(pkt.size(), 9);
    uint32_t net_len;
    memcpy(&net_len, pkt.data(), 4);
    EXPECT_EQ(ntohl(net_len), 5);
    EXPECT_EQ(string(pkt.data() + 4, 5), "hello");
}

TEST(PackMsgTest, Empty) {
    auto pkt = pack_msg("");
    ASSERT_EQ(pkt.size(), 4);
    uint32_t net_len;
    memcpy(&net_len, pkt.data(), 4);
    EXPECT_EQ(ntohl(net_len), 0);
}