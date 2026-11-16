#include <gtest/gtest.h>
#include "buffer.h"   // 你的 buffer.h 在 tcp/ 下

TEST(BufferTest, AppendAndRetrieve) {
    Buffer buf;
    buf.append("hello", 5);
    EXPECT_EQ(buf.readableBytes(), 5);
    
    std::string s = buf.retrieveAllAsString();
    EXPECT_EQ(s, "hello");
    EXPECT_EQ(buf.readableBytes(), 0);
}

TEST(BufferTest, AppendString) {
    Buffer buf;
    buf.append(std::string("world"));
    EXPECT_EQ(buf.readableBytes(), 5);
}

TEST(BufferTest, EnsureWritable) {
    Buffer buf(10);
    buf.append("0123456789", 10);
    buf.append("x", 1);
    EXPECT_EQ(buf.readableBytes(), 11);
}

TEST(BufferTest, RetrievePartial) {
    Buffer buf;
    buf.append("abcdef", 6);
    buf.retrieve(3);
    EXPECT_EQ(buf.readableBytes(), 3);
    EXPECT_EQ(buf.retrieveAllAsString(), "def");
}