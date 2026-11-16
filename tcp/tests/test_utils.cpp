#include<gtest/gtest.h>
#include<string>
#include<vector>
#include<sstream>
using namespace std;
vector<string>split(const string&s,char sep){
    vector<string>res;
    size_t pos=0,prev=0;
    while(pos<s.size()){
        if(s[pos]==sep){
        res.push_back(s.substr(prev,pos-prev));
        prev=pos+1;
        }
        pos++;
    }
    res.push_back(s.substr(prev));
    return res;
}
TEST(SplitTest,Basic){
    auto parts=split("a|b|c",'|');
    ASSERT_EQ(parts.size(),3);
    EXPECT_EQ(parts[0],"a");
    EXPECT_EQ(parts[1],"b");
    EXPECT_EQ(parts[2],"c");
}
TEST(SplitTest,Empty){
    auto parts=split("",'|');
    ASSERT_EQ(parts.size(),1);
    EXPECT_EQ(parts[0],"");
}
TEST(SplitTest,NoSeparator){
    auto parts=split("abc",'|');
    ASSERT_EQ(parts.size(),1);
    EXPECT_EQ(parts[0],"abc");
}

TEST(SplitTest, TrailingSeparator) {
    auto parts = split("a|b|", '|');
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[2], "");
}

TEST(SplitTest, ConsecutiveSeparators) {
    auto parts = split("a||b", '|');
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[1], "");
}