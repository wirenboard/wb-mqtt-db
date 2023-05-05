#include "utils.h"
#include <gtest/gtest.h>

class UtilsTest: public ::testing::Test
{};

TEST_F(UtilsTest, Join)
{
    std::vector<std::string> v = {"a", "b", "c"};
    auto res = Utils::Join(
        v.begin(),
        v.end(),
        [](const std::string& s) { return s; },
        ", ");
    ASSERT_EQ(res, "a, b, c");
}

TEST_F(UtilsTest, isNumber)
{
    ASSERT_TRUE(Utils::isNumber("1"));
    ASSERT_TRUE(Utils::isNumber("1.1"));
    ASSERT_TRUE(Utils::isNumber("1.1e-1"));
    ASSERT_TRUE(Utils::isNumber("1.1e+1"));
    ASSERT_FALSE(Utils::isNumber("a"));
    ASSERT_FALSE(Utils::isNumber("1a"));
    ASSERT_FALSE(Utils::isNumber("a1"));
    ASSERT_FALSE(Utils::isNumber("1.1.1"));
}

TEST_F(UtilsTest, AddCommonWhereClause)
{
    std::string str;
    Utils::AddCommonWhereClause(str, 3);
    EXPECT_EQ(str, "channel IN (?,?,?) AND timestamp > ? AND timestamp < ?");
    str.clear();
    Utils::AddCommonWhereClause(str, 0);
    EXPECT_EQ(str, "channel IN () AND timestamp > ? AND timestamp < ?");
}

TEST_F(UtilsTest, AddWithAverageQuery)
{
    std::string str;
    Utils::AddWithAverageQuery(str, 3);
    EXPECT_EQ(str,
              "SELECT MAX(uid), channel, value, MAX(timestamp), MIN(min), MAX(max), "
              "retained, AVG(value) FROM data INDEXED BY data_topic_timestamp WHERE "
              "channel IN (?,?,?) AND timestamp > ? AND timestamp < ? AND uid > ? "
              "GROUP BY (round(timestamp/?)), channel");
}

TEST_F(UtilsTest, AddWithoutAverageQuery)
{
    std::string str;
    Utils::AddWithoutAverageQuery(str, 3);
    EXPECT_EQ(str,
              "SELECT uid, channel, value, timestamp, min, max, retained, value "
              "FROM data INDEXED BY data_topic_timestamp WHERE channel IN "
              "(?,?,?) AND timestamp > ? AND timestamp < ? AND uid > ?");
}
