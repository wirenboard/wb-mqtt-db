#include "config.h"
#include <gtest/gtest.h>
#include <vector>

class TConfigTest : public testing::Test
{
protected:
    std::string testRootDir;
    std::string schemaFile;

    void SetUp()
    {
        char* d = getenv("TEST_DIR_ABS");
        if (d != NULL) {
            testRootDir = d;
            testRootDir += '/';
        }
        testRootDir += "config_test_data";

        schemaFile = testRootDir + "/../../wb-mqtt-db.schema.json";
    }
};

TEST_F(TConfigTest, no_file)
{
    ASSERT_THROW(LoadConfig("fake.config", ""), std::runtime_error);
    ASSERT_THROW(LoadConfig(testRootDir + "/bad/wb-mqtt-db.conf", ""), std::runtime_error);
}

TEST_F(TConfigTest, bad_config)
{
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_THROW(LoadConfig(testRootDir + "/bad/bad" + std::to_string(i) + ".conf", schemaFile), std::runtime_error) << "bad" << i <<".conf";
    }
}

TEST_F(TConfigTest, good_config)
{
    TMQTTDBLoggerConfig c = LoadConfig(testRootDir + "/good/wb-mqtt-db.conf", schemaFile);
    ASSERT_TRUE(c.Debug);
    ASSERT_EQ(c.DBFile, "/var/lib/wirenboard/db/data.db");
    ASSERT_EQ(c.GetValuesRpcRequestTimeout, std::chrono::seconds(1234));
    ASSERT_EQ(c.Cache.Groups.size(), 3);

    ASSERT_EQ(c.Cache.Groups[0].Name, "everything else");
    ASSERT_EQ(c.Cache.Groups[0].ChangedInterval, std::chrono::seconds(220));
    ASSERT_EQ(c.Cache.Groups[0].MaxChannelRecords, 2000);
    ASSERT_EQ(c.Cache.Groups[0].MaxRecords, 200000);
    ASSERT_EQ(c.Cache.Groups[0].ControlPatterns.size(), 1);
    ASSERT_EQ(c.Cache.Groups[0].ControlPatterns[0].Device, "+");
    ASSERT_EQ(c.Cache.Groups[0].ControlPatterns[0].Control, "+");
    ASSERT_EQ(c.Cache.Groups[0].UnchangedInterval, std::chrono::seconds(2200));

    ASSERT_EQ(c.Cache.Groups[1].Name, "more specific");
    ASSERT_EQ(c.Cache.Groups[1].ChangedInterval, std::chrono::seconds(320));
    ASSERT_EQ(c.Cache.Groups[1].MaxChannelRecords, 3000);
    ASSERT_EQ(c.Cache.Groups[1].MaxRecords, 300000);
    ASSERT_EQ(c.Cache.Groups[1].ControlPatterns.size(), 2);
    ASSERT_EQ(c.Cache.Groups[1].ControlPatterns[0].Device, "wb-adc");
    ASSERT_EQ(c.Cache.Groups[1].ControlPatterns[0].Control, "+");
    ASSERT_EQ(c.Cache.Groups[1].ControlPatterns[1].Device, "wb-gpio");
    ASSERT_EQ(c.Cache.Groups[1].ControlPatterns[1].Control, "+");
    ASSERT_EQ(c.Cache.Groups[1].UnchangedInterval, std::chrono::seconds(3200));

    ASSERT_EQ(c.Cache.Groups[2].Name, "most specific");
    ASSERT_EQ(c.Cache.Groups[2].ChangedInterval, std::chrono::seconds(420));
    ASSERT_EQ(c.Cache.Groups[2].MaxChannelRecords, 4000);
    ASSERT_EQ(c.Cache.Groups[2].MaxRecords, 400000);
    ASSERT_EQ(c.Cache.Groups[2].ControlPatterns.size(), 1);
    ASSERT_EQ(c.Cache.Groups[2].ControlPatterns[0].Device, "wb-adc");
    ASSERT_EQ(c.Cache.Groups[2].ControlPatterns[0].Control, "A1");
    ASSERT_EQ(c.Cache.Groups[2].UnchangedInterval, std::chrono::seconds(4200));
}
