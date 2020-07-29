#include "config.h"
#include <gtest/gtest.h>
#include "sqlite_storage.h"
#include <stdio.h>

class TSqliteStorageTest : public testing::Test
{
protected:
    std::string testRootDir;
    std::string shemaFile;

    void SetUp()
    {
        char* d = getenv("TEST_DIR_ABS");
        if (d != NULL) {
            testRootDir = d;
            testRootDir += '/';
        }
        testRootDir += "sqlite_storage_test_data";

        shemaFile = testRootDir + "/../../wb-mqtt-db.schema.json";
    }

    void CreateOldDb(const std::string& file)
    {
        std::remove(file.c_str());
        SQLite::Database DB(file, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);

        DB.exec("CREATE TABLE IF NOT EXISTS devices ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device VARCHAR(255) UNIQUE "
                " )  ");

        DB.exec("CREATE TABLE IF NOT EXISTS channels ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device VARCHAR(255), "
                "control VARCHAR(255)"
                ")  ");

        DB.exec("CREATE TABLE IF NOT EXISTS groups ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "group_id VARCHAR(255) "
                ")  ");

        DB.exec("CREATE TABLE IF NOT EXISTS data ("
                "uid INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device INTEGER,"
                "channel INTEGER,"
                "value VARCHAR(255),"
                "timestamp INTEGER DEFAULT(0),"
                "group_id INTEGER,"
                "max VARCHAR(255),"
                "min VARCHAR(255),"
                "retained INTEGER"
                ")");

        DB.exec("CREATE TABLE IF NOT EXISTS variables ("
                "name VARCHAR(255) PRIMARY KEY, "
                "value VARCHAR(255) )");

        DB.exec("INSERT OR REPLACE INTO variables (name, value) VALUES ('db_version', 3)");

        DB.exec("CREATE INDEX IF NOT EXISTS data_topic ON data (channel)");
        DB.exec("CREATE INDEX IF NOT EXISTS data_topic_timestamp ON data (channel, timestamp)");
        DB.exec("CREATE INDEX IF NOT EXISTS data_gid ON data (group_id)");
        DB.exec("CREATE INDEX IF NOT EXISTS data_gid_timestamp ON data (group_id, timestamp)");

        for (size_t i = 0; i < 3; ++i) {
            DB.exec("INSERT INTO devices (device) VALUES ('device" + std::to_string(i) + "')");
        }

        // same channels but with different id's
        for (size_t j = 0; j < 2; ++j ) {
            for (size_t i = 0; i < 6; ++i) {
                DB.exec("INSERT INTO channels (device, control) VALUES ('device" + std::to_string(i/2) + "', 'control" + std::to_string(i/2) + "')");
            }
        }

        for (size_t i = 0; i < 2; ++i) {
            DB.exec("INSERT INTO groups (group_id) VALUES ('group" + std::to_string(i) + "')");
        }

        for (size_t i = 0; i < 10; ++i) {
            DB.exec("INSERT INTO groups (group_id) VALUES ('group" + std::to_string(i) + "')");
        }

    }
};

TEST_F(TSqliteStorageTest, convert_db_from_old_format)
{
    std::string dbFileName(testRootDir + "/old.db");
    CreateOldDb(dbFileName);
}

TEST_F(TConfigTest, bad_config)
{
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_THROW(LoadConfig(testRootDir + "/bad/bad" + std::to_string(i) + ".conf", shemaFile), std::runtime_error) << "bad" << i <<".conf";
    }
}

TEST_F(TConfigTest, good_config)
{
    TMQTTDBLoggerConfig c = LoadConfig(testRootDir + "/good/wb-mqtt-db.conf", shemaFile);
    ASSERT_TRUE(c.Debug);
    ASSERT_EQ(c.DBFile, "/var/lib/wirenboard/db/data.db");
    ASSERT_EQ(c.RequestTimeout, std::chrono::seconds(1234));
    ASSERT_EQ(c.Cache.Groups.size(), 3);

    ASSERT_EQ(c.Cache.Groups[0].Name, "everything else");
    ASSERT_EQ(c.Cache.Groups[0].ChangedInterval, std::chrono::seconds(220));
    ASSERT_EQ(c.Cache.Groups[0].MaxChannelRecords, 2000);
    ASSERT_EQ(c.Cache.Groups[0].MaxRecords, 200000);
    ASSERT_EQ(c.Cache.Groups[0].MqttTopicPatterns.size(), 1);
    ASSERT_EQ(c.Cache.Groups[0].MqttTopicPatterns[0], "/devices/+/controls/+");
    ASSERT_EQ(c.Cache.Groups[0].UnchangedInterval, std::chrono::seconds(2200));

    ASSERT_EQ(c.Cache.Groups[1].Name, "more specific");
    ASSERT_EQ(c.Cache.Groups[1].ChangedInterval, std::chrono::seconds(320));
    ASSERT_EQ(c.Cache.Groups[1].MaxChannelRecords, 3000);
    ASSERT_EQ(c.Cache.Groups[1].MaxRecords, 300000);
    ASSERT_EQ(c.Cache.Groups[1].MqttTopicPatterns.size(), 2);
    ASSERT_EQ(c.Cache.Groups[1].MqttTopicPatterns[0], "/devices/wb-adc/controls/+");
    ASSERT_EQ(c.Cache.Groups[1].MqttTopicPatterns[1], "/devices/wb-gpio/controls/+");
    ASSERT_EQ(c.Cache.Groups[1].UnchangedInterval, std::chrono::seconds(3200));

    ASSERT_EQ(c.Cache.Groups[2].Name, "most specific");
    ASSERT_EQ(c.Cache.Groups[2].ChangedInterval, std::chrono::seconds(420));
    ASSERT_EQ(c.Cache.Groups[2].MaxChannelRecords, 4000);
    ASSERT_EQ(c.Cache.Groups[2].MaxRecords, 400000);
    ASSERT_EQ(c.Cache.Groups[2].MqttTopicPatterns.size(), 1);
    ASSERT_EQ(c.Cache.Groups[2].MqttTopicPatterns[0], "/devices/wb-adc/controls/A1");
    ASSERT_EQ(c.Cache.Groups[2].UnchangedInterval, std::chrono::seconds(4200));
}
