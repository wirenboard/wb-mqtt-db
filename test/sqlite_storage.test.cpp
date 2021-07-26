#include "config.h"
#include "sqlite_storage.h"
#include "db_migrations.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <wblib/testing/fake_mqtt.h>
#include <wblib/testing/testlog.h>

namespace
{
    class TRecordsVisitor: public IRecordsVisitor
    {
        WBMQTT::Testing::TLoggedFixture& Fixture;
    public:
        TRecordsVisitor(WBMQTT::Testing::TLoggedFixture& fixture): Fixture(fixture)
        {}

        bool ProcessRecord(int                                   recordId,
                           const TChannelInfo&                   channel,
                           double                                averageValue,
                           std::chrono::system_clock::time_point timestamp,
                           double                                minValue,
                           double                                maxValue,
                           bool                                  retain)
        {
            Fixture.Emit() << "Record id: " << recordId;
            Fixture.Emit() << "\tChannel: " << channel.GetName() << "[" << channel.GetId() << "]";
            Fixture.Emit() << "\tAverage: " << averageValue;
            Fixture.Emit() << "\tTime: "    << std::chrono::duration_cast<std::chrono::seconds>(timestamp - std::chrono::system_clock::time_point()).count();
            Fixture.Emit() << "\tMin: "     << minValue;
            Fixture.Emit() << "\tMax: "     << maxValue;
            Fixture.Emit() << "\tRetain: "  << retain;
            return true;
        }

        bool ProcessRecord(int                                   recordId,
                           const TChannelInfo&                   channel,
                           const std::string&                    value,
                           std::chrono::system_clock::time_point timestamp,
                           bool                                  retain)
        {
            Fixture.Emit() << "Record id: " << recordId;
            Fixture.Emit() << "\tChannel: " << channel.GetName() << "[" << channel.GetId() << "]";
            Fixture.Emit() << "\tValue: "   << value;
            Fixture.Emit() << "\tTime: "    << std::chrono::duration_cast<std::chrono::seconds>(timestamp - std::chrono::system_clock::time_point()).count();
            Fixture.Emit() << "\tRetain: "  << retain;
            return true;
        }
    };

    class TChannelVisitor: public IChannelVisitor
    {
        WBMQTT::Testing::TLoggedFixture& Fixture;
    public:
        TChannelVisitor(WBMQTT::Testing::TLoggedFixture& fixture): Fixture(fixture)
        {}

        void ProcessChannel(PChannelInfo channel) override
        {
            Fixture.Emit() << "Channel id: "  << channel->GetId();
            Fixture.Emit() << "\tName: "      << channel->GetName();
            Fixture.Emit() << "\tCount: "     << channel->GetRecordCount();
            Fixture.Emit() << "\tLast time: " << std::chrono::duration_cast<std::chrono::seconds>(channel->GetLastRecordTime() - std::chrono::system_clock::time_point()).count();
            Fixture.Emit() << "\tPrecision: " << channel->GetPrecision();
        }
    };
}

class TSqliteStorageTest : public WBMQTT::Testing::TLoggedFixture
{
};

TEST_F(TSqliteStorageTest, migrations)
{
    ASSERT_EQ(TSqliteStorage::GetDBVersion(), GetMigrations().size());
}

TEST_F(TSqliteStorageTest, write)
{
    TSqliteStorage storage(":memory:");
    TChannelName channelName("wb-adc", "Vin");
    auto vin = storage.CreateChannel(channelName);
    std::chrono::system_clock::time_point time;

    storage.WriteChannel(*vin, "10", "",   "",   true,  time + std::chrono::seconds(50));
    storage.WriteChannel(*vin, "11", "10", "12", false, time + std::chrono::seconds(100));

    TRecordsVisitor visitor(*this);
    storage.GetRecords(visitor, {channelName}, time, time + std::chrono::seconds(200), 0, 100, std::chrono::milliseconds(0));

    ASSERT_EQ(vin->GetRecordCount(), 2);
}

TEST_F(TSqliteStorageTest, deleteRows)
{
    TSqliteStorage storage(":memory:");
    TChannelName channelName("wb-adc", "Vin");
    auto vin = storage.CreateChannel(channelName);
    std::chrono::system_clock::time_point time;
    for (size_t i = 1; i < 10; ++i) {
        storage.WriteChannel(*vin, std::to_string(i), "", "", false, time + std::chrono::seconds(i * 10));
    }
    ASSERT_EQ(vin->GetRecordCount(), 9);

    storage.DeleteRecords(*vin, 5);
    ASSERT_EQ(vin->GetRecordCount(), 4);

    TRecordsVisitor visitor(*this);
    storage.GetRecords(visitor, {channelName}, time, time + std::chrono::seconds(200), 0, 100, std::chrono::milliseconds(0));

    // Delete more than exists
    storage.DeleteRecords(*vin, 5);
    ASSERT_EQ(vin->GetRecordCount(), 0);
}

TEST_F(TSqliteStorageTest, deleteGroupRows)
{
    TSqliteStorage storage(":memory:");

    TChannelName vinChannelName("wb-adc", "Vin");
    auto vin = storage.CreateChannel(vinChannelName);
    std::chrono::system_clock::time_point time;
    for (size_t i = 1; i < 10; ++i) {
        storage.WriteChannel(*vin, std::to_string(i), "", "", false, time + std::chrono::seconds(i * 10));
    }

    TChannelName a1ChannelName("wb-adc", "A1");
    auto a1 = storage.CreateChannel(a1ChannelName);
    for (size_t i = 1; i < 10; ++i) {
        storage.WriteChannel(*a1, std::to_string(i * 10), "", "", false, time + std::chrono::seconds(i * 10 + 5));
    }

    TRecordsVisitor visitor(*this);

    Emit() << "## Delete records from Vin";
    storage.DeleteRecords({vin}, 5);
    ASSERT_EQ(vin->GetRecordCount(), 4);
    ASSERT_EQ(a1->GetRecordCount(), 9);
    storage.GetRecords(visitor, {vinChannelName, a1ChannelName}, time, time + std::chrono::seconds(200), 0, 100, std::chrono::milliseconds(0));

    Emit() << "## Delete records from Vin and A1";
    storage.DeleteRecords({vin, a1}, 6);
    ASSERT_EQ(vin->GetRecordCount(), 3);
    ASSERT_EQ(a1->GetRecordCount(), 4);
    storage.GetRecords(visitor, {vinChannelName, a1ChannelName}, time, time + std::chrono::seconds(200), 0, 100, std::chrono::milliseconds(0));

    // Delete more than exists
    storage.DeleteRecords(*vin, 5);
    ASSERT_EQ(vin->GetRecordCount(), 0);
}

TEST_F(TSqliteStorageTest, loadOldValues)
{
    TSqliteStorage storage("file::memory:?cache=shared");

    TChannelName vinChannelName("wb-adc", "Vin");
    auto vin = storage.CreateChannel(vinChannelName);
    storage.SetChannelPrecision(*vin, 0.01);
    std::chrono::system_clock::time_point time;
    for (size_t i = 1; i < 10; ++i) {
        storage.WriteChannel(*vin, std::to_string(i), "", "", false, time + std::chrono::seconds(i * 10));
    }

    TChannelName a1ChannelName("wb-adc", "A1");
    auto a1 = storage.CreateChannel(a1ChannelName);
    for (size_t i = 1; i < 10; ++i) {
        storage.WriteChannel(*a1, std::to_string(i * 10), "", "", false, time + std::chrono::seconds(i * 10 + 5));
    }
    storage.Commit();

    TSqliteStorage storage2("file::memory:?cache=shared");
    TChannelVisitor cv(*this);
    storage2.GetChannels(cv);
}

TEST_F(TSqliteStorageTest, precision)
{
    TSqliteStorage storage(":memory:");
    auto vin = storage.CreateChannel({"wb-adc", "Vin"});
    ASSERT_EQ(vin->GetPrecision(), 0.0);
    storage.SetChannelPrecision(*vin, 0.01);
    storage.Commit();
    ASSERT_EQ(vin->GetPrecision(), 0.01);
}
