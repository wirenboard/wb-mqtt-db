#include "config.h"
#include "sqlite_storage.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <wblib/wbmqtt.h>
#include <wblib/testing/fake_mqtt.h>
#include <wblib/testing/testlog.h>

using namespace std::chrono;

namespace
{
    class TFakeStorage : public IStorage
    {
        WBMQTT::Testing::TLoggedFixture&      Fixture;
        bool                                  HasRecords;
        uint64_t                              ChannelId;

    public:
        TFakeStorage(WBMQTT::Testing::TLoggedFixture& fixture)
            : Fixture(fixture),
              HasRecords(false),
              ChannelId(1)
        {
        }

        PChannelInfo CreateChannel(const TChannelName& channelName) override
        {
            Fixture.Emit() << "Create channel " << channelName;
            return CreateChannelPrivate(ChannelId++, channelName.Device, channelName.Control);
        }

        void SetChannelPrecision(TChannelInfo& channelInfo, double precision) override
        {
            if (channelInfo.GetPrecision() != precision) {
                Fixture.Emit() << "Set precision for " << channelInfo.GetName() << " to " << precision;
                channelInfo.SetPrecision(precision);
            }
        }

        void WriteChannel(TChannelInfo&                         channelInfo,
                          const std::string&                    value,
                          const std::string&                    minimum,
                          const std::string&                    maximum,
                          bool                                  retained,
                          std::chrono::system_clock::time_point time) override
        {
            Fixture.Emit() << "Storage data:";
            Fixture.Emit() << "  Value: " << value;
            Fixture.Emit() << "  Min: "   << minimum;
            Fixture.Emit() << "  Max: "   << maximum;
            HasRecords = true;
        }

        void Commit() override
        {
            if (HasRecords) {
                Fixture.Emit() << "Commit";
            }
            HasRecords = false;
        }

        void GetRecordsWithAveragingInterval
            (IRecordsVisitor&                      visitor,
             const std::vector<TChannelName>&      channels,
             std::chrono::system_clock::time_point startTime,
             std::chrono::system_clock::time_point endTime,
             int64_t                               startId,
             uint32_t                              maxRecords,
             std::chrono::milliseconds             minInterval) override
        {
        }

        void GetRecordsWithLimit
            (IRecordsVisitor&                      visitor,
             const std::vector<TChannelName>&      channels,
             std::chrono::system_clock::time_point startTime,
             std::chrono::system_clock::time_point endTime,
             int64_t                               startId,
             uint32_t                              maxRecords,
             size_t                                overallRecordsLimit) override
        {
        }

        void GetChannels(IChannelVisitor& visitor) override {}
        void DeleteRecords(TChannelInfo& channel, uint32_t count) override {}
        void DeleteRecords(const std::vector<std::reference_wrapper<TChannelInfo>>& channels, uint32_t count) override {}
    };

    class TFakeChannelWriter: public IChannelWriter
    {
        WBMQTT::Testing::TLoggedFixture& Fixture;
        system_clock::time_point         StartTime;
        std::unique_ptr<TChannelWriter>  ChannelWriter;
    public:
        TFakeChannelWriter(WBMQTT::Testing::TLoggedFixture& fixture,
                           system_clock::time_point         startTime)
            : Fixture(fixture),
              StartTime(startTime),
              ChannelWriter(std::make_unique<TChannelWriter>())
        {}

        void WriteChannel(IStorage&                storage, 
                          TChannel&                channel,
                          system_clock::time_point writeTime,
                          const std::string&       groupName) override
        {
            Fixture.Emit() << "Write \"" << groupName << "\" " << channel.ChannelInfo->GetName()
                           << " " << duration_cast<milliseconds>(writeTime - StartTime).count();
            Fixture.Emit() << "  Last value: " << channel.LastValue;
            Fixture.Emit() << "  Changed: " << channel.Changed;
            Fixture.Emit() << "  Accumulator value count: " << channel.Accumulator.ValueCount;
            ChannelWriter->WriteChannel(storage, channel, writeTime, groupName);
        }
    };
} // namespace

class TDBLoggerTest : public WBMQTT::Testing::TLoggedFixture
{
protected:
    void StoreByTimeout(system_clock::time_point&    systemTime,
                        steady_clock::time_point&    steadyTime,
                        steady_clock::time_point&    startSteadyTime,
                        steady_clock::time_point&    nextSaveTime,
                        const milliseconds&          expectedNextSaveTime,
                        TMqttDbLoggerMessageHandler& handler)
    {
        systemTime += nextSaveTime - steadyTime;
        steadyTime = nextSaveTime;
        Emit() << "Store by timeout " << duration_cast<milliseconds>(steadyTime - startSteadyTime).count();
        std::queue<TValueFromMqtt> messages;
        nextSaveTime = handler.HandleMessages(messages, steadyTime, systemTime);
        ASSERT_EQ(duration_cast<milliseconds>(nextSaveTime - startSteadyTime), expectedNextSaveTime);
    }

    void StoreByMessage(std::queue<TValueFromMqtt>&     messages,
                        const system_clock::time_point& systemTime,
                        const steady_clock::time_point& steadyTime,
                        const steady_clock::time_point& startSteadyTime,
                        steady_clock::time_point&       nextSaveTime,
                        TMqttDbLoggerMessageHandler&    handler)
    {
        Emit() << "Store by message " << duration_cast<milliseconds>(steadyTime - startSteadyTime).count();
        nextSaveTime = handler.HandleMessages(messages, steadyTime, systemTime);
    }

    void StoreByMessage(std::queue<TValueFromMqtt>&     messages,
                        const system_clock::time_point& systemTime,
                        const steady_clock::time_point& steadyTime,
                        const steady_clock::time_point& startSteadyTime,
                        steady_clock::time_point&       nextSaveTime,
                        const milliseconds&             expectedNextSaveTime,
                        TMqttDbLoggerMessageHandler&    handler)
    {
        StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, handler);
        ASSERT_EQ(duration_cast<milliseconds>(nextSaveTime - startSteadyTime), expectedNextSaveTime);
    }
};

TEST_F(TDBLoggerTest, two_groups)
{
    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(2);
    group.UnchangedInterval = seconds(3);
    group.ControlPatterns.push_back({"wb-adc", "Vin"});
    group.Name = "most specific";
    cache.Groups.push_back(group);

    group.ChangedInterval   = seconds(3);
    group.UnchangedInterval = seconds(4);
    group.ControlPatterns.push_back({"wb-adc", "A1"});
    group.Name = "most specific2";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);

    auto systemTime      = system_clock::now();
    auto steadyTime      = steady_clock::now();
    auto startSteadyTime = steadyTime;
    auto startSystemTime = systemTime;
    auto nextSaveTime    = steadyTime;

    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TFakeChannelWriter>(*this, systemTime));

    // steadyTime: 0 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    handler.Start(startSteadyTime);
    std::queue<TValueFromMqtt> messages;
    messages.push({{"wb-adc", "Vin"}, "12.000", "", 0.0, systemTime});
    // steadyTime: 0 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 0 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    systemTime += milliseconds(100);
    steadyTime += milliseconds(100);
    messages.push({{"wb-adc", "A1"}, "2.000", "", 0.0, systemTime});
    // steadyTime: 100 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 0 ms
    // A1: last save = 100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    systemTime += milliseconds(1000);
    steadyTime += milliseconds(1000);
    messages.push({{"wb-adc", "Vin"}, "13.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "3.000",  "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "4.000",  "", 0.0, systemTime});
    // steadyTime: 1100 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific: next save by ChangedInterval = 2000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // most specific2: next save by ChangedInterval = 3100 ms
    // Vin: last save = 0 ms
    // A1: last save = 100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(2000), handler);

    // steadyTime: 2000 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // most specific2: next save by ChangedInterval = 3100 ms
    // Vin: last save = 2000 ms
    // A1: last save = 100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    // steadyTime: 3000 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // most specific2: next save by ChangedInterval = 3100 ms
    // Vin: last save = 2000 ms
    // A1: last save = 100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3100), handler);

    // steadyTime: 3100 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 2000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(4000), handler);

    // The values are the same
    systemTime = startSystemTime + milliseconds(3500);
    steadyTime = startSteadyTime + milliseconds(3500);
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "4.000",  "", 0.0, systemTime});
    // steadyTime: 3500 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // most specific2: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 2000 ms
    // A1: last save = 3100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(4000), handler);

    // steadyTime: 4000 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // most specific2: next save by UnchangedInterval = 8000 ms
    // Vin: last save = 4000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(6000), handler);

    // steadyTime: 6000 ms
    // most specific: next save by UnchangedInterval = 9000 ms
    // most specific2: next save by UnchangedInterval = 8000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(8000), handler);

    // steadyTime: 8000 ms
    // most specific: next save by UnchangedInterval = 9000 ms
    // most specific2: next save by UnchangedInterval = 12000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(9000), handler);

    // steadyTime: 9000 ms
    // most specific: next save by UnchangedInterval = 12000 ms
    // most specific2: next save by UnchangedInterval = 12000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(12000), handler);

    // steadyTime: 12000 ms
    // most specific: next save by UnchangedInterval = 15000 ms
    // most specific2: next save by UnchangedInterval = 16000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(15000), handler);
}

TEST_F(TDBLoggerTest, two_overlapping_groups)
{
    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(2);
    group.UnchangedInterval = seconds(3);
    group.ControlPatterns.push_back({"wb-adc", "Vin"});
    group.Name = "most specific";
    cache.Groups.push_back(group);

    group.ChangedInterval   = seconds(3);
    group.UnchangedInterval = seconds(4);
    group.ControlPatterns.push_back({"wb-adc", "+"});
    group.Name = "general";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);

    auto systemTime      = system_clock::now();
    auto steadyTime      = steady_clock::now();
    auto startSteadyTime = steadyTime;
    auto startSystemTime = systemTime;
    auto nextSaveTime    = steadyTime;

    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TFakeChannelWriter>(*this, systemTime));

    // steadyTime: 0 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // general: next save by UnchangedInterval = 4000 ms
    handler.Start(startSteadyTime);
    std::queue<TValueFromMqtt> messages;
    messages.push({{"wb-adc", "Vin"}, "12.000", "", 0.0, systemTime});
    // steadyTime: 0 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 0 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    systemTime += milliseconds(100);
    steadyTime += milliseconds(100);
    messages.push({{"wb-adc", "A1"}, "2.000", "", 0.0, systemTime});
    // steadyTime: 100 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 0 ms
    // A1: last save = 100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    systemTime += milliseconds(1000);
    steadyTime += milliseconds(1000);
    messages.push({{"wb-adc", "Vin"}, "13.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "3.000",  "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "4.000",  "", 0.0, systemTime});
    // steadyTime: 1100 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // most specific: next save by ChangedInterval = 2000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // general: next save by ChangedInterval = 3100 ms
    // Vin: last save = 0 ms
    // A1: last save = 100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(2000), handler);

    // steadyTime: 2000 ms
    // most specific: next save by UnchangedInterval = 3000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // general: next save by ChangedInterval = 3100 ms
    // Vin: last save = 2000 ms
    // A1: last save = 100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);

    // steadyTime: 3000 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // general: next save by ChangedInterval = 3100 ms
    // Vin: last save = 2000 ms
    // A1: last save = 100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3100), handler);

    // steadyTime: 3100 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 2000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(4000), handler);

    // The values are the same
    systemTime = startSystemTime + milliseconds(3500);
    steadyTime = startSteadyTime + milliseconds(3500);
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "A1"},  "4.000",  "", 0.0, systemTime});
    // steadyTime: 3500 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // general: next save by UnchangedInterval = 4000 ms
    // Vin: last save = 2000 ms
    // A1: last save = 3100 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(4000), handler);

    // steadyTime: 4000 ms
    // most specific: next save by UnchangedInterval = 6000 ms
    // general: next save by UnchangedInterval = 8000 ms
    // Vin: last save = 4000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(6000), handler);

    // steadyTime: 6000 ms
    // most specific: next save by UnchangedInterval = 9000 ms
    // general: next save by UnchangedInterval = 8000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 3100 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(8000), handler);

    // steadyTime: 8000 ms
    // most specific: next save by UnchangedInterval = 9000 ms
    // general: next save by UnchangedInterval = 12000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(9000), handler);

    // steadyTime: 9000 ms
    // most specific: next save by UnchangedInterval = 12000 ms
    // general: next save by UnchangedInterval = 12000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(12000), handler);

    // steadyTime: 12000 ms
    // most specific: next save by UnchangedInterval = 15000 ms
    // general: next save by UnchangedInterval = 16000 ms
    // Vin: last save = 6000 ms
    // A1: last save = 8000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(15000), handler);
}

TEST_F(TDBLoggerTest, save_precision)
{
    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(0);
    group.UnchangedInterval = seconds(0);
    group.ControlPatterns.push_back({"+", "+"});
    group.Name = "all";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);
    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TChannelWriter>());

    auto systemTime = system_clock::time_point();
    auto steadyTime = steady_clock::now();

    auto timeStep = milliseconds(100);
    handler.Start(steadyTime);
    std::queue<TValueFromMqtt> messages;
    messages.push({{"wb-adc", "Vin"}, "12.000", "", 0.1, systemTime});
    handler.HandleMessages(messages, steadyTime, systemTime);
    steadyTime += timeStep;
    systemTime += timeStep;
    messages.push({{"wb-adc", "Vin"}, "12.001", "", 0.1, systemTime});
    handler.HandleMessages(messages, steadyTime, systemTime);
    steadyTime += timeStep;
    systemTime += timeStep;
    messages.push({{"wb-adc", "A1"}, "13", "", 0.0, systemTime});
    handler.HandleMessages(messages, steadyTime, systemTime);
    steadyTime += timeStep;
    systemTime += timeStep;
    messages.push({{"wb-adc", "A1"}, "14.001", "", 0.0, systemTime});
    handler.HandleMessages(messages, steadyTime, systemTime);
    steadyTime += timeStep;
    systemTime += timeStep;
}

TEST(TPrecisionTest, find_precision)
{
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Precision = 0.01;
        UpdatePrecision(channelData, msg, true);
        ASSERT_EQ(channelData.Precision, 0.01);
    }
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Precision = 0.01;
        UpdatePrecision(channelData, msg, false);
        ASSERT_EQ(channelData.Precision, 0.01);
    }
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Value = "100";
        UpdatePrecision(channelData, msg, true);
        ASSERT_EQ(channelData.Precision, 1.0);
    }
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Value = "100";
        UpdatePrecision(channelData, msg, false);
        ASSERT_EQ(channelData.Precision, 0.0);
    }
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Value = "100.1";
        UpdatePrecision(channelData, msg, true);
        ASSERT_DOUBLE_EQ(channelData.Precision, 0.1);
    }
    {
        TChannel       channelData;
        TValueFromMqtt msg;
        msg.Value = "100.001";
        UpdatePrecision(channelData, msg, true);
        ASSERT_DOUBLE_EQ(channelData.Precision, 0.001);
    }
}

TEST_F(TDBLoggerTest, burst)
{
    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(2);
    group.UnchangedInterval = seconds(3);
    group.MaxBurstRecords   = 3;
    group.ControlPatterns.push_back({"+", "+"});
    group.Name = "all";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);

    auto systemTime      = system_clock::now();
    auto steadyTime      = steady_clock::now();
    auto startSteadyTime = steadyTime;
    auto nextSaveTime    = steadyTime;

    std::queue<TValueFromMqtt>  messages;
    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TFakeChannelWriter>(*this, systemTime));
    handler.Start(startSteadyTime);

    /* Check that BurstRecords remains unchanged on save */

    messages.push({{"wb-adc", "Vin"}, "12.000", "", 0.0, systemTime});
    // steadyTime: 0 ms
    // next save by UnchangedInterval = 3000 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    systemTime += milliseconds(1100);
    steadyTime += milliseconds(1100);
    messages.push({{"wb-adc", "Vin"}, "13.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    // steadyTime: 1100 ms
    // next save by UnchangedInterval = 3000 ms
    // next save by ChangedInterval = 2000 ms
    // Vin: last save = 0 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(2000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    /* Check that BurstRecords increases */

    // steadyTime: 2000 ms
    // next save by UnchangedInterval = 3000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 3000 ms
    // next save by UnchangedInterval = 6000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(6000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 6000 ms
    // next save by UnchangedInterval = 9000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(9000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 2);

    // steadyTime: 9000 ms
    // next save by UnchangedInterval = 12000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(12000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 3);

    /* Check that BurstRecords is not more than limit */

    // steadyTime: 12000 ms
    // next save by UnchangedInterval = 15000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(15000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 3);

    /* Check unscheduled write */

    systemTime += milliseconds(2500);
    steadyTime += milliseconds(2500);
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    // steadyTime: 14500 ms
    // next save by UnchangedInterval = 15000 ms
    // Vin: last save = 14500 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(15000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 2);

    /* BurstRecords = 2 but only one interval without messages is passed. Check that BurstRecords is not decreased */

    // steadyTime: 15000 ms
    // next save by UnchangedInterval = 18000 ms
    // Vin: last save = 14500 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(18000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 2);

    // steadyTime: 18000 ms
    // next save by UnchangedInterval = 21000 ms
    // Vin: last save = 14500 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(21000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 2);

    /* Check writing after BurstRecords underflow */

    systemTime += milliseconds(2500);
    steadyTime += milliseconds(2500);
    messages.push({{"wb-adc", "Vin"}, "11.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "12.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "13.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "14.000", "", 0.0, systemTime});
    messages.push({{"wb-adc", "Vin"}, "15.000", "", 0.0, systemTime});
    // steadyTime: 20500 ms
    // next save by UnchangedInterval = 21000 ms
    // next save by ChangedInterval = 22500 ms
    // Vin: last save = 20500 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(21000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 21000 ms
    // next save by UnchangedInterval = 24000 ms
    // next save by ChangedInterval = 22500 ms
    // Vin: last save = 20500 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(22500), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 22500 ms
    // next save by UnchangedInterval = 24000 ms
    // Vin: last save = 22500 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(24000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 24000 ms
    // next save by UnchangedInterval = 27000 ms
    // Vin: last save = 22500 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(27000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    /* ADC pattern. One message every 500 ms */
    for (size_t i = 0; i < 15; ++i) {
        systemTime += milliseconds(500);
        steadyTime += milliseconds(500);
        messages.push({{"wb-adc", "Vin"}, "11.000", "", 0.0, systemTime});
        StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, handler);
        ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);
    }
}

TEST_F(TDBLoggerTest, burstSwitch)
{
    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(2);
    group.UnchangedInterval = seconds(3);
    group.MaxBurstRecords   = 3;
    group.ControlPatterns.push_back({"+", "+"});
    group.Name = "all";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);

    auto systemTime      = system_clock::now();
    auto steadyTime      = steady_clock::now();
    auto startSteadyTime = steadyTime;
    auto nextSaveTime    = steadyTime;

    std::queue<TValueFromMqtt>  messages;
    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TFakeChannelWriter>(*this, systemTime));
    handler.Start(startSteadyTime);

    for (size_t i = 0; i < 5; ++i) {
        messages.push({{"wb-adc", "Vin"}, std::to_string(i), "switch", 0.0, systemTime});
    }
    // steadyTime: 0 ms
    // next save by UnchangedInterval = 3000 ms
    // next save by ChangedInterval = 2000 ms
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(2000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);

    // steadyTime: 2000 ms
    // next save by UnchangedInterval = 3000 ms
    // Vin: last save = 2000 ms
    StoreByTimeout(systemTime, steadyTime, startSteadyTime, nextSaveTime, milliseconds(3000), handler);
    ASSERT_EQ(cache.Groups[0].Channels.begin()->second.BurstRecords, 0);
}

TEST_F(TDBLoggerTest, burstSwitchAndValue)
{
    // If max_burst is non zero, TDBLogger will start writing discrete channels before analog channels
    // It will lead to group records count calculation on cache with channels with ChannelInfo==nullptr
    // So we will get a segfault
    // The test check crash fix

    TLoggerCache cache;

    TLoggingGroup group;
    group.ChangedInterval   = seconds(2);
    group.UnchangedInterval = seconds(3);
    group.MaxBurstRecords   = 3;
    group.MaxRecords        = 100;
    group.ControlPatterns.push_back({"+", "+"});
    group.Name = "all";
    cache.Groups.push_back(group);

    TFakeStorage storage(*this);

    auto systemTime      = system_clock::now();
    auto steadyTime      = steady_clock::now();
    auto startSteadyTime = steadyTime;
    auto nextSaveTime    = steadyTime;

    std::queue<TValueFromMqtt>  messages;
    TMqttDbLoggerMessageHandler handler(cache, storage, std::make_unique<TFakeChannelWriter>(*this, systemTime));
    handler.Start(startSteadyTime);

    messages.push({{"wb-adc", "A1"}, "10.0", "", 0.0, systemTime});
    for (size_t i = 0; i < 5; ++i) {
        messages.push({{"wb-adc", "Vin"}, "1", "switch", 0.0, systemTime});
    }
    StoreByMessage(messages, systemTime, steadyTime, startSteadyTime, nextSaveTime, handler);
    // Must not crash
}
