#include "config.h"
#include "sqlite_storage.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <wblib/wbmqtt.h>
#include <wblib/testing/fake_mqtt.h>
#include <wblib/testing/testlog.h>

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

        void SetChannelPrecision(TChannelInfo& channelInfo, double precision)
        {
            if (channelInfo.GetPrecision() != precision) {
                Fixture.Emit() << "Set precision for " << channelInfo.GetName() << " to " << precision;
                SetPrecision(channelInfo, precision);
            }
        }

        void WriteChannel(TChannelInfo&                         channelInfo,
                          const std::string&                    value,
                          const std::string&                    minimum,
                          const std::string&                    maximum,
                          bool                                  retained,
                          std::chrono::system_clock::time_point time)
        {
            Fixture.Emit() << "Storage data:";
            Fixture.Emit() << "  Value: " << value;
            Fixture.Emit() << "  Min: "   << minimum;
            Fixture.Emit() << "  Max: "   << maximum;
            HasRecords = true;
        }

        void Commit()
        {
            if (HasRecords) {
                Fixture.Emit() << "Commit";
            }
            HasRecords = false;
        }

        void GetRecords(IRecordsVisitor&                      visitor,
                        const std::vector<TChannelName>&      channels,
                        std::chrono::system_clock::time_point startTime,
                        std::chrono::system_clock::time_point endTime,
                        int64_t                               startId,
                        uint32_t                              maxRecords,
                        std::chrono::milliseconds             minInterval)
        {
        }

        void GetChannels(IChannelVisitor& visitor) {}
        void DeleteRecords(TChannelInfo& channel, uint32_t count) {}
        void DeleteRecords(const std::vector<PChannelInfo>& channels, uint32_t count) {}
    };

    class TFakeChannelWriter: public IChannelWriter
    {
        WBMQTT::Testing::TLoggedFixture&      Fixture;
        std::chrono::steady_clock::time_point StartTime;
        std::unique_ptr<TChannelWriter>       ChannelWriter;
    public:
        TFakeChannelWriter(WBMQTT::Testing::TLoggedFixture& fixture)
            : Fixture(fixture),
              StartTime(std::chrono::steady_clock::now()),
              ChannelWriter(std::make_unique<TChannelWriter>())
        {}

        void WriteChannel(IStorage&                             storage, 
                          TChannelInfo&                         channelInfo, 
                          const TChannel&                       channelData,
                          std::chrono::system_clock::time_point writeTime,
                          const std::string&                    groupName) override
        {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            Fixture.Emit() << std::chrono::duration_cast<std::chrono::seconds>(now - StartTime).count();
            Fixture.Emit() << "Write \"" << groupName << "\" " << channelInfo.GetName();
            Fixture.Emit() << "  Last value: " << channelData.LastValue;
            Fixture.Emit() << "  Changed: " << channelData.Changed;
            Fixture.Emit() << "  Accumulator value count: " << channelData.Accumulator.ValueCount;
            ChannelWriter->WriteChannel(storage, channelInfo, channelData, writeTime, groupName);
        }
    };
} // namespace

class TDBLoggerTest : public WBMQTT::Testing::TLoggedFixture
{
protected:
    WBMQTT::Testing::PFakeMqttBroker Broker;
    WBMQTT::Testing::PFakeMqttClient Client;

    std::string testRootDir;
    std::string schemaFile;

    void SetUp()
    {
        Broker = WBMQTT::Testing::NewFakeMqttBroker(*this);
        Client = Broker->MakeClient("dblogger_test");

        char* d = getenv("TEST_DIR_ABS");
        if (d != NULL) {
            testRootDir = d;
            testRootDir += '/';
        }
        testRootDir += "dblogger_test_data";

        schemaFile = testRootDir + "/../../wb-mqtt-db.schema.json";

        Client->Start();
    }
};

TEST_F(TDBLoggerTest, two_groups)
{
    TLoggerCache cache(LoadConfig(testRootDir + "/wb-mqtt-db2.conf", schemaFile).Cache);
    auto backend = WBMQTT::NewDriverBackend(Client);
    auto driver = WBMQTT::NewDriver(WBMQTT::TDriverArgs{}.SetId("test").SetBackend(backend));
    std::shared_ptr<TMQTTDBLogger> logger(
        new TMQTTDBLogger(driver,
                          cache,
                          std::make_unique<TFakeStorage>(*this),
                          WBMQTT::NewMqttRpcServer(Client, "db_logger"),
                          std::make_unique<TFakeChannelWriter>(*this),
                          std::chrono::seconds(5)));

    auto        future = Broker->WaitForPublish("/rpc/v1/db_logger/history/get_channels");
    std::thread t([=]() { logger->Start(); });
    future.Wait();
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "12.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "2.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "13.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "3.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::seconds(8));
    logger->Stop();
    t.join();
}

TEST_F(TDBLoggerTest, two_overlapping_groups)
{
    TLoggerCache cache(LoadConfig(testRootDir + "/wb-mqtt-db3.conf", schemaFile).Cache);
    auto backend = WBMQTT::NewDriverBackend(Client);
    auto driver = WBMQTT::NewDriver(WBMQTT::TDriverArgs{}.SetId("test").SetBackend(backend));
    std::shared_ptr<TMQTTDBLogger> logger(
        new TMQTTDBLogger(driver,
                          cache,
                          std::make_unique<TFakeStorage>(*this),
                          WBMQTT::NewMqttRpcServer(Client, "db_logger"),
                          std::make_unique<TFakeChannelWriter>(*this),
                          std::chrono::seconds(5)));
    auto        future = Broker->WaitForPublish("/rpc/v1/db_logger/history/get_channels");
    std::thread t([=]() { logger->Start(); });
    future.Wait();
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "12.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "2.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "13.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "3.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::seconds(8));
    logger->Stop();
    t.join();
}

TEST_F(TDBLoggerTest, save_precision)
{
    TLoggerCache cache(LoadConfig(testRootDir + "/save_precision.conf", schemaFile).Cache);
    auto backend = WBMQTT::NewDriverBackend(Client);
    auto driver = WBMQTT::NewDriver(WBMQTT::TDriverArgs{}.SetId("test").SetBackend(backend));

    std::shared_ptr<TMQTTDBLogger> logger(
        new TMQTTDBLogger(driver,
                        cache,
                        std::make_unique<TFakeStorage>(*this),
                        WBMQTT::NewMqttRpcServer(Client, "db_logger"),
                        std::make_unique<TChannelWriter>(),
                        std::chrono::seconds(5)));
    auto        future = Broker->WaitForPublish("/rpc/v1/db_logger/history/get_channels");
    std::thread t([=]() { logger->Start(); });
    future.Wait();
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin/meta/precision", "0.1", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "12.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "12.001", 1, false}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "13", 1, false}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "14.001", 1, false}});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    logger->Stop();
    t.join();
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
