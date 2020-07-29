#include "config.h"
#include "sqlite_storage.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <wblib/testing/fake_mqtt.h>
#include <wblib/testing/testlog.h>

namespace
{
    class TFakeStorage : public IStorage
    {
        WBMQTT::Testing::TLoggedFixture&      Fixture;
        std::chrono::steady_clock::time_point StartTime;
        bool                                  HasRecords;

    public:
        TFakeStorage(WBMQTT::Testing::TLoggedFixture& fixture)
            : Fixture(fixture), StartTime(std::chrono::steady_clock::now()), HasRecords(false)
        {
        }

        void Load(TLoggerCache& cache) {}

        void WriteChannel(const TChannelName& channelName, TChannel& channel, TLoggingGroup& group)
        {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            Fixture.Emit()
                << std::chrono::duration_cast<std::chrono::seconds>(now - StartTime).count();
            Fixture.Emit() << "Write \"" << group.Name << "\" " << channelName;
            Fixture.Emit() << "  Last value: " << channel.LastValue;
            Fixture.Emit() << "  Changed: " << channel.Changed;
            Fixture.Emit() << "  Accumulator value count: " << channel.Accumulator.ValueCount;
            if (channel.Accumulator.HasValues()) {
                Fixture.Emit() << "  Sum: " << channel.Accumulator.Average();
                Fixture.Emit() << "  Min: " << channel.Accumulator.Min;
                Fixture.Emit() << "  Max: " << channel.Accumulator.Max;
            }
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
    };
} // namespace

class TDBLoggerTest : public WBMQTT::Testing::TLoggedFixture
{
protected:
    WBMQTT::Testing::PFakeMqttBroker Broker;
    WBMQTT::Testing::PFakeMqttClient Client;

    std::string testRootDir;
    std::string shemaFile;

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

        shemaFile = testRootDir + "/../../wb-mqtt-db.schema.json";

        Client->Start();
    }
};

TEST_F(TDBLoggerTest, two_groups)
{
    TLoggerCache cache(LoadConfig(testRootDir + "/wb-mqtt-db2.conf", shemaFile).Cache);
    std::shared_ptr<TMQTTDBLogger> logger(
        new TMQTTDBLogger(Client,
                          cache,
                          std::make_unique<TFakeStorage>(*this),
                          WBMQTT::NewMqttRpcServer(Client, "db_logger"),
                          std::chrono::seconds(5)));
    std::thread t([=]() { logger->Start(); });
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "12.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "13.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "2.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "3.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::milliseconds(3500));
    Broker->Publish("test", {{"/devices/wb-adc/controls/Vin", "14.000", 1, true}});
    Broker->Publish("test", {{"/devices/wb-adc/controls/A1", "4.000", 1, true}});
    std::this_thread::sleep_for(std::chrono::seconds(8));
    logger->Stop();
    t.join();
}