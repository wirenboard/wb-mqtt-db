#pragma once

#include <wbmqtt/utils.h>
#include <wbmqtt/mqtt_wrapper.h>
#include <wbmqtt/mqttrpc.h>
#include <chrono>
#include <string>
#include <set>
#include <unordered_map>
#include  "SQLiteCpp/SQLiteCpp.h"

static const char * DB_BACKUP_FILE_EXTENSION = ".backup";
inline std::string BackupFileName(const std::string &filename) {
    return filename + DB_BACKUP_FILE_EXTENSION;
}

struct TChannelName
{
    std::string Device;
    std::string Control;

    bool operator==(const TChannelName& rhs) const {
        return std::tie(this->Device, this->Control) == std::tie(rhs.Device, rhs.Control);
    }

};

// hasher for TChannelName
namespace std {
    template<>
    struct hash<TChannelName>
    {
        typedef TChannelName argument_type;
        typedef std::size_t result_type;

        result_type operator()(const argument_type &s) const
        {
            return std::hash<std::string>()(s.Device) ^
                   std::hash<std::string>()(s.Control);
        }
    };
};

std::ostream& operator<<(std::ostream& out, const struct TChannelName &name);

struct TChannel
{
    TChannelName Name;

    std::string LastValue;

    struct {
        int ValueCount = 0;
        double Sum = 0.0;
        double Min = 0.0;
        double Max = 0.0;

        void Reset() {
            ValueCount = 0;
            Sum = Min = Max = 0.0;
        }
    } Accumulator;

    std::chrono::steady_clock::time_point LastProcessed;

    int RowCount;

    bool Changed = false;
    bool Accumulated = false;
    bool Retained = false;
};

class TMQTTDBLogger: public TMQTTPrefixedWrapper
{
public:
    TMQTTDBLogger(const TMQTTDBLogger::TConfig& mqtt_config, const TMQTTDBLoggerConfig config,
                  std::string prefix = "", std::string user = "", std::string password = "");
    ~TMQTTDBLogger();

    void OnConnect(int rc);
    void OnMessage(const struct mosquitto_message *message);
    void OnSubscribe(int mid, int qos_count, const int *granted_qos);

    void Init2();

    Json::Value GetValues(const Json::Value& input);
    Json::Value GetChannels(const Json::Value& input);

    std::chrono::steady_clock::time_point ProcessTimer(std::chrono::steady_clock::time_point next_call);

private:
    void InitDB();
    void CreateTables();
    void CreateIndices();
    int GetOrCreateChannelId(const TChannelName& channel);
    int GetOrCreateDeviceId(const std::string& device);
    void InitChannelIds();
    void InitDeviceIds();
    void InitGroupIds();
    void InitCaches();

    bool CheckBackupFile();
    void CreateBackupFile();
    void RemoveBackupFile();
    void RestoreBackupFile();

    int ReadDBVersion();
    void UpdateDB(int prev_version);
    bool UpdateAccumulator(int channel_id, const std::string &payload);
    void WriteChannel(TChannel &ch, TLoggingGroup &group);

    std::tuple<int, int> GetOrCreateIds(const std::string &device, const std::string &control);
    std::tuple<int, int> GetOrCreateIds(const std::string &topic);
    std::tuple<int, int> GetOrCreateIds(const TChannelName &name);

    std::string Mask;
    std::unique_ptr<SQLite::Database> DB;
    TMQTTDBLoggerConfig LoggerConfig;
    std::shared_ptr<TMQTTRPCServer> RPCServer;
    std::unordered_map<TChannelName, int> ChannelIds;
    std::unordered_map<std::string, int> DeviceIds;

    std::unordered_map<int, TChannel> ChannelDataCache;
    std::unordered_map<int, int> GroupRowNumberCache;

    const int DBVersion = WB_DB_VERSION;
};
