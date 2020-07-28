#pragma once

#include <chrono>
#include <queue>
#include <string>
#include <unordered_map>

#include <wblib/mqtt.h>
#include <wblib/rpc.h>
#include <wblib/thread_safe.h>

//! Records from DB will be deleted on limit * (1 + RECORDS_CLEAR_THRESHOLDR) entries
const float RECORDS_CLEAR_THRESHOLDR = 0.02;

//! loop will be interrupted at least once in this interval (in ms) for DB update event
const int WB_DB_LOOP_TIMEOUT_MS = 10;

/**
 * @brief Device name and control name pair for identificaition of a control
 */
struct TChannelName
{
    std::string Device;  //! Device name from MQTT /devices/XXXX
    std::string Control; //! Control name from MQTT /devices/+/controls/XXXX

    TChannelName() = default;

    TChannelName(const std::string& mqttTopic);

    TChannelName(const std::string& device, const std::string& control);

    bool operator==(const TChannelName& rhs) const;
};

// hasher for TChannelName
namespace std
{
    template <> struct hash<TChannelName>
    {
        typedef TChannelName argument_type;
        typedef std::size_t  result_type;

        result_type operator()(const argument_type& s) const
        {
            return std::hash<std::string>()(s.Device) ^ std::hash<std::string>()(s.Control);
        }
    };
}; // namespace std

std::ostream& operator<<(std::ostream& out, const struct TChannelName& name);

struct TAccumulator
{
    int    ValueCount = 0;
    double Sum        = 0.0;
    double Min        = 0.0;
    double Max        = 0.0;

    void Reset();
    bool Update(const std::string& mqttPayload);
};

struct TChannel
{
    static const int UNDEFIDED_ID;

    //! Unique id in storage
    int StorageId = UNDEFIDED_ID;

    std::string LastValue;

    TAccumulator Accumulator;

    //! Channel's last modification time
    std::chrono::system_clock::time_point LastChanged;

    //! Channel's last save time
    std::chrono::steady_clock::time_point LastSaved;

    //! Number of records of the channel in DB
    int RecordCount = 0;

    //! True if channel's LastValue has been modified since last save to storage
    bool Changed = false;

    bool Accumulated = false;
    bool Retained    = false;
};

struct TLoggingGroup
{
    std::vector<std::string> MqttTopicPatterns;

    //! Maximum records in DB of a single channel in the group. Unlimited if 0
    int MaxChannelRecords = 0;

    //! Maximum records in DB of the group. Unlimited if 0
    int MaxRecords = 0;

    //! Interval of saving modified values
    std::chrono::seconds ChangedInterval = std::chrono::seconds(0);

    //! Interval of saving unmodified values
    std::chrono::seconds UnchangedInterval = std::chrono::seconds(0);

    //! Group name
    std::string Name;

    //! Unique id in storage
    int StorageId;

    std::unordered_map<TChannelName, TChannel> Channels;

    //! Modified values last saving time
    std::chrono::steady_clock::time_point LastSaved;

    //! Unmodified values last saving time
    std::chrono::steady_clock::time_point LastUSaved;

    //! Number of records of the group in DB
    int RecordCount = 0;
};

struct TLoggerCache
{
    std::vector<TLoggingGroup> Groups;
};

class IRecordsVisitor
{
public:
    virtual ~IRecordsVisitor();

    virtual bool ProcessRecord(int                                   recordId,
                               int                                   channelNameId,
                               const TChannelName&                   channelName,
                               double                                averageValue,
                               std::chrono::system_clock::time_point timestamp,
                               double                                minValue,
                               double                                maxValue,
                               bool                                  retain) = 0;

    virtual bool ProcessRecord(int                                   recordId,
                               int                                   channelNameId,
                               const TChannelName&                   channelName,
                               const std::string&                    value,
                               std::chrono::system_clock::time_point timestamp,
                               bool                                  retain) = 0;
};

class IStorage
{
public:
    virtual ~IStorage();

    virtual void Load(TLoggerCache& cache) = 0;

    virtual void WriteChannel(const TChannelName& channelName,
                              TChannel&           channel_data,
                              TLoggingGroup&      group) = 0;

    virtual void Commit() = 0;

    virtual void GetRecords(IRecordsVisitor&                      visitor,
                            const std::vector<TChannelName>&      channels,
                            std::chrono::system_clock::time_point startTime,
                            std::chrono::system_clock::time_point endTime,
                            int64_t                               startId,
                            uint32_t                              maxRecords,
                            std::chrono::milliseconds             minInterval) = 0;
};

struct TMqttMsg
{
    TLoggingGroup&                        Group;
    WBMQTT::TMqttMessage                  Message;
    std::chrono::system_clock::time_point ReceiveTime;
};

class TMQTTDBLogger
{
public:
    TMQTTDBLogger(WBMQTT::PMqttClient       mqttClient,
                  const TLoggerCache&       cache,
                  std::unique_ptr<IStorage> storage,
                  WBMQTT::PMqttRpcServer    rpcServer,
                  std::chrono::seconds      getValuesRpcRequestTimeout);

    ~TMQTTDBLogger();

    void Start();

    void Stop();

private:
    struct TMqttMsg
    {
        TLoggingGroup&                        Group;
        WBMQTT::TMqttMessage                  Message;
        std::chrono::system_clock::time_point ReceiveTime;
    };

    std::chrono::milliseconds ProcessTimer();

    void ProcessMessages(std::queue<TMqttMsg>& messages);

    Json::Value GetChannels(const Json::Value& params);
    Json::Value GetValues(const Json::Value& params);

    TLoggerCache                          Cache;
    WBMQTT::PMqttClient                   MqttClient;
    std::unique_ptr<IStorage>             Storage;
    WBMQTT::PMqttRpcServer                RpcServer;
    std::mutex                            Mutex;
    std::condition_variable               WakeupCondition;
    bool                                  Active;
    std::queue<TMqttMsg>                  MessagesQueue;
    std::chrono::seconds                  GetValuesRpcRequestTimeout;
    std::chrono::steady_clock::time_point NextSaveTime;
};

class TBenchmark
{
    std::string                                    Message;
    std::chrono::high_resolution_clock::time_point Start;
    bool                                           Enabled;

public:
    TBenchmark(const std::string& message, bool enabled = true);
    ~TBenchmark();

    void Enable();
};