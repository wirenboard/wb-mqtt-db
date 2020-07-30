#pragma once

#include <chrono>
#include <queue>
#include <string>
#include <unordered_map>

#include <wblib/mqtt.h>
#include <wblib/rpc.h>

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

/**
 * @brief The class holds minimum, maximum and summary of a series of values
 */
struct TAccumulator
{
    uint32_t ValueCount = 0;
    double   Sum        = 0.0;
    double   Min        = 0.0;
    double   Max        = 0.0;

    //! Clear state. All fields are set to 0
    void Reset();

    /**
     * @brief Try to convert mqttPayload to number and hold it
     *
     * @param mqttPayload a string got from MQTT
     * @return true mqttPayload is number and it is processed
     * @return false mqttPayload can't be converted to number
     */
    bool Update(const std::string& mqttPayload);

    //! ValueCount > 1
    bool HasValues() const;

    //! Average accumulated value or 0
    double Average() const;
};

//! Information about speceific channel
struct TChannel
{
    static const int UNDEFIDED_ID;

    //! Unique id in storage
    int StorageId = UNDEFIDED_ID;

    std::string LastValue;

    TAccumulator Accumulator;

    //! Last value receive time
    std::chrono::system_clock::time_point LastValueTime;

    //! Channel's last save time
    std::chrono::steady_clock::time_point LastSaved;

    //! Number of records of the channel in DB
    int RecordCount = 0;

    //! True if channel's LastValue has been modified since last save to storage
    bool Changed = false;

    bool Retained = false;
};

//! A group of channels with storage settings
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

    //! Check if mqttTopic matches any of MqttTopicPatterns
    bool MatchPatterns(const std::string& mqttTopic) const;

    //! Calculate timepoint of nearest save for the group
    std::chrono::steady_clock::time_point GetNextSaveCheckTime() const;
};

struct TLoggerCache
{
    std::vector<TLoggingGroup> Groups;
};

//!
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

/**
 * @brief An interface for storages
 */
class IStorage
{
public:
    virtual ~IStorage();

    /**
     * @brief Load information about stored channels
     *
     * @param cache the object to fill with data from DB
     */
    virtual void Load(TLoggerCache& cache) = 0;

    /**
     * @brief Write channel data into storage. One must call Commit to finalaze writing.
     */
    virtual void WriteChannel(const TChannelName& channelName,
                              TChannel&           channel,
                              TLoggingGroup&      group) = 0;

    /**
     * @brief Save all modifications
     */
    virtual void Commit() = 0;

    /**
     * @brief Get records from storage according to constraints, call visitors ProcessRecord for every
     * record
     *
     * @param visitor an object 
     * @param channels get recods only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting from the id
     * @param maxRecords maximum records to get from storage
     * @param minInterval minimum time between records
     */
    virtual void GetRecords(IRecordsVisitor&                      visitor,
                            const std::vector<TChannelName>&      channels,
                            std::chrono::system_clock::time_point startTime,
                            std::chrono::system_clock::time_point endTime,
                            int64_t                               startId,
                            uint32_t                              maxRecords,
                            std::chrono::milliseconds             minInterval) = 0;
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
        WBMQTT::TMqttMessage                  Message;
        std::chrono::system_clock::time_point ReceiveTime;
    };

    std::chrono::steady_clock::time_point ProcessTimer(
        std::chrono::steady_clock::time_point nextSaveTime);

    void ProcessMessages(std::queue<TMqttMsg>& messages);

    Json::Value GetChannels(const Json::Value& params);
    Json::Value GetValues(const Json::Value& params);

    std::mutex                CacheMutex;
    TLoggerCache              Cache;
    WBMQTT::PMqttClient       MqttClient;
    std::mutex                StorageMutex;
    std::unique_ptr<IStorage> Storage;
    WBMQTT::PMqttRpcServer    RpcServer;
    std::mutex                Mutex;
    std::condition_variable   WakeupCondition;
    bool                      Active;
    std::queue<TMqttMsg>      MessagesQueue;
    std::chrono::seconds      GetValuesRpcRequestTimeout;
};

//! RAII-style spend time benchmark. Calculates time period between construction a nd destruction of
//! an object, prints the time into log.
class TBenchmark
{
    std::string                                    Message;
    std::chrono::high_resolution_clock::time_point Start;
    bool                                           Enabled;

public:
    /**
     * @brief Construct a new TBenchmark object
     *
     * @param message prefix for log message with spend time
     * @param enabled true - print and calculate spend time in destructor, false - do nothig, wait
     * Enable call
     */
    TBenchmark(const std::string& message, bool enabled = true);
    ~TBenchmark();

    /**
     * @brief Enables spend time calculation
     */
    void Enable();
};