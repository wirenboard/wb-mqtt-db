#pragma once

#include <chrono>
#include <queue>
#include <string>
#include <unordered_map>
#include <mutex>
#include <condition_variable>

#include <wblib/mqtt.h>
#include <wblib/rpc.h>
#include <wblib/filters.h>

class IStorage;

/**
 * @brief Device name and control name pair for identificaition of a control
 */
struct TChannelName
{
    std::string Device;  //! Device name from MQTT /devices/XXXX
    std::string Control; //! Control name from MQTT /devices/+/controls/XXXX

    TChannelName() = default;
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
 * @brief Device name and control name pair for identificaition of a control
 */
class TChannelInfo
{
    friend class IStorage; // Only IStorage can create and modify TChannelInfo

    int64_t                               Id;
    int                                   RecordCount;
    std::chrono::system_clock::time_point LastRecordTime;
    TChannelName                          Name;
    double                                Precision;

    TChannelInfo(int64_t id, const std::string& device, const std::string& control);

    TChannelInfo(const TChannelInfo&) = delete;
    TChannelInfo& operator=(const TChannelInfo&) = delete;
public:
    const TChannelName&                          GetName() const;
    int                                          GetRecordCount() const;
    const std::chrono::system_clock::time_point& GetLastRecordTime() const;
    int64_t                                      GetId() const;

    //! Value's precision if channel stores numbers. 0.0 - no rounding
    double GetPrecision() const;
};

typedef std::shared_ptr<TChannelInfo> PChannelInfo;

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
    std::string LastValue;

    TAccumulator Accumulator;

    bool HasUnsavedMessages = false;

    //! Channel's last save time
    std::chrono::steady_clock::time_point LastSaved;

    //! Receive time of a last message from MQTT
    std::chrono::system_clock::time_point LastDataTime;

    //! True if channel's LastValue has been modified since last save to storage
    bool Changed = false;

    bool Retained = false;

    double Precision = 0.0;
};

struct TValueFromMqtt
{
    std::string                           Device;
    std::string                           Control;
    std::string                           Value;
    std::string                           Type;
    double                                Precision = 0.0;
    std::chrono::system_clock::time_point Time;
};

/**
 * @brief Update Precision of the channel according to message received from MQTT.
 *        A value from /meta/precision is used if present.
 *        If /meta/precision is missing and isNumber == true,
 *        try to set precision according to value's fractional part.
 * 
 * @param channelData channel to update
 * @param msg message from MQTT
 * @param isNumber flag indicating that message contains number
 */
void UpdatePrecision(TChannel& channelData, const TValueFromMqtt& msg, bool isNumber);

//! A group of channels with storage settings
struct TLoggingGroup
{
    typedef std::unordered_map<TChannelName, std::pair<PChannelInfo, TChannel>> TChannelsMap;

    std::vector<TChannelName> ControlPatterns;

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

    TChannelsMap Channels;

    //! Unmodified values last saving time
    std::chrono::steady_clock::time_point LastUSaved;

    //! Check if device/control matches any of MqttTopicPatterns
    bool MatchPatterns(const std::string& device, const std::string& control) const;

    TChannel& GetChannelData(const TChannelName& channelName);
};

std::vector<PChannelInfo> GetChannelInfos(const TLoggingGroup& group);
uint32_t GetRecordCount(const TLoggingGroup& group);

struct TLoggerCache
{
    std::vector<TLoggingGroup> Groups;
};

class IRecordsVisitor
{
public:
    virtual ~IRecordsVisitor() = default;

    virtual bool ProcessRecord(int                                   recordId,
                               const TChannelInfo&                   channel,
                               double                                averageValue,
                               std::chrono::system_clock::time_point timestamp,
                               double                                minValue,
                               double                                maxValue,
                               bool                                  retain) = 0;

    virtual bool ProcessRecord(int                                   recordId,
                               const TChannelInfo&                   channel,
                               const std::string&                    value,
                               std::chrono::system_clock::time_point timestamp,
                               bool                                  retain) = 0;
};

class IChannelVisitor
{
public:
    virtual ~IChannelVisitor() = default;

    virtual void ProcessChannel(PChannelInfo channel) = 0;
};

/**
 * @brief An interface for storages. All methods must be threadsafe
 */
class IStorage
{
public:
    virtual ~IStorage() = default;

    virtual PChannelInfo CreateChannel(const TChannelName& channelName) = 0;

    /**
     * @brief Set channel's precision. One must call Commit to finalaze writing to storage.
     */
    virtual void SetChannelPrecision(TChannelInfo& channelInfo, double precision) = 0;

    /**
     * @brief Write channel data into storage. One must call Commit to finalaze writing.
     */
    virtual void WriteChannel(TChannelInfo&                         channelInfo,
                              const std::string&                    value,
                              const std::string&                    minimum,
                              const std::string&                    maximum,
                              bool                                  retained,
                              std::chrono::system_clock::time_point time) = 0;

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

    /**
     * @brief Get channels from storage
     */
    virtual void GetChannels(IChannelVisitor& visitor) = 0;

    virtual void DeleteRecords(TChannelInfo& channel, uint32_t count) = 0;
    virtual void DeleteRecords(const std::vector<PChannelInfo>& channels, uint32_t count) = 0;

protected:
    PChannelInfo CreateChannelPrivate(uint64_t id, const std::string& device, const std::string& control);
    void SetRecordCount(TChannelInfo& channel, int recordCount);
    void SetLastRecordTime(TChannelInfo& channel, const std::chrono::system_clock::time_point& time);
    void SetPrecision(TChannelInfo& channel, double precision);
    const std::unordered_map<TChannelName, PChannelInfo>& GetChannelsPrivate() const;
    PChannelInfo FindChannel(const TChannelName& channelName) const;

private:
    std::unordered_map<TChannelName, PChannelInfo> Channels;
};

class TControlFilter: public WBMQTT::TDeviceFilter
{
    std::vector<WBMQTT::TDeviceControlPair> Controls;
public:
    void addControlPatterns(const std::vector<TChannelName>& patterns);

    std::vector<WBMQTT::TDeviceControlPair> Topics() const override;

    bool MatchTopic(const std::string& topic) const override;
};

class IChannelWriter
{
public:
    virtual ~IChannelWriter() = default;

    virtual void WriteChannel(IStorage&                             storage, 
                              TChannelInfo&                         channelInfo, 
                              const TChannel&                       channelData,
                              std::chrono::system_clock::time_point writeTime,
                              const std::string&                    groupName) = 0;
};

class TChannelWriter: public IChannelWriter
{
public:
    void WriteChannel(IStorage&                             storage, 
                      TChannelInfo&                         channelInfo, 
                      const TChannel&                       channelData,
                      std::chrono::system_clock::time_point writeTime,
                      const std::string&                    groupName) override;
};

class TMQTTDBLogger
{
public:
    TMQTTDBLogger(WBMQTT::PDeviceDriver           driver,
                  const TLoggerCache&             cache,
                  std::unique_ptr<IStorage>       storage,
                  WBMQTT::PMqttRpcServer          rpcServer,
                  std::unique_ptr<IChannelWriter> channelWriter,
                  std::chrono::seconds            getValuesRpcRequestTimeout);

    ~TMQTTDBLogger();

    void Start();

    void Stop();

private:
    std::chrono::steady_clock::time_point ProcessTimer(
        std::chrono::steady_clock::time_point currentTime);

    void ProcessMessages(std::queue<TValueFromMqtt>& messages);

    void CheckChannelOverflow(const TLoggingGroup& group, TChannelInfo& channel);
    void CheckGroupOverflow(const TLoggingGroup& group);

    Json::Value GetChannels(const Json::Value& params);
    Json::Value GetValues(const Json::Value& params);

    TLoggerCache                      Cache;
    WBMQTT::PDeviceDriver             Driver;
    std::unique_ptr<IStorage>         Storage;
    WBMQTT::PMqttRpcServer            RpcServer;
    std::mutex                        Mutex;
    std::condition_variable           WakeupCondition;
    bool                              Active;
    std::queue<TValueFromMqtt>        MessagesQueue;
    std::chrono::seconds              GetValuesRpcRequestTimeout;
    std::shared_ptr<TControlFilter>   Filter;
    WBMQTT::PDriverEventHandlerHandle EventHandle;
    std::unique_ptr<IChannelWriter>   ChannelWriter;
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

class TJsonRecordsVisitor : public IRecordsVisitor
{
    int                                   ProtocolVersion;
    int                                   RowLimit;
    int                                   RowCount;
    std::chrono::steady_clock::time_point StartTime;
    std::chrono::steady_clock::duration   Timeout;

    bool CommonProcessRecord(Json::Value&                          row,
                             int                                   recordId,
                             const TChannelInfo&                   channel,
                             std::chrono::system_clock::time_point timestamp,
                             bool                                  retain);

public:
    Json::Value Root;

    TJsonRecordsVisitor(int protocolVersion, int rowLimit, std::chrono::steady_clock::duration timeout);

    bool ProcessRecord(int                                   recordId,
                       const TChannelInfo&                   channel,
                       const std::string&                    value,
                       std::chrono::system_clock::time_point timestamp,
                       bool                                  retain) override;

    bool ProcessRecord(int                                   recordId,
                       const TChannelInfo&                   channel,
                       double                                averageValue,
                       std::chrono::system_clock::time_point timestamp,
                       double                                minValue,
                       double                                maxValue,
                       bool                                  retain) override;
};
