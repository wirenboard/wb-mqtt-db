#pragma once

#include "storage.h"
#include <queue>
#include <mutex>
#include <condition_variable>

#include <wblib/mqtt.h>
#include <wblib/rpc.h>
#include <wblib/filters.h>

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

class TMQTTDBLoggerRpcHandler
{
public:
    TMQTTDBLoggerRpcHandler(const TLoggerCache&    cache,
                            IStorage&              storage,
                            std::chrono::seconds   getValuesRpcRequestTimeout);

    void Register(WBMQTT::TMqttRpcServer& rpcServer);

private:
    Json::Value GetChannels(const Json::Value& params);
    Json::Value GetValues(const Json::Value& params);

    const TLoggerCache&    Cache;
    IStorage&              Storage;
    std::chrono::seconds   GetValuesRpcRequestTimeout;
};

class TMqttDbLoggerMessageHandler
{
public:
    TMqttDbLoggerMessageHandler(TLoggerCache& cache, IStorage& storage, std::unique_ptr<IChannelWriter> channelWriter);

    std::chrono::steady_clock::time_point ProcessTimer(std::chrono::steady_clock::time_point currentTime);

    void ProcessMessages(std::queue<TValueFromMqtt>& messages);
private:

    void CheckChannelOverflow(const TLoggingGroup& group, TChannelInfo& channel);
    void CheckGroupOverflow(const TLoggingGroup& group);

    TLoggerCache&                   Cache;
    IStorage&                       Storage;
    std::unique_ptr<IChannelWriter> ChannelWriter;
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
    TLoggerCache                      Cache;
    WBMQTT::PDeviceDriver             Driver;
    std::unique_ptr<IStorage>         Storage;
    WBMQTT::PMqttRpcServer            RpcServer;
    std::mutex                        Mutex;
    std::condition_variable           WakeupCondition;
    bool                              Active;
    std::queue<TValueFromMqtt>        MessagesQueue;
    std::shared_ptr<TControlFilter>   Filter;
    WBMQTT::PDriverEventHandlerHandle EventHandle;
    TMqttDbLoggerMessageHandler       MessageHandler;
    TMQTTDBLoggerRpcHandler           RpcHandler;
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
