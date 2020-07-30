#include "dblogger.h"

#include "log.h"

#include <wblib/exceptions.h>
#include <wblib/json_utils.h>
#include <wblib/mqtt.h>

using namespace std;
using namespace std::chrono;
using namespace WBMQTT;

#define LOG(logger) ::logger.Log() << "[dblogger] "

namespace WBMQTT
{
    namespace JSON
    {

        template <> inline bool Is<milliseconds>(const Json::Value& value)
        {
            return value.isUInt();
        }

        template <> inline milliseconds As<milliseconds>(const Json::Value& value)
        {
            return milliseconds(value.asUInt());
        }

        template <> inline bool Is<system_clock::time_point>(const Json::Value& value)
        {
            return value.isUInt();
        }

        template <>
        inline system_clock::time_point As<system_clock::time_point>(const Json::Value& value)
        {
            return system_clock::time_point(seconds(value.asUInt()));
        }
    } // namespace JSON
} // namespace WBMQTT

IStorage::~IStorage() {}

IRecordsVisitor::~IRecordsVisitor() {}

TChannelName::TChannelName(const string& mqttTopic)
{
    vector<string> tokens(StringSplit(mqttTopic, '/'));
    Device  = tokens[2];
    Control = tokens[4];
}

TChannelName::TChannelName(const std::string& device, const std::string& control)
    : Device(device), Control(control)
{
}

bool TChannelName::operator==(const TChannelName& rhs) const
{
    return std::tie(this->Device, this->Control) == std::tie(rhs.Device, rhs.Control);
}

const int TChannel::UNDEFIDED_ID = -1;

bool TAccumulator::Update(const string& payload)
{
    double value;

    // try to cast value to double and run stats
    try {
        value = stod(payload);
    } catch (...) {
        return false; // no processing for uncastable values
    }

    ++ValueCount;

    if (ValueCount == 1) {
        Min = Max = Sum = value;
    } else {
        if (Min > value)
            Min = value;
        if (Max < value)
            Max = value;
        Sum += value;
    }

    return true;
}

void TAccumulator::Reset()
{
    ValueCount = 0;
    Sum = Min = Max = 0.0;
}

bool TAccumulator::HasValues() const
{
    return (ValueCount > 1);
}

double TAccumulator::Average() const
{
    return (ValueCount > 0 ? Sum / double(ValueCount) : 0.0); // 0.0 - error value
}

bool TLoggingGroup::MatchPatterns(const std::string& mqttTopic) const
{
    for (auto& pattern : MqttTopicPatterns) {
        if (WBMQTT::TopicMatchesSub(pattern, mqttTopic)) {
            return true;
        }
    }
    return false;
}

std::chrono::steady_clock::time_point TLoggingGroup::GetNextSaveCheckTime() const
{
    return min(LastSaved + ChangedInterval, LastUSaved + UnchangedInterval);
}

TMQTTDBLogger::TMQTTDBLogger(PMqttClient               mqttClient,
                             const TLoggerCache&       cache,
                             std::unique_ptr<IStorage> storage,
                             PMqttRpcServer            rpcServer,
                             std::chrono::seconds      getValuesRpcRequestTimeout)
    : Cache(cache), MqttClient(mqttClient), Storage(std::move(storage)), RpcServer(rpcServer),
      Active(false), GetValuesRpcRequestTimeout(getValuesRpcRequestTimeout)
{
    std::vector<std::string> patterns;

    for (auto& group : Cache.Groups) {
        for (const auto& pattern : group.MqttTopicPatterns) {
            patterns.push_back(pattern);
        }
    }
    MqttClient->Subscribe(
        [&](const TMqttMessage& message) {
            {
                std::lock_guard<std::mutex> lg(Mutex);
                LOG(Info) << message.Topic;
                MessagesQueue.push({message, system_clock::now()});
            }
            WakeupCondition.notify_all();
        },
        patterns);
}

TMQTTDBLogger::~TMQTTDBLogger()
{
    try {
        Stop();
    } catch (const std::exception& e) {
        LOG(Error) << e.what();
    }
}

void TMQTTDBLogger::Start()
{
    {
        std::lock_guard<std::mutex> lg(Mutex);
        if (Active) {
            LOG(Error) << "Attempt to start already started driver";
            return;
        }
        Active = true;
    }


    Storage->Load(Cache);

    RpcServer->RegisterMethod("history",
                              "get_values",
                              bind(&TMQTTDBLogger::GetValues, this, placeholders::_1));
    RpcServer->RegisterMethod("history",
                              "get_channels",
                              bind(&TMQTTDBLogger::GetChannels, this, placeholders::_1));

    MqttClient->Start();
    RpcServer->Start();
    steady_clock::time_point nextSaveTime = steady_clock::now();
    for (auto& group : Cache.Groups) {
        group.LastSaved  = nextSaveTime;
        group.LastUSaved = nextSaveTime;
    }
    while (Active) {
        queue<TMqttMsg> localQueue;
        {
            std::unique_lock<std::mutex> lk(Mutex);
            WakeupCondition.wait_for(lk,
                                     duration_cast<seconds>(nextSaveTime - steady_clock::now()));
            if (!Active) {
                return;
            }
            if (!MessagesQueue.empty()) {
                MessagesQueue.swap(localQueue);
            }
        }
        ProcessMessages(localQueue);
        nextSaveTime = ProcessTimer(nextSaveTime);
    }
}

void TMQTTDBLogger::Stop()
{
    {
        std::lock_guard<std::mutex> lg(Mutex);
        if (!Active) {
            return;
        }
        Active = false;
    }
    WakeupCondition.notify_all();
    RpcServer->Stop();
    MqttClient->Stop();
}

void TMQTTDBLogger::ProcessMessages(queue<TMqttMsg>& messages)
{
    std::lock_guard<std::mutex> lg(CacheMutex);
    while (!messages.empty()) {
        auto msg = messages.front();
        if (!msg.Message.Payload.empty()) {
            for (auto& group : Cache.Groups) {
                if (group.MatchPatterns(msg.Message.Topic)) {
                    auto& channelData = group.Channels[TChannelName(msg.Message.Topic)];

                    const char* status = "is same";
                    if (msg.Message.Payload != channelData.LastValue) {
                        status              = "IS CHANGED";
                        channelData.Changed = true;
                    }
                    LOG(Info) << "\"" << group.Name << "\" " << msg.Message.Topic << ": \""
                              << msg.Message.Payload << "\" " << status;

                    channelData.Accumulator.Update(msg.Message.Payload);

                    channelData.LastValue     = msg.Message.Payload;
                    channelData.LastValueTime = msg.ReceiveTime;
                    channelData.Retained      = msg.Message.Retained;
                    break;
                }
            }
        }
        messages.pop();
    }
}

std::ostream& operator<<(std::ostream& out, const struct TChannelName& name)
{
    out << name.Device << "/" << name.Control;
    return out;
}

// check if current group is ready to process changed values
// or ready to process unchanged values
bool ShouldWriteChannel(steady_clock::time_point now,
                        const TLoggingGroup&     group,
                        const TChannel&          channel)
{
    if (channel.Changed) {
        return (now >= group.LastSaved + group.ChangedInterval);
    }
    return ((now >= channel.LastSaved + group.ChangedInterval) &&
            (now >= group.LastUSaved + group.UnchangedInterval));
}

steady_clock::time_point TMQTTDBLogger::ProcessTimer(steady_clock::time_point nextSaveTime)
{
    auto startTime = steady_clock::time_point(duration_cast<seconds>(steady_clock::now().time_since_epoch()));

    if (nextSaveTime > startTime) {
        return nextSaveTime;
    }

#ifndef NBENCHMARK
    TBenchmark benchmark("Bulk processing took", false);
#endif

    nextSaveTime = Cache.Groups[0].GetNextSaveCheckTime();
    std::lock_guard<std::mutex> lCache(CacheMutex);
    std::lock_guard<std::mutex> lStorage(StorageMutex);

    for (auto& group : Cache.Groups) {

        bool processChanged = true;
        bool saved          = false;

        for (auto& channel : group.Channels) {
            const char* saveStatus = "nothing to save";
            if (ShouldWriteChannel(startTime, group, channel.second)) {
                saveStatus = (channel.second.Changed ? "save changed" : "save UNCHANGED");
                Storage->WriteChannel(channel.first, channel.second, group);
                channel.second.Accumulator.Reset();

                processChanged           = processChanged && channel.second.Changed;
                saved                    = true;
                channel.second.LastSaved = startTime;
                channel.second.Changed   = false;
            }
            if (::Debug.IsEnabled()) {
                LOG(Debug) << "\"" << group.Name << "\" " << channel.first << ": " << saveStatus;
            }
        }

        if (saved) {
            group.LastSaved = startTime;
            if (!processChanged) {
                group.LastUSaved = startTime;
            }
#ifndef NBENCHMARK
            benchmark.Enable();
#endif
        }

        nextSaveTime = min(nextSaveTime, group.GetNextSaveCheckTime());
    }

    Storage->Commit();

    return nextSaveTime;
}

Json::Value TMQTTDBLogger::GetChannels(const Json::Value& /*params*/)
{
#ifndef NBENCHMARK
    TBenchmark benchmark("RPC request took");
#endif

    LOG(Info) << "Run RPC get_channels()";
    Json::Value result;
    std::lock_guard<std::mutex> lCache(CacheMutex);
    for (const auto& group : Cache.Groups) {
        for (const auto& channel : group.Channels) {
            Json::Value values;
            values["items"]   = channel.second.RecordCount;
            values["last_ts"] = Json::Value::Int64(
                duration_cast<seconds>(channel.second.LastValueTime.time_since_epoch()).count());

            result["channels"][channel.first.Device + "/" + channel.first.Control] = values;
        }
    }

    return result;
}

class TJsonRecordsVisitor : public IRecordsVisitor
{
    Json::Value&             Root;
    int                      ProtocolVersion;
    int                      RowLimit;
    int                      RowCount;
    steady_clock::time_point StartTime;
    steady_clock::duration   Timeout;

    bool CommonProcessRecord(Json::Value&                          row,
                             int                                   recordId,
                             int                                   channelNameId,
                             const TChannelName&                   channelName,
                             std::chrono::system_clock::time_point timestamp,
                             bool                                  retain)
    {
        if (steady_clock::now() - StartTime >= Timeout) {
            wb_throw(TRequestTimeoutException, "get_values");
        }

        if (RowLimit > 0 && RowCount >= RowLimit) {
            Root["has_more"] = true;
            return false;
        }

        if (ProtocolVersion == 1) {
            row["i"] = recordId;
            row["c"] = channelNameId;
            row["t"] =
                Json::Value::Int64(duration_cast<seconds>(timestamp.time_since_epoch()).count());
        } else {
            row["uid"]     = recordId;
            row["device"]  = channelName.Device;
            row["control"] = channelName.Control;
            row["timestamp"] =
                Json::Value::Int64(duration_cast<seconds>(timestamp.time_since_epoch()).count());
        }

        row["retain"] = retain;

        // append element to values list
        Root["values"].append(row);
        ++RowCount;

        return true;
    }

public:
    TJsonRecordsVisitor(Json::Value&           root,
                        int                    protocolVersion,
                        int                    rowLimit,
                        steady_clock::duration timeout)
        : Root(root), ProtocolVersion(protocolVersion), RowLimit(rowLimit), RowCount(0),
          Timeout(timeout)
    {
        StartTime = steady_clock::now();
    }

    bool ProcessRecord(int                                   recordId,
                       int                                   channelNameId,
                       const TChannelName&                   channelName,
                       const std::string&                    value,
                       std::chrono::system_clock::time_point timestamp,
                       bool                                  retain)
    {
        Json::Value row;
        row[(ProtocolVersion == 1) ? "v" : "value"] = value;
        return CommonProcessRecord(row, recordId, channelNameId, channelName, timestamp, retain);
    }

    bool ProcessRecord(int                                   recordId,
                       int                                   channelNameId,
                       const TChannelName&                   channelName,
                       double                                averageValue,
                       std::chrono::system_clock::time_point timestamp,
                       double                                minValue,
                       double                                maxValue,
                       bool                                  retain)
    {
        Json::Value row;

        row["min"]                                  = minValue;
        row["max"]                                  = maxValue;
        row[(ProtocolVersion == 1) ? "v" : "value"] = averageValue;

        return CommonProcessRecord(row, recordId, channelNameId, channelName, timestamp, retain);
    }
};

Json::Value TMQTTDBLogger::GetValues(const Json::Value& params)
{
    LOG(Info) << "Run RPC get_values()";

#ifndef NBENCHMARK
    TBenchmark benchmark("get_values() took");
#endif

    if (!params.isMember("channels"))
        wb_throw(TBaseException, "no channels specified");

    int protocolVersion = 0;
    JSON::Get(params, "ver", protocolVersion);
    if ((protocolVersion != 0) && (protocolVersion != 1)) {
        wb_throw(TBaseException, "unsupported request version");
    }

    steady_clock::duration timeout = GetValuesRpcRequestTimeout;
    if (params.isMember("request_timeout")) {
        timeout = chrono::seconds(params["request_timeout"].asInt());
    }

    Json::Value result;
    result["values"] = Json::Value(Json::arrayValue);

    int rowLimit = std::numeric_limits<int>::max() - 1;
    JSON::Get(params, "limit", rowLimit);

    TJsonRecordsVisitor visitor(result, protocolVersion, rowLimit, timeout);

    system_clock::time_point timestamp_gt;
    system_clock::time_point timestamp_lt = system_clock::now();

    if (params.isMember("timestamp")) {
        JSON::Get(params["timestamp"], "gt", timestamp_gt);
        JSON::Get(params["timestamp"], "lt", timestamp_lt);
    }

    int64_t startingRecordId = -1;
    if (params.isMember("uid")) {
        if (params["uid"].isMember("gt")) {
            startingRecordId = params["uid"]["gt"].asInt64();
        }
    }

    milliseconds minInterval(0);
    JSON::Get(params, "min_interval", minInterval);

    std::vector<TChannelName> channels;
    for (const auto& channelItem : params["channels"]) {
        if (!(channelItem.isArray() && (channelItem.size() == 2)))
            wb_throw(TBaseException, "'channels' items must be an arrays of size two ");
        channels.emplace_back(channelItem[0u].asString(), channelItem[1u].asString());
    }

    std::lock_guard<std::mutex> lStorage(StorageMutex);
    // we request one extra row to know whether there are more than 'limit' available
    Storage->GetRecords(visitor,
                        channels,
                        timestamp_gt,
                        timestamp_lt,
                        startingRecordId,
                        rowLimit + 1,
                        minInterval);

    return result;
}

TBenchmark::TBenchmark(const string& message, bool enabled) : Message(message), Enabled(enabled)
{
    Start = high_resolution_clock::now();
}

TBenchmark::~TBenchmark()
{
    if (Enabled) {
        high_resolution_clock::time_point stop = high_resolution_clock::now();
        LOG(Info) << Message << " " << duration_cast<milliseconds>(stop - Start).count() << " ms";
    }
}

void TBenchmark::Enable()
{
    Enabled = true;
}
