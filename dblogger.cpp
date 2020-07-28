#include "dblogger.h"

#include "log.h"

#include <wblib/exceptions.h>
#include <wblib/json_utils.h>

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

TMQTTDBLogger::TMQTTDBLogger(PMqttClient               mqttClient,
                             const TLoggerCache&       cache,
                             std::unique_ptr<IStorage> storage,
                             PMqttRpcServer            rpcServer,
                             std::chrono::seconds      getValuesRpcRequestTimeout)
    : Cache(cache), MqttClient(mqttClient), Storage(std::move(storage)), RpcServer(rpcServer),
      Active(false), GetValuesRpcRequestTimeout(getValuesRpcRequestTimeout)
{
    for (auto& group : Cache.Groups) {
        for (const auto& pattern : group.MqttTopicPatterns) {
            MqttClient->Subscribe(
                [&](const TMqttMessage& message) {
                    {
                        std::lock_guard<std::mutex> lg(Mutex);
                        MessagesQueue.push({group, message, system_clock::now()});
                    }
                    WakeupCondition.notify_all();
                },
                pattern);
        }
    }

    RpcServer->RegisterMethod("history",
                              "get_values",
                              bind(&TMQTTDBLogger::GetValues, this, placeholders::_1));
    RpcServer->RegisterMethod("history",
                              "get_channels",
                              bind(&TMQTTDBLogger::GetChannels, this, placeholders::_1));
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

    MqttClient->Start();
    RpcServer->Start();
    std::chrono::milliseconds timeout(5000);
    NextSaveTime = steady_clock::now() + timeout;
    while (Active) {
        queue<TMqttMsg> localQueue;
        {
            std::unique_lock<std::mutex> lk(Mutex);
            WakeupCondition.wait_for(lk, timeout);
            if (!Active) {
                return;
            }
            if (!MessagesQueue.empty()) {
                MessagesQueue.swap(localQueue);
            }
        }
        ProcessMessages(localQueue);
        timeout = ProcessTimer();
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
    while (!messages.empty()) {
        auto msg = messages.front();
        if (!msg.Message.Payload.empty()) {
            auto& channelData = msg.Group.Channels[TChannelName(msg.Message.Topic)];

            const char* status = "is same";
            if (msg.Message.Payload != channelData.LastValue) {
                status              = "IS CHANGED";
                channelData.Changed = true;
            }
            LOG(Debug) << "\"" << msg.Group.Name << "\" " << msg.Message.Topic << ": \""
                       << msg.Message.Payload << "\" " << status;

            if (channelData.Accumulator.Update(msg.Message.Payload)) {
                channelData.Accumulated = true;
            }
            channelData.LastValue   = msg.Message.Payload;
            channelData.LastChanged = msg.ReceiveTime;
            channelData.Retained    = msg.Message.Retained;
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
    if ((now >= group.LastSaved + group.ChangedInterval) && channel.Changed)
        return true;
    if ((now >= group.LastUSaved + group.UnchangedInterval) && !channel.Changed &&
        channel.LastSaved >= group.LastUSaved)
        return true;
    return false;
}

chrono::milliseconds TMQTTDBLogger::ProcessTimer()
{
    auto startTime = steady_clock::now();

    if (NextSaveTime > startTime) {
        return duration_cast<milliseconds>(NextSaveTime - startTime);
    }

#ifndef NBENCHMARK
    TBenchmark benchmark("Bulk processing took ", false);
#endif

    chrono::seconds timeout = min(Cache.Groups[0].ChangedInterval, Cache.Groups[0].UnchangedInterval);

    for (auto& group : Cache.Groups) {

        bool processChanged = true;
        bool saved          = false;

        for (auto& channel : group.Channels) {
            const char* saveStatus = "nothing to save";
            if (ShouldWriteChannel(startTime, group, channel.second)) {
                saveStatus = (channel.second.Changed ? "save changed" : "save UNCHANGED");
                Storage->WriteChannel(channel.first, channel.second, group);
                processChanged           = processChanged && channel.second.Changed;
                saved                    = true;
                channel.second.LastSaved = startTime;
                channel.second.Changed   = false;
            }
            LOG(Debug) << "\"" << group.Name << "\" " << channel.first << ": " << saveStatus;
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

        timeout = min(timeout, min(group.ChangedInterval, group.UnchangedInterval));
    }

    Storage->Commit();

    NextSaveTime = startTime + timeout;

    return duration_cast<milliseconds>(NextSaveTime - steady_clock::now());
}

Json::Value TMQTTDBLogger::GetChannels(const Json::Value& /*params*/)
{
#ifndef NBENCHMARK
    TBenchmark benchmark("RPC request took ");
#endif

    LOG(Info) << "Run RPC get_channels()";
    Json::Value result;

    for (const auto& group : Cache.Groups) {
        for (const auto& channel : group.Channels) {
            Json::Value values;
            values["items"]   = channel.second.RecordCount;
            values["last_ts"] = Json::Value::Int64(
                duration_cast<seconds>(channel.second.LastChanged.time_since_epoch()).count());

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
    TBenchmark benchmark("get_values() took ");
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

    int rowLimit = 0;
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
        LOG(Info) << Message << duration_cast<milliseconds>(stop - Start).count() << " ms";
    }
}

void TBenchmark::Enable()
{
    Enabled = true;
}
