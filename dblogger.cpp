#include "dblogger.h"

#include "log.h"

#include <wblib/exceptions.h>
#include <wblib/json_utils.h>
#include <wblib/mqtt.h>

using namespace std;
using namespace std::chrono;
using namespace WBMQTT;

#define LOG(logger) ::logger.Log() << "[dblogger] "

namespace
{
    //! Records from DB will be deleted on limit * (1 + RECORDS_CLEAR_THRESHOLDR) entries
    const float RECORDS_CLEAR_THRESHOLDR = 0.02;

    class TGroupsLoader: public IChannelVisitor
    {
        TLoggerCache& Cache;
    public:
        TGroupsLoader(TLoggerCache& cache): Cache(cache)
        {}

        void ProcessChannel(PChannelInfo channel) override
        {
            for (auto& group: Cache.Groups) {
                std::string topic("/devices/" + channel->GetName().Device + "/controls/" + channel->GetName().Control);
                if (group.MatchPatterns(topic)) {
                    group.Channels[channel->GetName()].first = channel;
                }
            }
        }
    };
}

namespace WBMQTT
{
    namespace JSON
    {

        template <> inline bool Is<milliseconds>(const Json::Value& value)
        {
            return value.isNumeric();
        }

        template <> inline milliseconds As<milliseconds>(const Json::Value& value)
        {
            auto t = value.asInt64();
            if (t < 0) {
                t = 0;
            }
            return milliseconds(t);
        }

        template <> inline bool Is<system_clock::time_point>(const Json::Value& value)
        {
            return value.isNumeric();
        }

        template <>
        inline system_clock::time_point As<system_clock::time_point>(const Json::Value& value)
        {
            return system_clock::time_point(seconds(value.asUInt64()));
        }
    } // namespace JSON
} // namespace WBMQTT

TChannelName::TChannelName(const std::string& device, const std::string& control)
    : Device(device), Control(control)
{
}

bool TChannelName::operator==(const TChannelName& rhs) const
{
    return std::tie(this->Device, this->Control) == std::tie(rhs.Device, rhs.Control);
}

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

TChannel& TLoggingGroup::GetChannelData(const TChannelName& channelName)
{
    return Channels[channelName].second;
}

std::vector<PChannelInfo> GetChannelInfos(const TLoggingGroup& group)
{
    std::vector<PChannelInfo> res;
    for (const auto& channel: group.Channels) {
        res.push_back(channel.second.first);
    }
    return res;
}

uint32_t GetRecordCount(const TLoggingGroup& group)
{
    uint32_t sum = 0;
    for (const auto& channel: group.Channels) {
        sum += channel.second.first->GetRecordCount();
    }
    return sum;
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
                MessagesQueue.push(message);
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

    TGroupsLoader loader(Cache);
    Storage->GetChannels(loader);

    auto nextSaveTime = steady_clock::now();

    RpcServer->RegisterMethod("history",
                              "get_values",
                              bind(&TMQTTDBLogger::GetValues, this, placeholders::_1));
    RpcServer->RegisterMethod("history",
                              "get_channels",
                              bind(&TMQTTDBLogger::GetChannels, this, placeholders::_1));

    MqttClient->Start();
    RpcServer->Start();
    for (auto& group : Cache.Groups) {
        group.LastUSaved = nextSaveTime;
    }

    while (Active) {
        steady_clock::time_point currentTime;
        queue<TMqttMessage>      localQueue;
        {
            std::unique_lock<std::mutex> lk(Mutex);
            if (MessagesQueue.empty()) {
                auto duration = duration_cast<milliseconds>(nextSaveTime - steady_clock::now());
                if (duration.count() > 0) {
                    WakeupCondition.wait_for(lk, duration + milliseconds(1));
                }
            }
            MessagesQueue.swap(localQueue);
            currentTime = steady_clock::now();
        }
        ProcessMessages(localQueue);
        nextSaveTime = ProcessTimer(currentTime);
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

void TMQTTDBLogger::ProcessMessages(queue<WBMQTT::TMqttMessage>& messages)
{
    while (!messages.empty()) {
        auto msg = messages.front();
        if (!msg.Payload.empty()) {
            for (auto& group : Cache.Groups) {
                if (group.MatchPatterns(msg.Topic)) {
                    auto items = WBMQTT::StringSplit(msg.Topic, "/");
                    if (items.size() > 4) {
                        auto& channelData = group.GetChannelData(TChannelName(items[2], items[4]));

                        const char* status = "is same";
                        if (msg.Payload != channelData.LastValue) {
                            status              = "IS CHANGED";
                            channelData.Changed = true;
                        }
                        LOG(Debug) << "\"" << group.Name << "\" " << msg.Topic << ": \"" << msg.Payload
                                << "\" " << status;

                        channelData.Accumulator.Update(msg.Payload);

                        channelData.LastValue          = msg.Payload;
                        channelData.Retained           = msg.Retained;
                        channelData.HasUnsavedMessages = true;
                    }
                    break;
                }
            }
        }
        messages.pop();
    }
}

std::ostream& operator<<(std::ostream& out, const TChannelName& name)
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
        return (now >= channel.LastSaved + group.ChangedInterval);
    }
    return (now >= group.LastUSaved + group.UnchangedInterval) && channel.HasUnsavedMessages;
}

struct TNextSaveTime
{
    bool                     IsEmpty = true;
    steady_clock::time_point Time;

    void Update(steady_clock::time_point newTime)
    {
        Time    = (IsEmpty ? newTime : min(Time, newTime));
        IsEmpty = false;
    }
};

void TMQTTDBLogger::CheckChannelOverflow(const TLoggingGroup& group, TChannelInfo& channel)
{
    if (group.MaxChannelRecords > 0) {
        if (channel.GetRecordCount() > group.MaxChannelRecords * (1 + RECORDS_CLEAR_THRESHOLDR)) {
            LOG(Warn) << "Channel data limit is reached: channel " << channel.GetName()
                      << ", row count " << channel.GetRecordCount() 
                      << ", limit " << group.MaxChannelRecords;
            Storage->DeleteRecords(channel, channel.GetRecordCount() - group.MaxChannelRecords);
        }
    }
}

void TMQTTDBLogger::CheckGroupOverflow(const TLoggingGroup& group)
{
    if (group.MaxRecords > 0) {
        auto groupRecordCount = GetRecordCount(group);
        if (groupRecordCount > group.MaxRecords * (1 + RECORDS_CLEAR_THRESHOLDR)) {
            LOG(Warn) << "Group data limit is reached: group " << group.Name 
                      << ", row count " << groupRecordCount
                      << ", limit " << group.MaxRecords;
            Storage->DeleteRecords(GetChannelInfos(group), groupRecordCount - group.MaxRecords);
        }
    }
}

steady_clock::time_point TMQTTDBLogger::ProcessTimer(steady_clock::time_point currentTime)
{
#ifndef NBENCHMARK
    TBenchmark benchmark("Bulk processing took", false);
#endif

    TNextSaveTime next;

    auto writeTime = system_clock::now();

    for (auto& group : Cache.Groups) {

        bool saved = false;

        for (auto& channel : group.Channels) {
            const char*         saveStatus  = "nothing to save";
            const TChannelName& channelName = channel.first;
            PChannelInfo&       channelInfo = channel.second.first;
            TChannel&           channelData = channel.second.second;
            if (ShouldWriteChannel(currentTime, group, channelData)) {
                saveStatus = (channelData.Changed ? "save changed" : "save UNCHANGED");
                if (!channelInfo) {
                    channelInfo = Storage->CreateChannel(channelName);
                }
                Storage->WriteChannel(*channelInfo, channelData, writeTime, group.Name);
                saved = true;

                if (!channelData.Changed) {
                    group.LastUSaved = currentTime;
                }

                channelData.Accumulator.Reset();
                channelData.LastSaved          = currentTime;
                channelData.Changed            = false;
                channelData.HasUnsavedMessages = false;

                CheckChannelOverflow(group, *channelInfo);
            } else {
                if (channelData.Changed) {
                    next.Update(channelData.LastSaved + group.ChangedInterval);
                }
            }
            if (::Debug.IsEnabled()) {
                LOG(Debug) << "\"" << group.Name << "\" " << channel.first << ": " << saveStatus;
            }
        }

        if (saved) {
            CheckGroupOverflow(group);
#ifndef NBENCHMARK
            benchmark.Enable();
#endif
        }

        if (currentTime >= group.LastUSaved + group.UnchangedInterval) {
            group.LastUSaved = group.LastUSaved + group.UnchangedInterval;
        }

        next.Update(group.LastUSaved + group.UnchangedInterval);
    }

    Storage->Commit();

    return next.Time;
}

class TJsonChannelsVisitor : public IChannelVisitor
{
public:
    Json::Value Root;

    void ProcessChannel(PChannelInfo channel) override
    {
        Json::Value values;
        values["items"] = channel->GetRecordCount();
        values["last_ts"] =
            Json::Value::Int64(duration_cast<seconds>(channel->GetLastRecordTime().time_since_epoch()).count());

        Root["channels"][channel->GetName().Device + "/" + channel->GetName().Control] = values;
    }
};

Json::Value TMQTTDBLogger::GetChannels(const Json::Value& /*params*/)
{
#ifndef NBENCHMARK
    TBenchmark benchmark("RPC request took");
#endif

    LOG(Debug) << "Run RPC get_channels()";
    TJsonChannelsVisitor visitor;
    Storage->GetChannels(visitor);
    return visitor.Root;
}

class TJsonRecordsVisitor : public IRecordsVisitor
{
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
    Json::Value Root;

    TJsonRecordsVisitor(int protocolVersion, int rowLimit, steady_clock::duration timeout)
        : ProtocolVersion(protocolVersion), RowLimit(rowLimit), RowCount(0), Timeout(timeout)
    {
        StartTime      = steady_clock::now();
        Root["values"] = Json::Value(Json::arrayValue);
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
    LOG(Debug) << "Run RPC get_values()";

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

    int rowLimit = std::numeric_limits<int>::max() - 1;
    JSON::Get(params, "limit", rowLimit);

    TJsonRecordsVisitor visitor(protocolVersion, rowLimit, timeout);

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

    try {
        // we request one extra row to know whether there are more than 'limit' available
        Storage->GetRecords(visitor,
                            channels,
                            timestamp_gt,
                            timestamp_lt,
                            startingRecordId,
                            rowLimit + 1,
                            minInterval);
    } catch (const std::exception& e) {
        LOG(Error) << e.what();
        throw;
    }
    return visitor.Root;
}

TBenchmark::TBenchmark(const string& message, bool enabled) : Message(message), Enabled(enabled)
{
    Start = high_resolution_clock::now();
}

TBenchmark::~TBenchmark()
{
    if (Enabled) {
        high_resolution_clock::time_point stop = high_resolution_clock::now();
        LOG(Debug) << Message << " " << duration_cast<milliseconds>(stop - Start).count() << " ms";
    }
}

void TBenchmark::Enable()
{
    Enabled = true;
}

TChannelInfo::TChannelInfo(int64_t id, const std::string& device, const std::string& control)
    : Id(id), RecordCount(0), Name(device, control)
{}

const TChannelName& TChannelInfo::GetName() const
{
    return Name;
}

int32_t TChannelInfo::GetRecordCount() const
{
    return RecordCount;
}

const std::chrono::system_clock::time_point& TChannelInfo::GetLastRecordTime() const
{
    return LastRecordTime;
}

int64_t TChannelInfo::GetId() const
{
    return Id;
}

PChannelInfo IStorage::CreateChannelPrivate(uint64_t id, const std::string& device, const std::string& control)
{
    PChannelInfo p(new TChannelInfo(id, device, control));
    Channels.emplace(TChannelName(device, control), p);
    return p;
}

void IStorage::SetRecordCount(TChannelInfo& channel, int recordCount)
{
    channel.RecordCount = (recordCount < 0) ? 0 : recordCount;
}

void IStorage::SetLastRecordTime(TChannelInfo& channel, const std::chrono::system_clock::time_point& time)
{
    channel.LastRecordTime = time;
}

const std::unordered_map<TChannelName, PChannelInfo>& IStorage::GetChannelsPrivate() const
{
    return Channels;
}

PChannelInfo IStorage::FindChannel(const TChannelName& channelName) const
{
    auto it = Channels.find(channelName);
    if (it != Channels.end()) {
        return it->second;
    }
    return PChannelInfo();
}
