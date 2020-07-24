#include "dblogger.h"

#include "log.h"

#include <wblib/exceptions.h>
#include <wblib/json_utils.h>

using namespace std;
using namespace std::chrono;
using namespace WBMQTT;

#define LOG(logger) ::logger.Log() << "[dblogger] "

template <> inline bool JSON::Is<milliseconds>(const Json::Value& value)
{
    return value.isUInt();
}

template <> inline milliseconds JSON::As<milliseconds>(const Json::Value& value)
{
    return milliseconds(value.asUInt());
}

template <> inline bool JSON::Is<system_clock::time_point>(const Json::Value& value)
{
    return value.isUInt();
}

template <>
inline system_clock::time_point JSON::As<system_clock::time_point>(const Json::Value& value)
{
    return system_clock::time_point(seconds(value.asUInt()));
}

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

TMQTTDBLogger::TMQTTDBLogger(PMqttClient          mqttClient,
                             const TLoggerCache&  cache,
                             IStorage&            storage,
                             PMqttRpcServer       rpcServer,
                             std::chrono::seconds getValuesRpcRequestTimeout)
    : Cache(cache), MqttClient(mqttClient), Storage(storage), RpcServer(rpcServer),
      GetValuesRpcRequestTimeout(getValuesRpcRequestTimeout)
{
    for (auto& group : Cache.Groups) {
        for (const auto& pattern : group.MqttTopicPatterns) {
            MqttClient->Subscribe(
                [&](const TMqttMessage& message) {
                    MessagesQueue.Lock()->push({group, message, system_clock::now()});
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

void TMQTTDBLogger::Start()
{
    {
        std::lock_guard<std::mutex> lg(ActiveMutex);
        if (Active) {
            LOG(Error) << "Attempt to start already started driver";
            return;
        }
        Active = true;
    }
    MqttClient->Start();
    RpcServer->Start();
    steady_clock::time_point timeout = steady_clock::now();
    while (Active) {
        ProcessMessages();
        timeout = ProcessTimer(timeout);
    }
}

void TMQTTDBLogger::Stop()
{
    {
        std::lock_guard<std::mutex> lg(ActiveMutex);
        if (!Active) {
            LOG(Error) << "Attempt to stop not started driver";
        }
        Active = false;
    }
    RpcServer->Stop();
    MqttClient->Stop();
}

void TMQTTDBLogger::ProcessMessages()
{
    queue<TMqttMsg> localQueue;
    {
        MessagesQueue.Lock()->swap(localQueue);
    }

    while (!localQueue.empty()) {
        auto msg = localQueue.front();
        if (!msg.Message.Payload.empty()) {
            LOG(Debug) << "MQTT message from topic " << msg.Message.Topic << ": \""
                       << msg.Message.Payload << "\", parsed as group \"" << msg.Group.Name << "\"";

            auto& channelData = msg.Group.Channels[TChannelName(msg.Message.Topic)];

            if (msg.Message.Payload != channelData.LastValue) {
                LOG(Debug) << "Data has changed!";
                channelData.Changed = true;
            }

            if (channelData.Accumulator.Update(msg.Message.Payload)) {
                channelData.Accumulated = true;
            }
            channelData.LastValue   = msg.Message.Payload;
            channelData.LastChanged = msg.ReceiveTime;
            channelData.Retained    = msg.Message.Retained;
        }
        localQueue.pop();
    }
}

std::ostream& operator<<(std::ostream& out, const struct TChannelName& name)
{
    out << name.Device << "/" << name.Control;
    return out;
}

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

steady_clock::time_point TMQTTDBLogger::ProcessTimer(steady_clock::time_point next_call)
{
    auto now = steady_clock::now();

    if (next_call > now) {
        return next_call; // there is some time to wait
    }

#ifndef NBENCHMARK
    TBenchmark benchmark("Bulk processing took ", false);
#endif

    for (auto& group : Cache.Groups) {

        bool process_changed = true;

        for (auto& channel : group.Channels) {
            // check if current group is ready to process changed values
            // or ready to process unchanged values
            if (ShouldWriteChannel(now, group, channel.second)) {
                LOG(Info) << "Processing channel " << channel.first << " from group " << group.Name
                          << (channel.second.Changed ? ", changed" : ", UNCHANGED");

                process_changed = process_changed && channel.second.Changed;
                Storage.WriteChannel(channel.first, channel.second, group);
                channel.second.Changed   = false;
                channel.second.LastSaved = now;
                group.LastSaved          = now;

#ifndef NBENCHMARK
                benchmark.Enable();
#endif
            }
        }

        if (!process_changed) {
            group.LastUSaved = now;
        }

        // select minimal next call time
        if (next_call > now + min(group.ChangedInterval, group.UnchangedInterval)) {
            next_call = now + min(group.ChangedInterval, group.UnchangedInterval);
        }
    }

    Storage.Commit();

    return next_call;
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
    Storage.GetRecords(visitor,
                       channels,
                       timestamp_gt,
                       timestamp_lt,
                       startingRecordId,
                       rowLimit + 1,
                       minInterval);

    return result;
}

// Json::Value TMQTTDBLogger::GetValues(const Json::Value& params)
// {
//     LOG(NOTICE) << "Run RPC get_values()";

// #ifndef NBENCHMARK
//     high_resolution_clock::time_point t1 = high_resolution_clock::now();
// #endif

//     steady_clock::time_point start_time = steady_clock::now();

//     Json::Value result;
//     int         limit           = -1;
//     long long   timestamp_gt    = 0;
//     int64_t     uid_gt          = -1;
//     long long   timestamp_lt    = 10675199167; // magic?
//     int         req_ver         = 0;
//     int         min_interval_ms = 0;

//     steady_clock::duration timeout = LoggerConfig.RequestTimeout;
//     if (params.isMember("request_timeout")) {
//         timeout = chrono::seconds(params["request_timeout"].asInt());
//     }

//     if (params.isMember("ver")) {
//         req_ver = params["ver"].asInt();
//     }

//     if ((req_ver != 0) && (req_ver != 1)) {
//         throw TBaseException("unsupported request version");
//     }

//     if (params.isMember("timestamp")) {
//         if (params["timestamp"].isMember("gt"))
//             timestamp_gt = params["timestamp"]["gt"].asInt64();

//         if (params["timestamp"].isMember("lt"))
//             timestamp_lt = params["timestamp"]["lt"].asInt64();
//     }

//     if (params.isMember("uid")) {
//         if (params["uid"].isMember("gt")) {
//             uid_gt = params["uid"]["gt"].asInt64();
//         }
//     }

//     if (params.isMember("limit"))
//         limit = params["limit"].asInt();

//     if (params.isMember("min_interval")) {
//         min_interval_ms = params["min_interval"].asInt();
//         if (min_interval_ms < 0) {
//             min_interval_ms = 0;
//         }
//     }

//     timestamp_gt *= 1000;
//     timestamp_lt *= 1000;

//     if (!params.isMember("channels"))
//         throw TBaseException("no channels specified");

//     result["values"] = Json::Value(Json::arrayValue);

//     // version 3.7 can't always figure out to use the proper index
//     string get_values_query_str;

//     if (min_interval_ms > 0)
//         get_values_query_str =
//             "SELECT uid, device, channel, AVG(value), timestamp / 1000, MIN(min), MAX(max), \
//                                 retained  FROM data INDEXED BY data_topic_timestamp WHERE ";
//     else
//         get_values_query_str = "SELECT uid, device, channel, value, timestamp / 1000, min, max, \
//                                 retained FROM data INDEXED BY data_topic_timestamp WHERE ";

//     if (!params["channels"].empty()) {
//         get_values_query_str += "channel IN ( ";
//         for (size_t i = 0; i < params["channels"].size(); ++i) {
//             if (i > 0)
//                 get_values_query_str += ", ";

//             get_values_query_str += "?";
//         }
//         get_values_query_str += ") AND ";
//     }

//     get_values_query_str += "timestamp > ? AND timestamp < ? AND uid > ? ";

//     if (min_interval_ms > 0) {
//         get_values_query_str += " GROUP BY (timestamp * ? / 86400000), channel ";
//     }

//     get_values_query_str += " ORDER BY uid ASC LIMIT ?";

//     SQLite::Statement get_values_query(*DB, get_values_query_str);
//     get_values_query.reset();

//     int                param_num = 0;
//     std::map<int, int> query_channel_ids; // map channel ids to they serial number in the request
//     std::map<int, TChannelName>
//            channel_names; // map channel ids to the their names  ((device, control) pairs)
//     size_t i = 0;
//     for (const auto& channel_item : params["channels"]) {
//         if (!(channel_item.isArray() && (channel_item.size() == 2)))
//             throw TBaseException("'channels' items must be an arrays of size two ");

//         const TChannelName channel = {channel_item[0u].asString(), channel_item[1u].asString()};

//         int channel_int_id = GetOrCreateChannelId(channel);

//         get_values_query.bind(++param_num, channel_int_id);

//         query_channel_ids[channel_int_id] = (i++);
//         channel_names[channel_int_id]     = channel;
//     }

//     get_values_query.bind(++param_num, timestamp_gt);
//     get_values_query.bind(++param_num, timestamp_lt);
//     get_values_query.bind(++param_num, static_cast<sqlite3_int64>(uid_gt));

//     if (min_interval_ms > 0) {
//         int day_fraction = 86400000 / min_interval_ms /* ms in day */;
//         LOG(DEBUG) << "day: fraction :" << day_fraction;
//         get_values_query.bind(++param_num, day_fraction);
//     }

//     get_values_query.bind(
//         ++param_num,
//         limit + 1); // we request one extra row to know whether there are more than 'limit'
//         available

//     int  row_count = 0;
//     bool has_more  = false;

//     while (1) {

//         // check timeout
//         if (steady_clock::now() - start_time >= timeout) {
//             throw TRequestTimeoutException("get_values");
//         }

//         if (!get_values_query.executeStep())
//             break;

//         if (row_count >= limit) {
//             has_more = true;
//             break;
//         }

//         Json::Value row;

//         int uid = static_cast<int>(get_values_query.getColumn(0));

//         if (req_ver == 0) {
//             row["uid"] = uid;

//             const TChannelName& channel = channel_names[get_values_query.getColumn(2)];
//             row["device"]               = channel.Device;
//             row["control"]              = channel.Control;
//         } else if (req_ver == 1) {
//             row["i"] = uid;
//             row["c"] = query_channel_ids[get_values_query.getColumn(2)];
//         }

//         // if there are min and max values, send'em too
//         if (get_values_query.getColumn(5).getType() != SQLITE_NULL) {
//             row["min"]                          =
//             static_cast<double>(get_values_query.getColumn(5)); row["max"] =
//             static_cast<double>(get_values_query.getColumn(6)); row[(req_ver == 1) ? "v" : "value"]
//             = static_cast<double>(get_values_query.getColumn(3));
//         } else {
//             row[(req_ver == 1) ? "v" : "value"] = get_values_query.getColumn(3).getText();
//         }

//         // add retained flag if it is set
//         if (static_cast<int>(get_values_query.getColumn(7)) > 0) {
//             row["retain"] = true;
//         }

//         // send timestamp
//         row[(req_ver == 1) ? "t" : "timestamp"] =
//             static_cast<long long>(get_values_query.getColumn(4));

//         // append element to values list
//         result["values"].append(row);
//         row_count += 1;
//     }

//     if (has_more) {
//         result["has_more"] = true;
//     }

// #ifndef NBENCHMARK
//     high_resolution_clock::time_point t2 = high_resolution_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

//     LOG(NOTICE) << "get_values() took " << duration << "ms";
// #endif

//     return result;
// }

TBenchmark::TBenchmark(const string& message, bool enabled) : Message(message), Enabled(enabled)
{
    Start = high_resolution_clock::now();
}

TBenchmark::~TBenchmark()
{
    if (Enabled) {
        high_resolution_clock::time_point stop = high_resolution_clock::now();
        LOG(Info) << Message << duration_cast<milliseconds>(Start - stop).count() << "ms";
    }
}

void TBenchmark::Enable()
{
    Enabled = true;
}
