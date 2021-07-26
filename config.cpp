#include "config.h"
#include "log.h"

#include <wblib/json_utils.h>

using namespace WBMQTT;
using namespace std::chrono;

namespace WBMQTT
{
    namespace JSON
    {
        template <> inline bool Is<seconds>(const Json::Value& value)
        {
            return value.isUInt();
        }

        template <> inline seconds As<seconds>(const Json::Value& value)
        {
            return seconds(value.asUInt());
        }
    } // namespace JSON
} // namespace WBMQTT

TMQTTDBLoggerConfig LoadConfig(const std::string& fileName, const std::string& schemaFileName)
{
    Json::Value root(JSON::Parse(fileName));
    JSON::Validate(root, JSON::Parse(schemaFileName));

    TMQTTDBLoggerConfig config;

    JSON::Get(root, "database", config.DBFile);
    JSON::Get(root, "debug", config.Debug);
    JSON::Get(root, "request_timeout", config.GetValuesRpcRequestTimeout);

    for (const auto& groupItem : root["groups"]) {
        TLoggingGroup group;

        JSON::Get(groupItem, "name", group.Name);
        JSON::Get(groupItem, "values", group.MaxChannelRecords);
        JSON::Get(groupItem, "values_total", group.MaxRecords);
        JSON::Get(groupItem, "min_interval", group.ChangedInterval);
        JSON::Get(groupItem, "min_unchanged_interval", group.UnchangedInterval);

        for (const auto& channelItem : groupItem["channels"]) {
            auto name_split = StringSplit(channelItem.asString(), '/');
            if (name_split.size() == 2) {
                group.ControlPatterns.emplace_back(name_split[0], name_split[1]);
            } else {
                ::Warn.Log() << "[config] Bad channel: " << channelItem.asString();
            }
        }

        config.Cache.Groups.push_back(group);
    }

    return config;
}
