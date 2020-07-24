#include "config.h"

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

TMQTTDBLoggerConfig LoadConfig(const std::string& fileName, const std::string& shemaFileName)
{
    Json::Value root(JSON::Parse(fileName));
    JSON::Validate(root, JSON::Parse(shemaFileName));

    TMQTTDBLoggerConfig config;

    JSON::Get(root, "database", config.DBFile);
    JSON::Get(root, "debug", config.Debug);
    JSON::Get(root, "request_timeout", config.RequestTimeout);

    for (const auto& groupItem : root["groups"]) {
        TLoggingGroup group;

        JSON::Get(groupItem, "name", group.Name);
        JSON::Get(groupItem, "values", group.MaxChannelRecords);
        JSON::Get(groupItem, "values_total", group.MaxRecords);
        JSON::Get(groupItem, "min_interval", group.ChangedInterval);
        JSON::Get(groupItem, "min_unchanged_interval", group.UnchangedInterval);

        for (const auto& channelItem : groupItem["channels"]) {
            // convert channel format from d/c to /devices/d/controls/c
            auto name_split = StringSplit(channelItem.asString(), '/');
            group.MqttTopicPatterns.emplace_back("/devices/" + name_split[0] + "/controls/" +
                                                 name_split[1]);
        }

        config.Cache.Groups.push_back(group);
    }

    return config;
}
