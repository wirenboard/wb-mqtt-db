#include "config.h"

#include <wblib/json_utils.h>

using namespace WBMQTT;
using namespace std::chrono;

template <> inline bool JSON::Is<steady_clock::duration>(const Json::Value& value)
{
    return value.isUInt();
}

template <> inline steady_clock::duration JSON::As<steady_clock::duration>(const Json::Value& value)
{
    return seconds(value.asUInt());
}

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

        JSON::Get(groupItem, "name", group.Id);
        JSON::Get(groupItem, "values", group.Values);
        JSON::Get(groupItem, "values_total", group.ValuesTotal);
        JSON::Get(groupItem, "min_interval", group.MinInterval);
        JSON::Get(groupItem, "min_unchanged_interval", group.MinUnchangedInterval);

        for (const auto& channelItem : groupItem["channels"]) {
            // convert channel format from d/c to /devices/d/controls/c
            auto name_split = StringSplit(channelItem.asString(), '/');
            TLoggingChannel channel = { "/devices/" + name_split[0] + "/controls/" + name_split[1] };
            group.Channels.push_back(channel);
        }

        config.Groups.push_back(group);
    }

    return config;
}
