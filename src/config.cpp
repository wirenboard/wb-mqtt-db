#include "config.h"
#include "log.h"

#include <wblib/json_utils.h>

using namespace WBMQTT;

TMQTTDBLoggerConfig LoadConfig(const std::string& fileName, const std::string& schemaFileName)
{
    Json::Value root(JSON::Parse(fileName));
    JSON::Validate(root, JSON::Parse(schemaFileName));

    TMQTTDBLoggerConfig config;

    JSON::Get(root, "database", config.DBFile);
    JSON::Get(root, "debug", config.Debug);
    JSON::Get(root, "request_timeout", config.GetValuesRpcRequestTimeout);

    for (const auto& groupItem: root["groups"]) {
        TLoggingGroup group;

        JSON::Get(groupItem, "name", group.Name);
        JSON::Get(groupItem, "values", group.MaxChannelRecords);
        JSON::Get(groupItem, "values_total", group.MaxRecords);
        JSON::Get(groupItem, "min_interval", group.ChangedInterval);
        JSON::Get(groupItem, "min_unchanged_interval", group.UnchangedInterval);
        JSON::Get(groupItem, "max_burst", group.MaxBurstRecords);

        for (const auto& channelItem: groupItem["channels"]) {
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
