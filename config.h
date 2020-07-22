#pragma once

#include <chrono>
#include <set>
#include <string>
#include <vector>

const int DEFAULT_TIMEOUT_SEC = 9;
const int WB_DB_VERSION       = 3;

//! ring buffer will be cleared on limit * (1 + RingBufferClearThreshold) entries
const float RingBufferClearThreshold = 0.02;

//! loop will be interrupted at least once in this interval (in ms) for DB update event
const int WB_DB_LOOP_TIMEOUT_MS = 10;

struct TLoggingChannel
{
    std::string Pattern;
};

struct TLoggingGroup
{
    std::vector<TLoggingChannel> Channels;
    int                          Values               = 0;
    int                          ValuesTotal          = 0;
    int                          MinInterval          = 0;
    int                          MinUnchangedInterval = 0;
    std::string                  Id;
    int                          IntId;

    // internal fields - for timer processing
    std::set<int> ChannelIds;

    std::chrono::steady_clock::time_point LastSaved;
    std::chrono::steady_clock::time_point LastUSaved;
};

struct TMQTTDBLoggerConfig
{
    std::vector<TLoggingGroup> Groups;
    std::string                DBFile;

    bool                                Debug          = false;
    std::chrono::steady_clock::duration RequestTimeout = std::chrono::seconds(DEFAULT_TIMEOUT_SEC);
};

/**
 * @brief Load config from JSON file
 *
 * @param fileName full path and file name of config
 * @param shemaFileName full path and file name of config's JSON Schema
 */
TMQTTDBLoggerConfig LoadConfig(const std::string& fileName, const std::string& shemaFileName);
