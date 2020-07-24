#pragma once

#include <chrono>
#include <set>
#include <string>
#include <vector>

#include "dblogger.h"

const int DEFAULT_TIMEOUT_SEC = 9;
struct TMQTTDBLoggerConfig
{
    TLoggerCache Cache;
    std::string  DBFile;

    bool                 Debug          = false;
    std::chrono::seconds RequestTimeout = std::chrono::seconds(DEFAULT_TIMEOUT_SEC);
};

/**
 * @brief Load config from JSON file
 *
 * @param fileName full path and file name of config
 * @param shemaFileName full path and file name of config's JSON Schema
 */
TMQTTDBLoggerConfig LoadConfig(const std::string& fileName, const std::string& shemaFileName);
