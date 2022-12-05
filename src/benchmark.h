#pragma once

#include <wblib/log.h>

//! RAII-style spend time benchmark. Calculates time period between construction a nd destruction of
//! an object, prints the time into log.
class TBenchmark
{
    std::string                                    Message;
    std::chrono::high_resolution_clock::time_point Start;
    bool                                           Enabled;
    WBMQTT::TLogger&                               Logger;

public:
    /**
     * @brief Construct a new TBenchmark object
     *
     * @param message prefix for log message with spend time
     * @param enabled true - print and calculate spend time in destructor, false - do nothig, wait
     * Enable call
     */
    TBenchmark(WBMQTT::TLogger& logger, const std::string& message, bool enabled = true);
    ~TBenchmark();

    /**
     * @brief Enables spend time calculation
     */
    void Enable();
};
