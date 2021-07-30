#include "benchmark.h"

using namespace std::chrono;

TBenchmark::TBenchmark(WBMQTT::TLogger& logger, const std::string& message, bool enabled) 
    : Message(message), 
      Enabled(enabled),
      Logger(logger)
{
    Start = high_resolution_clock::now();
}

TBenchmark::~TBenchmark()
{
    if (Enabled) {
        auto stop = high_resolution_clock::now();
        Logger.Log() << Message << " " << duration_cast<milliseconds>(stop - Start).count() << " ms";
    }
}

void TBenchmark::Enable()
{
    Enabled = true;
}
