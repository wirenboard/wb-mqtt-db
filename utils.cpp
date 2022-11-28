#include <fstream>
#include <chrono>

#include "utils.h"

namespace Utils
{
    const int UID_COLUMN = 0;
    const int VALUE_COLUMN = 2;
    const int TIMESTAMP_COLUMN = 3;
    const int MIN_COLUMN = 4;
    const int MAX_COLUMN = 5;
    const int RETAINED_COLUMN = 6;
    const int AVERAGE_VALUE_COLUMN = 7;

    void CopyFile(const std::string& from, const std::string& to)
    {
        std::ifstream src(from, std::ios::binary);
        std::ofstream dst(to, std::ios::binary);

        dst << src.rdbuf();
    }

    bool isNumber(const std::string& str)
    {
        const char* s = str.c_str();
        char* end     = nullptr;
        strtod(s, &end);
        return (end != s && end == s + str.length());
    }

    bool CallVisitor(IRecordsVisitor& visitor, SQLite::Statement& query, bool withAverage, const TChannelInfo& channel)
    {
        int  recordId = query.getColumn(UID_COLUMN).getInt();
        bool retain   = (query.getColumn(RETAINED_COLUMN).getInt() > 0);
        std::chrono::system_clock::time_point timestamp(std::chrono::milliseconds(query.getColumn(TIMESTAMP_COLUMN).getInt64()));

        if (withAverage && !isNumber(query.getColumn(VALUE_COLUMN).getString())) {
            return visitor.ProcessRecord(recordId,
                                         channel,
                                         query.getColumn(VALUE_COLUMN).getString(),
                                         timestamp,
                                         retain);
        }
        if (query.getColumn(MAX_COLUMN).isNull()) {
            return visitor.ProcessRecord(recordId,
                                         channel,
                                         query.getColumn(AVERAGE_VALUE_COLUMN).getString(),
                                         timestamp,
                                         retain);
        }
        return visitor.ProcessRecord(recordId,
                                     channel,
                                     query.getColumn(withAverage ? AVERAGE_VALUE_COLUMN : VALUE_COLUMN).getDouble(),
                                     timestamp,
                                     query.getColumn(MIN_COLUMN).getDouble(),
                                     query.getColumn(MAX_COLUMN).getDouble(),
                                     retain);
    }

    void AddCommonWhereClause(std::string& queryStr, size_t channelsCount)
    {
        queryStr += "channel IN (";
        if (channelsCount > 0) {
            for (size_t i = 0; i < channelsCount - 1; ++i) {
                queryStr += "?,";
            }
            queryStr += "?";
        }
        queryStr += ") AND timestamp > ? AND timestamp < ?";
    }

    void AddWithAverageQuery(std::string& queryStr, size_t channelsCount)
    {
        queryStr += "SELECT MAX(uid), channel, value, MAX(timestamp), MIN(min), MAX(max), retained, AVG(value) "
                    "FROM data INDEXED BY data_topic_timestamp WHERE ";
        AddCommonWhereClause(queryStr, channelsCount);
        queryStr += " AND uid > ? GROUP BY (round(timestamp/?)), channel";
    }

    void AddWithoutAverageQuery(std::string& queryStr, size_t channelsCount)
    {
        queryStr += "SELECT uid, channel, value, timestamp, min, max, retained, value "
                    "FROM data INDEXED BY data_topic_timestamp WHERE ";
        AddCommonWhereClause(queryStr, channelsCount);
        queryStr += " AND uid > ?";
    }
}
