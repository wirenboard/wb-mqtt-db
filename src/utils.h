#pragma once

#include <string>
#include <sstream>

#include "SQLiteCpp/SQLiteCpp.h"
#include "storage.h"

namespace Utils
{
    void CopyFile(const std::string& from, const std::string& to);

    template<typename Iterator, typename Predicate>
    std::string Join(Iterator begin, Iterator end, Predicate pred, const std::string& delim)
    {
        std::stringstream res;
        if (begin != end) {
            res << pred(*begin);
        }
        ++begin;
        for (; begin != end; ++begin) {
            res << delim << pred(*begin);
        }
        return res.str();
    }

    bool isNumber(const std::string& str);
    bool CallVisitor(IRecordsVisitor& visitor, SQLite::Statement& query, bool withAverage, const TChannelInfo& channel);
    void AddCommonWhereClause(std::string& queryStr, size_t channelsCount);
    void AddWithAverageQuery(std::string& queryStr, size_t channelsCount);
    void AddWithoutAverageQuery(std::string& queryStr, size_t channelsCount);
}
