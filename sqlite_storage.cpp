#include "sqlite_storage.h"
#include "SQLiteCpp/SQLiteCpp.h"

#include <fstream>
#include <sys/stat.h>
#include <wblib/utils.h>

#include "db_migrations.h"
#include "log.h"

using namespace std;
using namespace WBMQTT;
using namespace std::chrono;

#define LOG(logger) ::logger.Log() << "[sqlite] "

namespace
{
    const char* DB_BACKUP_FILE_EXTENSION = ".backup";
    const int   WB_DB_VERSION            = 6;

    const int UNDEFINED_ID = -1;

    const int UID_COLUMN = 0;
    const int CHANNEL_COLUMN = 1;
    const int VALUE_COLUMN = 2;
    const int TIMESTAMP_COLUMN = 3;
    const int MIN_COLUMN = 4;
    const int MAX_COLUMN = 5;
    const int RETAINED_COLUMN = 6;
    const int AVERAGE_VALUE_COLUMN = 7;

    string BackupFileName(const string& filename)
    {
        return filename + DB_BACKUP_FILE_EXTENSION;
    }

    void CopyFile(const string& from, const string& to)
    {
        ifstream src(from, ios::binary);
        ofstream dst(to, ios::binary);

        dst << src.rdbuf();
    }

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
        system_clock::time_point timestamp(milliseconds(query.getColumn(TIMESTAMP_COLUMN).getInt64()));

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

    void AddCommonWhereClause(string& queryStr, const std::vector<TChannelName>& channels)
    {
        if (!channels.empty()) {
            queryStr += "channel IN ( ";
            queryStr += Join(channels.begin(), channels.end(), [] (const auto&) {return '?';}, ",");
            queryStr += ") AND ";
        }
        queryStr += "timestamp > ? AND timestamp < ?";
    }

    void AddWithAverageQuery(string& queryStr, const std::vector<TChannelName>& channels)
    {
        queryStr += "SELECT uid, channel, value, timestamp, MIN(min), MAX(max), retained, AVG(value) \
                     FROM data INDEXED BY data_topic_timestamp WHERE ";
        AddCommonWhereClause(queryStr, channels);
        queryStr += " AND uid > ? GROUP BY (timestamp * ? / 86400000), channel";
    }

    void AddWithoutAverageQuery(string& queryStr, const std::vector<TChannelName>& channels)
    {
        queryStr += "SELECT uid, channel, value, timestamp, min, max, retained, value \
                     FROM data INDEXED BY data_topic_timestamp WHERE ";
        AddCommonWhereClause(queryStr, channels);
        queryStr += " AND uid > ?";
    }

} // namespace

TSqliteStorage::TSqliteStorage(const string& dbFile)
{
    bool isMemoryDb = (dbFile.find(":memory:") != string::npos);

    // check if backup file is present; if so, we should try to repair DB
    if (!isMemoryDb && CheckBackupFile(dbFile)) {
        LOG(Warn) << "Something went wrong last time, restoring old backup file";
        RestoreBackupFile(dbFile);
    }

    int flags = SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE;
    if (isMemoryDb) {
        flags |= SQLite::OPEN_URI;
    }

    DB.reset(new SQLite::Database(dbFile, flags));

    if (!DB->tableExists("data")) {
        // new DB file created
        LOG(Info) << "Creating tables";
        CreateTables(WB_DB_VERSION);
    } else {
        int file_db_version = ReadDBVersion();
        if (file_db_version > WB_DB_VERSION) {
            wb_throw(TBaseException, "Database file is created by newer version of wb-mqtt-db");
        }
        if (file_db_version < WB_DB_VERSION) {
            LOG(Warn) << "Old database format found, trying to update...";
            CreateBackupFile(dbFile);
            UpdateDB(file_db_version);
        } else {
            LOG(Info) << "Creating tables if necessary";
            CreateTables(WB_DB_VERSION);
        }
    }

    LOG(Info) << "Create indices if necessary";
    CreateIndices();

    LOG(Info) << "Analyzing data table";
    DB->exec("ANALYZE data");
    DB->exec("ANALYZE sqlite_master");

    InsertRowQuery.reset(new SQLite::Statement(
        *DB,
        "INSERT INTO data (channel, value, min, max, retained, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?)"));

    CleanChannelQuery.reset(
        new SQLite::Statement(*DB, "DELETE FROM data WHERE channel = ? ORDER BY timestamp ASC LIMIT ?"));

    LOG(Info) << "DB initialization is done";

    if (!isMemoryDb && CheckBackupFile(dbFile)) {
        RemoveBackupFile(dbFile);
    }

    Load();
}

void TSqliteStorage::CreateTables(int dbVersion)
{
    LOG(Debug) << "Creating 'channels' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS channels ( "
             "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "device VARCHAR(255), "
             "control VARCHAR(255), "
             "precision REAL, "
             "UNIQUE(device,control) "
             ")  ");

    LOG(Debug) << "Creating 'data' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS data ("
             "uid INTEGER PRIMARY KEY AUTOINCREMENT, "
             "channel INTEGER,"
             "value VARCHAR(255),"
             "timestamp INTEGER DEFAULT(0),"
             "max VARCHAR(255),"
             "min VARCHAR(255),"
             "retained INTEGER"
             ")");

    LOG(Debug) << "Creating 'variables' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS variables ("
             "name VARCHAR(255) PRIMARY KEY, "
             "value VARCHAR(255) )");

    {
        LOG(Debug) << "Updating database version variable...";
        SQLite::Statement query(
            *DB,
            "INSERT OR REPLACE INTO variables (name, value) VALUES ('db_version', ?)");
        query.bind(1, dbVersion);
        query.exec();
    }
}

void TSqliteStorage::CreateIndices()
{
    LOG(Debug) << "Creating 'data_topic' index on 'data' ('channel')";
    DB->exec("CREATE INDEX IF NOT EXISTS data_topic ON data (channel)");

    // NOTE: the following index is a "low quality" one according to sqlite documentation. However,
    // reversing the order of columns results in factor of two decrease in SELECT performance. So we
    // leave it here as it is.
    LOG(Debug) << "Creating 'data_topic_timestamp' index on 'data' ('channel', 'timestamp')";
    DB->exec("CREATE INDEX IF NOT EXISTS data_topic_timestamp ON data (channel, timestamp)");
}

void TSqliteStorage::Load()
{
    std::lock_guard<std::mutex> lg(Mutex);
    SQLite::Statement query(*DB, "SELECT int_id, device, control, precision FROM channels");
    SQLite::Statement rowCountQuery(*DB, "SELECT COUNT(uid), MAX(timestamp)/1000 FROM data WHERE channel=?");

    while (query.executeStep()) {
        rowCountQuery.reset();
        rowCountQuery.bind(1, query.getColumn(0).getInt64());
        rowCountQuery.executeStep();
        auto channel = CreateChannelPrivate(query.getColumn(0).getInt64(), query.getColumn(1), query.getColumn(2));
        SetRecordCount(*channel, rowCountQuery.getColumn(0));
        if (!rowCountQuery.getColumn(1).isNull()) {
            SetLastRecordTime(*channel, std::chrono::system_clock::from_time_t(rowCountQuery.getColumn(1).getInt64()));
        }
        if (!query.getColumn(3).isNull()) {
            SetPrecision(*channel, query.getColumn(3).getDouble());
        }
    }
}

int TSqliteStorage::ReadDBVersion()
{
    if (!DB->tableExists("variables")) {
        return 0;
    }

    SQLite::Statement query(*DB, "SELECT value FROM variables WHERE name = 'db_version'");
    while (query.executeStep()) {
        return query.getColumn(0).getInt();
    }

    return 0;
}

void TSqliteStorage::UpdateDB(int prev_version)
{
    auto migrations = GetMigrations();
    if (WB_DB_VERSION > migrations.size()) {
        wb_throw(TBaseException, "No migration to new DB version");
    }

    if (prev_version > WB_DB_VERSION) {
        wb_throw(TBaseException, "Unsupported DB version. Please consider deleting DB file.");
    }

    SQLite::Transaction transaction(*DB);
    for (; static_cast<unsigned int>(prev_version) < migrations.size(); ++prev_version) {
        LOG(Info) << "Convert database from version " << prev_version;
        migrations[prev_version](*DB);
    }
    transaction.commit();
    DB->exec("VACUUM");
}

/**
 * Check if DB backup file exists
 */
bool TSqliteStorage::CheckBackupFile(const string& dbFile)
{
    string      backup_file = dbFile + DB_BACKUP_FILE_EXTENSION;
    struct stat buffer;

    if (stat(backup_file.c_str(), &buffer) < 0) {
        return false;
    }

    return S_ISREG(buffer.st_mode);
}

/**
 * Create DB backup file from existing
 */
void TSqliteStorage::CreateBackupFile(const string& dbFile)
{
    LOG(Info) << "Creating backup file for DB";
    CopyFile(dbFile, BackupFileName(dbFile));
}

/**
 * Restore backup file
 */
void TSqliteStorage::RestoreBackupFile(const string& dbFile)
{
    LOG(Info) << "Restoring detected backup file for DB";
    CopyFile(BackupFileName(dbFile), dbFile);
}

/**
 * Remove backup file
 */
void TSqliteStorage::RemoveBackupFile(const string& dbFile)
{
    LOG(Info) << "Removing backup file";
    std::remove(BackupFileName(dbFile).c_str());
}

void TSqliteStorage::WriteChannel(TChannelInfo&                         channelInfo,
                                  const std::string&                    value,
                                  const std::string&                    minimum,
                                  const std::string&                    maximum,
                                  bool                                  retained,
                                  std::chrono::system_clock::time_point time)
{
    std::lock_guard<std::mutex> lg(Mutex);
    if (!Transaction) {
        Transaction.reset(new SQLite::Transaction(*DB));
    }

    LOG(Debug) << "Resulting channel ID for this request is " << channelInfo.GetId();

    InsertRowQuery->clearBindings();
    InsertRowQuery->bind(1, channelInfo.GetId());
    InsertRowQuery->bind(2, value);
    if (!minimum.empty()) {
        InsertRowQuery->bind(3, minimum);
    }
    if (!maximum.empty()) {
        InsertRowQuery->bind(4, maximum);
    }
    InsertRowQuery->bind(5, retained ? 1 : 0);
    InsertRowQuery->bind(6, std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count());
    InsertRowQuery->exec();
    InsertRowQuery->reset();

    SetRecordCount(channelInfo, channelInfo.GetRecordCount() + 1);
    SetLastRecordTime(channelInfo, time);
}

void TSqliteStorage::Commit()
{
    std::lock_guard<std::mutex> lg(Mutex);
    if (Transaction) {
        Transaction->commit();
        Transaction.reset();
    }
}

PChannelInfo TSqliteStorage::CreateChannel(const TChannelName& channelName)
{
    LOG(Info) << "Creating channel " << channelName.Device << "/" << channelName.Control;

    SQLite::Statement query(*DB, "INSERT INTO channels (device, control) VALUES (?, ?) ");
    query.bindNoCopy(1, channelName.Device);
    query.bindNoCopy(2, channelName.Control);
    query.exec();

    return CreateChannelPrivate(DB->getLastInsertRowid(), channelName.Device, channelName.Control);
}

/**
 * @brief Set channel's precision. One must call Commit to finalaze writing to storage.
 */
void TSqliteStorage::SetChannelPrecision(TChannelInfo& channelInfo, double precision)
{
    if (precision == channelInfo.GetPrecision()) {
        return;
    }

    LOG(Debug) << "Set channel's " << channelInfo.GetName() << " precision to " << precision;

    SQLite::Statement query(*DB, "UPDATE channels SET precision = ? WHERE int_id = ?");
    query.bind(1, precision);
    query.bind(2, channelInfo.GetId());
    query.exec();
    SetPrecision(channelInfo, precision);
}

void TSqliteStorage::GetRecords(IRecordsVisitor&                      visitor,
                                const std::vector<TChannelName>&      channels,
                                std::chrono::system_clock::time_point startTime,
                                std::chrono::system_clock::time_point endTime,
                                int64_t                               startId,
                                uint32_t                              maxRecords,
                                std::chrono::milliseconds             minInterval)
{

    if (minInterval.count() > 0) {
        GetRecordsWithAverage(visitor, channels, startTime, endTime, startId, maxRecords, minInterval);
    } else {
        GetRecordsWithoutAverage(visitor, channels, startTime, endTime, startId, maxRecords);
    }
}

int TSqliteStorage::BindParams(SQLite::Statement&                    query,
                               int                                   param_num,
                               const std::vector<TChannelName>&      channels,
                               std::chrono::system_clock::time_point startTime,
                               std::chrono::system_clock::time_point endTime,
                               int64_t                               startId)
{
    for (const auto& channel: channels) {
        int  channelId = -1;
        auto pChannel  = FindChannel(channel);
        if (pChannel) {
            channelId = pChannel->GetId();
        }
        query.bind(++param_num, channelId);
    }

    query.bind(++param_num, duration_cast<milliseconds>(startTime.time_since_epoch()).count());
    query.bind(++param_num, duration_cast<milliseconds>(endTime.time_since_epoch()).count());
    query.bind(++param_num, startId);
    return param_num;
}

void TSqliteStorage::GetRecordsWithoutAverage(IRecordsVisitor&                      visitor,
                                              const std::vector<TChannelName>&      channels,
                                              std::chrono::system_clock::time_point startTime,
                                              std::chrono::system_clock::time_point endTime,
                                              int64_t                               startId,
                                              uint32_t                              maxRecords)
{
    string queryStr;
    AddWithoutAverageQuery(queryStr, channels);
    queryStr += " ORDER BY uid ASC LIMIT ?";

    std::lock_guard<std::mutex> lg(Mutex);

    std::unordered_map<int, PChannelInfo> channelIdToNameMap;
    for(const auto& ch: GetChannelsPrivate()) {
        channelIdToNameMap.insert({ch.second->GetId(), ch.second});
    }

    SQLite::Statement query(*DB, queryStr);
    int param_num = BindParams(query, 0, channels, startTime, endTime, startId);
    query.bind(++param_num, maxRecords);

    while (query.executeStep()) {
        int channelId(query.getColumn(CHANNEL_COLUMN).getInt());
        if (!CallVisitor(visitor, query, false, *channelIdToNameMap[channelId])) {
            return;
        }
    }
}

void TSqliteStorage::GetRecordsWithAverage(IRecordsVisitor&                      visitor,
                                           const std::vector<TChannelName>&      channels,
                                           std::chrono::system_clock::time_point startTime,
                                           std::chrono::system_clock::time_point endTime,
                                           int64_t                               startId,
                                           uint32_t                              maxRecords,
                                           std::chrono::milliseconds             minInterval)
{

    auto recordsCount = GetRecordsCount(channels, startTime, endTime);
    std::vector<TChannelName> withAverage;
    std::vector<TChannelName> withoutAverage;

    int maxNotAveragedRecords = (((endTime - startTime) / minInterval) * 1.1);
    for(const auto& ch: GetRecordsCount(channels, startTime, endTime)) {
        if (ch.second > maxNotAveragedRecords) {
            withAverage.emplace_back(ch.first);
        } else {
            withoutAverage.emplace_back(ch.first);
        }
    }

    string queryStr;

    if (!withoutAverage.empty()) {
        AddWithoutAverageQuery(queryStr, withoutAverage);
    }

    if (!withAverage.empty()) {
        if (!queryStr.empty()) {
            queryStr += " UNION ALL ";
        }
        AddWithAverageQuery(queryStr, withAverage);
    }

    queryStr += " ORDER BY uid ASC LIMIT ?";

    std::lock_guard<std::mutex> lg(Mutex);
    std::unordered_map<int, PChannelInfo> channelIdToNameMap;
    for(const auto& ch: GetChannelsPrivate()) {
        channelIdToNameMap.insert({ch.second->GetId(), ch.second});
    }

    SQLite::Statement query(*DB, queryStr);

    int param_num = 0;
    if (!withoutAverage.empty()) {
        param_num = BindParams(query, param_num, withoutAverage, startTime, endTime, startId);
    }
    if (!withAverage.empty()) {
        param_num = BindParams(query, param_num, withAverage, startTime, endTime, startId);
        int dayFraction = 86400000 / minInterval.count(); // ms in day
        LOG(Debug) << "day: fraction :" << dayFraction;
        query.bind(++param_num, dayFraction);
    }

    query.bind(++param_num, maxRecords);

    while (query.executeStep()) {
        int channelId(query.getColumn(CHANNEL_COLUMN).getInt());
        if (!CallVisitor(visitor, query, true, *channelIdToNameMap[channelId])) {
            return;
        }
    }
}

void TSqliteStorage::GetChannels(IChannelVisitor& visitor)
{
    std::lock_guard<std::mutex> lg(Mutex);
    for (const auto& channel : GetChannelsPrivate()) {
        visitor.ProcessChannel(channel.second);
    }
}

void TSqliteStorage::DeleteRecords(TChannelInfo& channel, uint32_t count)
{
    std::lock_guard<std::mutex> lg(Mutex);
    CleanChannelQuery->bind(1, channel.GetId());
    CleanChannelQuery->bind(2, count);
    auto deletedRows = CleanChannelQuery->exec();
    CleanChannelQuery->reset();
    SetRecordCount(channel, channel.GetRecordCount() - deletedRows);
    LOG(Debug) << "Clear channel id = " << channel.GetId();
}

void TSqliteStorage::DeleteRecords(const std::vector<std::reference_wrapper<TChannelInfo>>& channels, uint32_t count)
{
    auto ids = Join(channels.cbegin(), channels.cend(), [](const TChannelInfo& ch) { return ch.GetId();}, ",");
    std::unordered_map<uint64_t, int> deletedRows;
    {
        std::stringstream queryText;
        queryText << "SELECT count(), channel FROM "
                  << "(SELECT channel FROM data WHERE channel in (" << ids << ") ORDER BY timestamp ASC LIMIT " << count << ") "
                  << "GROUP BY channel";
        SQLite::Statement query(*DB, queryText.str());
        while(query.executeStep()) {
            deletedRows[query.getColumn(1).getInt64()] = query.getColumn(0).getInt();
        }
    }
    {
        std::stringstream queryText;
        queryText << "DELETE FROM data WHERE channel in (" << ids << ") ORDER BY timestamp ASC LIMIT " << count;
        SQLite::Statement query(*DB, queryText.str());
        query.exec();
    }

    for (TChannelInfo& channel: channels) {
        auto it = deletedRows.find(channel.GetId());
        if (it != deletedRows.end()) {
            SetRecordCount(channel, channel.GetRecordCount() - it->second);
        }
    }
}

int TSqliteStorage::GetDBVersion()
{
    return WB_DB_VERSION;
}

std::unordered_map<TChannelName, int> TSqliteStorage::GetRecordsCount(std::vector<TChannelName>             channels,
                                                                      std::chrono::system_clock::time_point startTime,
                                                                      std::chrono::system_clock::time_point endTime)
{
    string queryStr;

    queryStr = "SELECT COUNT(*), channel FROM data INDEXED BY data_topic_timestamp WHERE ";
    AddCommonWhereClause(queryStr, channels);
    queryStr += " GROUP BY channel";

    std::lock_guard<std::mutex> lg(Mutex);
    SQLite::Statement query(*DB, queryStr);

    std::unordered_map<TChannelName, int> res;
    int param_num = 0;
    for (const auto& channel: channels) {
        res[channel] = 0;
        int  channelId = -1;
        auto pChannel  = FindChannel(channel);
        if (pChannel) {
            channelId = pChannel->GetId();
        }
        query.bind(++param_num, channelId);
    }

    std::unordered_map<int, PChannelInfo> channelIdToNameMap;
    for(const auto& ch: GetChannelsPrivate()) {
        channelIdToNameMap.insert({ch.second->GetId(), ch.second});
    }

    query.bind(++param_num, duration_cast<milliseconds>(startTime.time_since_epoch()).count());
    query.bind(++param_num, duration_cast<milliseconds>(endTime.time_since_epoch()).count());

    while (query.executeStep()) {
        int channelId(query.getColumn(CHANNEL_COLUMN).getInt());
        res[channelIdToNameMap[channelId]->GetName()] = query.getColumn(0).getInt();
    }
    return res;
}
