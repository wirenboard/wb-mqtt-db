#include "sqlite_storage.h"
#include "SQLiteCpp/SQLiteCpp.h"

#include <fstream>
#include <sys/stat.h>
#include <wblib/utils.h>

#include "log.h"

using namespace std;
using namespace WBMQTT;
using namespace std::chrono;

#define LOG(logger) ::logger.Log() << "[sqlite] "

namespace
{
    const char* DB_BACKUP_FILE_EXTENSION = ".backup";
    const int   WB_DB_VERSION            = 4;

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

    void convertDBFrom3To4(SQLite::Database& DB)
    {
        // save old channels table
        DB.exec("ALTER TABLE channels RENAME TO channels_old");

        // save new channels table
        DB.exec("CREATE TABLE channels ( "
                 "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                 "device VARCHAR(255), "
                 "control VARCHAR(255), "
                 "UNIQUE(device,control) "
                 ")");

        // save unique channels to new channels table and update keys in data table
        {
            SQLite::Statement query(
                DB,
                "SELECT int_id, device, control FROM channels_old ORDER BY device, control");
            int          prevId = -1;
            TChannelName prevName;
            while (query.executeStep()) {
                TChannelName curName(query.getColumn(1), query.getColumn(2));
                if (curName == prevName) {
                    SQLite::Statement updateQuery(DB,
                                                  "UPDATE data SET channel=? WHERE channel=?");
                    updateQuery.bind(1, prevId);
                    updateQuery.bind(2, query.getColumn(0).getInt());
                    updateQuery.exec();
                } else {
                    prevName = curName;
                    prevId   = query.getColumn(0).getInt();
                    SQLite::Statement insertQuery(DB,
                                                  "INSERT INTO channels(int_id, device, control) VALUES(?, ?, ?)");
                    insertQuery.bind(1, prevId);
                    insertQuery.bind(2, curName.Device);
                    insertQuery.bind(3, curName.Control);
                    insertQuery.exec();
                }
            }
        }

        // drop old channels table
        DB.exec("DROP TABLE channels_old");

        // save old data table
        DB.exec("ALTER TABLE data RENAME TO data_old");

        // create new data table
        DB.exec("CREATE TABLE data ("
                 "uid INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "channel INTEGER,"
                 "value VARCHAR(255),"
                 "timestamp INTEGER DEFAULT(0),"
                 "group_id INTEGER,"
                 "max VARCHAR(255),"
                 "min VARCHAR(255),"
                 "retained INTEGER"
                 ")");

        // copy all data without device field
        DB.exec("INSERT INTO data "
                 "SELECT uid, channel, value, timestamp, group_id, max, min, retained "
                 "FROM data_old");

        // drop old data table
        DB.exec("DROP TABLE data_old");

        // drop old devices table
        DB.exec("DROP TABLE devices");

        DB.exec("UPDATE variables SET value=\"4\" WHERE name=\"db_version\"");
    }
} // namespace

TSqliteStorage::TSqliteStorage(const string& dbFile)
{
    // check if backup file is present; if so, we should try to repair DB
    if (CheckBackupFile(dbFile)) {
        LOG(Warn) << "Something went wrong last time, restoring old backup file";
        RestoreBackupFile(dbFile);
    }

    DB.reset(new SQLite::Database(dbFile, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE));

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

    LOG(Info) << "DB initialization is done";

    if (CheckBackupFile(dbFile)) {
        RemoveBackupFile(dbFile);
    }
}

TSqliteStorage::~TSqliteStorage() {}

void TSqliteStorage::CreateTables(int dbVersion)
{
    LOG(Debug) << "Creating 'channels' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS channels ( "
             "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "device VARCHAR(255), "
             "control VARCHAR(255), "
             "UNIQUE(device,control) "
             ")  ");

    LOG(Debug) << "Creating 'groups' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS groups ( "
             "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "group_id VARCHAR(255) "
             ")  ");

    LOG(Debug) << "Creating 'data' table...";
    DB->exec("CREATE TABLE IF NOT EXISTS data ("
             "uid INTEGER PRIMARY KEY AUTOINCREMENT, "
             "channel INTEGER,"
             "value VARCHAR(255),"
             "timestamp INTEGER DEFAULT(0),"
             "group_id INTEGER,"
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

    LOG(Debug) << "Creating 'data_gid' index on 'data' ('group_id')";
    DB->exec("CREATE INDEX IF NOT EXISTS data_gid ON data (group_id)");

    LOG(Debug) << "Creating 'data_gid_timestamp' index on 'data' ('group_id', 'timestamp')";
    DB->exec("CREATE INDEX IF NOT EXISTS data_gid_timestamp ON data (group_id, timestamp)");
}

void TSqliteStorage::Load(TLoggerCache& cache)
{
    {
        SQLite::Statement query(*DB, "SELECT int_id, device, control FROM channels");
        while (query.executeStep()) {
            TChannelName name(query.getColumn(1), query.getColumn(2));
            StoredChannelIds[name] = query.getColumn(0);
        }
    }

    unordered_map<string, int> storedGroupIds;
    {
        SQLite::Statement query(*DB, "SELECT int_id, group_id FROM groups");
        while (query.executeStep()) {
            storedGroupIds[query.getColumn(1).getText()] = query.getColumn(0);
        }
    }

    for (auto& group : cache.Groups) {
        auto it = storedGroupIds.find(group.Name);
        if (it != storedGroupIds.end()) {
            group.StorageId = it->second;
        } else {
            SQLite::Statement query(*DB, "INSERT INTO groups (group_id) VALUES (?) ");
            query.bind(1, group.Name);
            query.exec();
            group.StorageId = DB->getLastInsertRowid();
        }
    }

    {
        LOG(Info) << "Fill channel's RowCount, LastChanged, LastValue...";
        string dataQuery =
            "SELECT c.device, c.control, c.int_id, d.group_id, d.timestamp, d.value, d.cn "
            "FROM channels c "
            "JOIN (SELECT d1.group_id, d1.channel, d1.timestamp, d1.value, d2.cn "
            "    FROM (SELECT COUNT(timestamp) cn, MAX(timestamp) t, group_id, channel "
            "            FROM data "
            "            GROUP BY channel, group_id) d2 "
            "    JOIN data d1 "
            "    ON d1.timestamp = d2.t AND d1.group_id = d2.group_id AND d1.channel = d2.channel) d "
            "ON c.int_id = d.channel "
            "ORDER BY d.group_id";

        SQLite::Statement query(*DB, dataQuery);

        TLoggingGroup* group = nullptr;

        while (query.executeStep()) {
            int groupId = query.getColumn(3).getInt();

            if (!group || group->StorageId != groupId) {
                for (auto& g : cache.Groups) {
                    if (g.StorageId == groupId) {
                        group = &g;
                        break;
                    }
                }
            }

            if (group) {
                TChannelName channelName(query.getColumn(0).getString(),
                                         query.getColumn(1).getString());
                LOG(Debug) << "Load " << channelName.Device << "/" << channelName.Control << " from "
                           << group->Name;
                TChannel& channel   = group->Channels[channelName];
                channel.StorageId   = query.getColumn(2).getInt();
                auto d              = milliseconds(query.getColumn(4).getInt64());
                channel.LastChanged = system_clock::time_point(d);
                channel.LastValue   = query.getColumn(5).getString();
                channel.RecordCount = query.getColumn(6).getInt();
                group->RecordCount += channel.RecordCount;
            }
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
    SQLite::Transaction transaction(*DB);

    switch (prev_version) {
    case 0:
        LOG(Info) << "Convert database from version 0";

        DB->exec("ALTER TABLE data RENAME TO tmp");

        // drop existing indexes
        DB->exec("DROP INDEX data_topic");
        DB->exec("DROP INDEX data_topic_timestamp");
        DB->exec("DROP INDEX data_gid");
        DB->exec("DROP INDEX data_gid_timestamp");

        // create tables with most recent schema
        CreateTables(WB_DB_VERSION);

        // generate internal integer ids from old data table
        DB->exec("INSERT OR IGNORE INTO devices (device) SELECT device FROM tmp GROUP BY device");
        DB->exec("INSERT OR IGNORE INTO channels (device, control) SELECT device, control FROM tmp "
                 "GROUP BY device, control");
        DB->exec(
            "INSERT OR IGNORE INTO groups (group_id) SELECT group_id FROM tmp GROUP BY group_id");

        // populate data table using values from old data table
        DB->exec(
            "INSERT INTO data(uid, device, channel,value,timestamp,group_id) "
            "SELECT uid, devices.int_id, channels.int_id, value, julianday(timestamp), groups.int_id "
            "FROM tmp "
            "LEFT JOIN devices ON tmp.device = devices.device "
            "LEFT JOIN channels ON tmp.device = channels.device AND tmp.control = channels.control "
            "LEFT JOIN groups ON tmp.group_id = groups.group_id ");

        DB->exec("DROP TABLE tmp");

        DB->exec("UPDATE variables SET value=\"1\" WHERE name=\"db_version\"");

    case 1:
        // In versions >= 2, there is a difference in 'data' table:
        // add data.max, data.min columns
        LOG(Info) << "Convert database from version 1";

        DB->exec("ALTER TABLE data ADD COLUMN max VARCHAR(255)");
        DB->exec("ALTER TABLE data ADD COLUMN min VARCHAR(255)");
        DB->exec("ALTER TABLE data ADD COLUMN retained INTEGER");

        DB->exec("UPDATE data SET max = value");
        DB->exec("UPDATE data SET min = value");
        DB->exec("UPDATE data SET retained = 0");

        DB->exec("UPDATE variables SET value=\"2\" WHERE name=\"db_version\"");

    case 2:
        LOG(Info) << "Convert database from version 2";

        // save old data table
        DB->exec("ALTER TABLE data RENAME TO data_old");

        // create new data table
        DB->exec("CREATE TABLE data ("
                 "uid INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "device INTEGER,"
                 "channel INTEGER,"
                 "value VARCHAR(255),"
                 "timestamp INTEGER DEFAULT(0),"
                 "group_id INTEGER,"
                 "max VARCHAR(255),"
                 "min VARCHAR(255),"
                 "retained INTEGER"
                 ")");

        // copy all data casting timestamps
        DB->exec("INSERT INTO data "
                 "SELECT uid, device, channel, value, "
                 "CAST((timestamp - 2440587.5) * 86400000 AS INTEGER), group_id, max, min, retained "
                 "FROM data_old");

        // drop old data table
        DB->exec("DROP TABLE data_old");

        DB->exec("UPDATE variables SET value=\"3\" WHERE name=\"db_version\"");

    case 3:
        LOG(Info) << "Convert database from version 3";
        convertDBFrom3To4(*DB);
        break;

    default:
        wb_throw(TBaseException, "Unsupported DB version. Please consider deleting DB file.");
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

void TSqliteStorage::WriteChannel(const TChannelName& channelName,
                                  TChannel&           channel,
                                  TLoggingGroup&      group)
{
    if (!Transaction) {
        Transaction.reset(new SQLite::Transaction(*DB));
    }

    GetOrCreateChannelId(channelName, channel);

    LOG(Debug) << "Resulting channel ID for this request is " << channel.StorageId;

    SQLite::Statement insert_row_query(*DB, "INSERT INTO data (channel, value, group_id, min, max, \
                                                    retained, timestamp) VALUES (?, ?, ?, ?, ?, ?, strftime(\"%s\", 'now') * 1000)");

    insert_row_query.bind(1, channel.StorageId);
    insert_row_query.bind(3, group.StorageId);

    // min, max and average
    if (channel.Accumulated && channel.Changed) {
        auto&  accum = channel.Accumulator;
        double val   = accum.ValueCount > 0 ? accum.Sum / accum.ValueCount : 0.0; // 0.0 - error value

        insert_row_query.bind(2, val); // avg

        insert_row_query.bind(4, accum.Min); // min
        insert_row_query.bind(5, accum.Max); // max

        accum.Reset();
    } else {
        insert_row_query.bind(2, channel.LastValue); // avg == value
        insert_row_query.bind(4);                    // bind NULL values
        insert_row_query.bind(5);                    // bind NULL values
    }

    insert_row_query.bind(6, channel.Retained ? 1 : 0);
    insert_row_query.exec();

    ++channel.RecordCount;

    // local cache is needed here since SELECT COUNT are extremely slow in sqlite
    // so we only ask DB at startup. This applies to two if blocks below.

    if (group.MaxChannelRecords > 0) {
        if (channel.RecordCount > group.MaxChannelRecords * (1 + RECORDS_CLEAR_THRESHOLDR)) {
            SQLite::Statement clean_channel_query(
                *DB,
                "DELETE FROM data WHERE channel = ? ORDER BY rowid ASC LIMIT ?");
            clean_channel_query.bind(1, channel.StorageId);
            clean_channel_query.bind(2, channel.RecordCount - group.MaxChannelRecords);

            clean_channel_query.exec();

            LOG(Warn) << "Channel data limit is reached: channel " << channelName << ", row count "
                      << channel.RecordCount << ", limit " << group.MaxChannelRecords;

            LOG(Debug) << clean_channel_query.getQuery();

            channel.RecordCount = group.MaxChannelRecords;
        }
    }

    ++group.RecordCount;
    if (group.MaxRecords > 0) {
        if (group.RecordCount > group.MaxRecords * (1 + RECORDS_CLEAR_THRESHOLDR)) {
            SQLite::Statement clean_group_query(
                *DB,
                "DELETE FROM data WHERE group_id = ? ORDER BY rowid ASC LIMIT ?");
            clean_group_query.bind(1, group.StorageId);
            clean_group_query.bind(2, group.RecordCount - group.MaxRecords);
            clean_group_query.exec();

            LOG(Warn) << "Group data limit is reached: group " << group.Name << ", row count "
                      << group.RecordCount << ", limit " << group.MaxRecords;

            LOG(Debug) << clean_group_query.getQuery();

            group.RecordCount = group.MaxRecords;
        }
    }
}

void TSqliteStorage::Commit()
{
    if (Transaction) {
        Transaction->commit();
        Transaction.reset();
    }
}

void TSqliteStorage::GetOrCreateChannelId(const TChannelName& channelName, TChannel& channel)
{
    if (channel.StorageId != TChannel::UNDEFIDED_ID)
        return;

    auto it = StoredChannelIds.find(channelName);
    if (it != StoredChannelIds.end()) {
        channel.StorageId = it->second;
        return;
    }

    LOG(Info) << "Creating channel " << channelName;

    SQLite::Statement query(*DB, "INSERT INTO channels (device, control) VALUES (?, ?) ");

    query.bind(1, channelName.Device);
    query.bind(2, channelName.Control);
    query.exec();

    channel.StorageId             = DB->getLastInsertRowid();
    StoredChannelIds[channelName] = channel.StorageId;
}

void TSqliteStorage::GetRecords(IRecordsVisitor&                      visitor,
                                const std::vector<TChannelName>&      channels,
                                std::chrono::system_clock::time_point startTime,
                                std::chrono::system_clock::time_point endTime,
                                int64_t                               startId,
                                uint32_t                              maxRecords,
                                std::chrono::milliseconds             minInterval)
{
    // version 3.7 can't always figure out to use the proper index
    string queryStr;

    if (minInterval.count() > 0)
        queryStr = "SELECT uid, device, channel, AVG(value), timestamp, MIN(min), MAX(max), \
                                retained  FROM data INDEXED BY data_topic_timestamp WHERE ";
    else
        queryStr = "SELECT uid, device, channel, value, timestamp, min, max, \
                                retained FROM data INDEXED BY data_topic_timestamp WHERE ";

    if (!channels.empty()) {
        queryStr += "channel IN ( ";
        for (size_t i = 0; i < channels.size(); ++i) {
            if (i > 0) {
                queryStr += ", ";
            }
            queryStr += "?";
        }
        queryStr += ") AND ";
    }

    queryStr += "timestamp > ? AND timestamp < ? AND uid > ? ";

    if (minInterval.count() > 0) {
        queryStr += " GROUP BY (timestamp * ? / 86400000), channel ";
    }

    queryStr += " ORDER BY uid ASC LIMIT ?";

    SQLite::Statement query(*DB, queryStr);

    int param_num = 0;
    for (const auto& channel : channels) {
        int  channelId = -1;
        auto it        = StoredChannelIds.find(channel);
        if (it != StoredChannelIds.end()) {
            channelId = it->second;
        }
        query.bind(++param_num, channelId);
    }

    query.bind(++param_num, duration_cast<milliseconds>(startTime.time_since_epoch()).count());
    query.bind(++param_num, duration_cast<milliseconds>(endTime.time_since_epoch()).count());
    query.bind(++param_num, startId);

    if (minInterval.count() > 0) {
        int dayFraction = 86400000 / minInterval.count(); // ms in day
        LOG(Debug) << "day: fraction :" << dayFraction;
        query.bind(++param_num, dayFraction);
    }

    query.bind(++param_num, maxRecords);

    while (query.executeStep()) {
        int  recordId = query.getColumn(0).getInt();
        bool retain   = (query.getColumn(7).getInt() > 0);

        TChannelName channel(query.getColumn(1).getString(), query.getColumn(2).getString());
        system_clock::time_point timestamp(milliseconds(query.getColumn(4).getInt64()));

        if (!query.getColumn(5).isNull()) {
            if (!visitor.ProcessRecord(recordId,
                                       StoredChannelIds[channel],
                                       channel,
                                       query.getColumn(3).getDouble(),
                                       timestamp,
                                       query.getColumn(5).getDouble(),
                                       query.getColumn(6).getDouble(),
                                       retain))
                return;

        } else {
            if (!visitor.ProcessRecord(recordId,
                                       StoredChannelIds[channel],
                                       channel,
                                       query.getColumn(3).getString(),
                                       timestamp,
                                       retain))
                return;
        }
    }
}
