#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "dblogger.h"

class TSqliteStorage : public IStorage
{
    std::unique_ptr<SQLite::Database>     DB;
    std::unordered_map<TChannelName, int> StoredChannelIds;
    std::unique_ptr<SQLite::Transaction>  Transaction;

    void CreateTables(int dbVersion);
    void CreateIndices();

    bool CheckBackupFile(const std::string& dbFile);
    void CreateBackupFile(const std::string& dbFile);
    void RemoveBackupFile(const std::string& dbFile);
    void RestoreBackupFile(const std::string& dbFile);

    int  ReadDBVersion();
    void UpdateDB(int prev_version);

    void GetOrCreateChannelId(const TChannelName& channelName, TChannel& channel);

public:
    TSqliteStorage(const std::string& dbFile);
    ~TSqliteStorage();

    void Load(TLoggerCache& cache) override;
    void WriteChannel(const TChannelName& channelName,
                      TChannel&           channel,
                      TLoggingGroup&      group) override;
    void Commit() override;

    void GetRecords(IRecordsVisitor&                      visitor,
                    const std::vector<TChannelName>&      channels,
                    std::chrono::system_clock::time_point startTime,
                    std::chrono::system_clock::time_point endTime,
                    int64_t                               startId,
                    uint32_t                              maxRecords,
                    std::chrono::milliseconds             minInterval);
};
