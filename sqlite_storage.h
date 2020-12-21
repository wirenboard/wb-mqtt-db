#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "dblogger.h"

/**
 * @brief The class implements Istorage interface for SQLite.
 * All methods are threadsafe.
 */
class TSqliteStorage : public IStorage
{
    struct TChannelInfo
    {
        int                                   Id;
        uint32_t                              RecordCount;
        std::chrono::system_clock::time_point LastRecordTime;

        TChannelInfo();
    };

    std::unique_ptr<SQLite::Database>              DB;
    std::unordered_map<TChannelName, TChannelInfo> StoredChannelIds;
    std::unique_ptr<SQLite::Transaction>           Transaction;
    std::unique_ptr<SQLite::Statement>             InsertRowQuery;
    std::unique_ptr<SQLite::Statement>             CleanChannelQuery;
    std::unique_ptr<SQLite::Statement>             CleanGroupQuery;
    std::mutex                                     Mutex;

    void CreateTables(int dbVersion);
    void CreateIndices();

    bool CheckBackupFile(const std::string& dbFile);
    void CreateBackupFile(const std::string& dbFile);
    void RemoveBackupFile(const std::string& dbFile);
    void RestoreBackupFile(const std::string& dbFile);

    int  ReadDBVersion();
    void UpdateDB(int prev_version);

    TChannelInfo* GetOrCreateChannelId(const TChannelName& channelName);

public:
    /**
     * @brief Construct a new TSqliteStorage object
     *
     * @param dbFile full path and name of .db file
     */
    TSqliteStorage(const std::string& dbFile);
    ~TSqliteStorage();

    /**
     * @brief Load information about stored channels
     *
     * @param cache the object to fill with data from DB
     */
    void Load(TLoggerCache& cache) override;

    /**
     * @brief Write channel data into storage. One must call Commit to finalaze writing.
     */
    void WriteChannel(const TChannelName& channelName,
                      TChannel&           channel,
                      TLoggingGroup&      group) override;

    /**
     * @brief Save all modifications in DB
     */
    void Commit() override;

    /**
     * @brief Get records from storage according to constraints, call visitors ProcessRecord for every
     * record
     *
     * @param visitor an object
     * @param channels get recods only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting from the id
     * @param maxRecords maximum records to get from storage
     * @param minInterval minimum time between records
     */
    void GetRecords(IRecordsVisitor&                      visitor,
                    const std::vector<TChannelName>&      channels,
                    std::chrono::system_clock::time_point startTime,
                    std::chrono::system_clock::time_point endTime,
                    int64_t                               startId,
                    uint32_t                              maxRecords,
                    std::chrono::milliseconds             minInterval) override;

    /**
     * @brief Get channels from storage
     */
    void GetChannels(IChannelVisitor& visitor) override;
};
