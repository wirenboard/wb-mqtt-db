#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "dblogger.h"

/**
 * @brief The class implements Istorage interface for SQLite.
 * All methods are threadsafe.
 */
class TSqliteStorage : public IStorage
{
    std::unique_ptr<SQLite::Database>              DB;
    std::unique_ptr<SQLite::Transaction>           Transaction;
    std::unique_ptr<SQLite::Statement>             InsertRowQuery;
    std::unique_ptr<SQLite::Statement>             CleanChannelQuery;
    std::mutex                                     Mutex;

    void CreateTables(int dbVersion);
    void CreateIndices();

    bool CheckBackupFile(const std::string& dbFile);
    void CreateBackupFile(const std::string& dbFile);
    void RemoveBackupFile(const std::string& dbFile);
    void RestoreBackupFile(const std::string& dbFile);

    int  ReadDBVersion();
    void UpdateDB(int prev_version);

    void Load();

public:
    /**
     * @brief Construct a new TSqliteStorage object
     *
     * @param dbFile full path and name of .db file
     */
    TSqliteStorage(const std::string& dbFile);

    PChannelInfo CreateChannel(const TChannelName& channelName);

    /**
     * @brief Set channel's precision. One must call Commit to finalaze writing to storage.
     */
    void SetChannelPrecision(TChannelInfo& channelInfo, double precision);

    /**
     * @brief Write channel data into storage. One must call Commit to finalaze writing.
     */
    void WriteChannel(TChannelInfo&                         channelInfo,
                      const std::string&                    value,
                      const std::string&                    minimum,
                      const std::string&                    maximum,
                      bool                                  retained,
                      std::chrono::system_clock::time_point time) override;

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

    //! Delete count oldest records of channel
    void DeleteRecords(TChannelInfo& channel, uint32_t count);

    //! Delete count oldest records of channels
    void DeleteRecords(const std::vector<PChannelInfo>& channels, uint32_t count);

    //! Get current DB version
    static int GetDBVersion();
};
