#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "storage.h"
#include <mutex>

/**
 * @brief The class implements IStorage interface for SQLite.
 * All methods are threadsafe.
 */
class TSqliteStorage: public IStorage
{
    std::unique_ptr<SQLite::Database> DB;
    std::unique_ptr<SQLite::Transaction> Transaction;
    std::unique_ptr<SQLite::Statement> InsertRowQuery;
    std::unique_ptr<SQLite::Statement> CleanChannelQuery;
    std::mutex Mutex;

    void CreateTables(int dbVersion);
    void CreateIndices();

    bool CheckBackupFile(const std::string& dbFile);
    void CreateBackupFile(const std::string& dbFile);
    void RemoveBackupFile(const std::string& dbFile);
    void RestoreBackupFile(const std::string& dbFile);

    int ReadDBVersion();
    void UpdateDB(int prev_version);

    void Load();

    void GetRecordsWithoutAverage(IRecordsVisitor& visitor,
                                  const std::vector<TChannelName>& channels,
                                  std::chrono::system_clock::time_point startTime,
                                  std::chrono::system_clock::time_point endTime,
                                  int64_t startId,
                                  uint32_t maxRecords);

    void GetRecordsWithAverage(IRecordsVisitor& visitor,
                               const std::vector<TChannelName>& channels,
                               std::chrono::system_clock::time_point startTime,
                               std::chrono::system_clock::time_point endTime,
                               int64_t startId,
                               uint32_t maxRecords,
                               std::chrono::milliseconds minInterval);

    std::unordered_map<int64_t, size_t> GetRecordsCount(const std::vector<int64_t>& channelIds,
                                                        std::chrono::system_clock::time_point startTime,
                                                        std::chrono::system_clock::time_point endTime);

    int BindParams(SQLite::Statement& query,
                   int param_num,
                   const std::vector<int64_t>& channelIds,
                   std::chrono::system_clock::time_point startTime,
                   std::chrono::system_clock::time_point endTime,
                   int64_t startId);

    void ProcessGetRecordsResult(SQLite::Statement& query, IRecordsVisitor& visitor) const;

    std::vector<int64_t> GetChannelIds(const std::vector<TChannelName>& channels) const;

public:
    /**
     * @brief Construct a new TSqliteStorage object
     *
     * @param dbFile full path and name of .db file
     */
    TSqliteStorage(const std::string& dbFile);

    PChannelInfo CreateChannel(const TChannelName& channelName) override;

    /**
     * @brief Set channel's precision. One must call Commit to finalize writing to
     * storage.
     */
    void SetChannelPrecision(TChannelInfo& channelInfo, double precision) override;

    /**
     * @brief Write channel data into storage. One must call Commit to finalize
     * writing.
     */
    void WriteChannel(TChannelInfo& channelInfo,
                      const std::string& value,
                      const std::string& minimum,
                      const std::string& maximum,
                      bool retained,
                      std::chrono::system_clock::time_point time) override;

    /**
     * @brief Save all modifications in DB
     */
    void Commit() override;

    /**
     * @brief Get records from storage according to constraints, call visitors
     * ProcessRecord for every record. The whole result set is divided into chunks
     * by minInterval. All values in a chunk are averaged.
     *
     * @param visitor an object
     * @param channels get records only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting after the id
     * @param maxRecords maximum records to get from storage
     * @param minInterval averaging interval (minimum time between records), 0 -
     * without averaging
     */
    void GetRecordsWithAveragingInterval(IRecordsVisitor& visitor,
                                         const std::vector<TChannelName>& channels,
                                         std::chrono::system_clock::time_point startTime,
                                         std::chrono::system_clock::time_point endTime,
                                         int64_t startId,
                                         uint32_t maxRecords,
                                         std::chrono::milliseconds minInterval) override;

    /**
     * @brief Get records from storage according to constraints, call visitors
     * ProcessRecord for every record. If result set has less than or equal to
     * maxRecords records, it is returned as is. If more than maxRecords, it is
     * divided into maxRecords chunks. All values in a chunk are averaged.
     *
     * @param visitor an object
     * @param channels get records only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting after the id
     * @param maxRecords maximum records to get from storage
     * @param overallRecordsLimit maximum records count in whole interval between
     * startTime and endTime, 0 - without averaging
     */
    void GetRecordsWithLimit(IRecordsVisitor& visitor,
                             const std::vector<TChannelName>& channels,
                             std::chrono::system_clock::time_point startTime,
                             std::chrono::system_clock::time_point endTime,
                             int64_t startId,
                             uint32_t maxRecords,
                             size_t overallRecordsLimit) override;

    /**
     * @brief Get channels from storage
     */
    void GetChannels(IChannelVisitor& visitor) override;

    //! Delete count oldest records of channel
    void DeleteRecords(TChannelInfo& channel, uint32_t count) override;

    //! Delete count oldest records of channels
    void DeleteRecords(const std::vector<std::reference_wrapper<TChannelInfo>>& channels, uint32_t count) override;

    //! Get current DB version
    static int GetDBVersion();
};
