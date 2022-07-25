#pragma once

#include <chrono>
#include <string>
#include <memory>
#include <unordered_map>
#include <vector>
#include <ostream>

class IStorage;

/**
 * @brief Device name and control name pair for identification of a control
 */
struct TChannelName
{
    std::string Device;  //! Device name from MQTT /devices/XXXX
    std::string Control; //! Control name from MQTT /devices/+/controls/XXXX

    TChannelName() = default;
    TChannelName(const std::string& device, const std::string& control);

    bool operator==(const TChannelName& rhs) const;
};

// hasher for TChannelName
namespace std
{
    template <> struct hash<TChannelName>
    {
        typedef TChannelName argument_type;
        typedef std::size_t  result_type;

        result_type operator()(const argument_type& s) const
        {
            return std::hash<std::string>()(s.Device) ^ std::hash<std::string>()(s.Control);
        }
    };
}; // namespace std

std::ostream& operator<<(std::ostream& out, const struct TChannelName& name);

/**
 * @brief Device name and control name pair for identification of a control
 */
class TChannelInfo
{
    friend class IStorage; // Only IStorage can create and modify TChannelInfo

    int64_t                               Id;
    int                                   RecordCount;
    std::chrono::system_clock::time_point LastRecordTime;
    TChannelName                          Name;
    double                                Precision;

    TChannelInfo(int64_t id, const std::string& device, const std::string& control);

    TChannelInfo(const TChannelInfo&) = delete;
    TChannelInfo& operator=(const TChannelInfo&) = delete;
public:
    const TChannelName&                          GetName() const;
    int                                          GetRecordCount() const;
    const std::chrono::system_clock::time_point& GetLastRecordTime() const;
    int64_t                                      GetId() const;

    //! Value's precision if channel stores numbers. 0.0 - no rounding
    double GetPrecision() const;
};

typedef std::shared_ptr<TChannelInfo> PChannelInfo;

class IRecordsVisitor
{
public:
    virtual ~IRecordsVisitor() = default;

    virtual bool ProcessRecord(int                                   recordId,
                               const TChannelInfo&                   channel,
                               double                                averageValue,
                               std::chrono::system_clock::time_point timestamp,
                               double                                minValue,
                               double                                maxValue,
                               bool                                  retain) = 0;

    virtual bool ProcessRecord(int                                   recordId,
                               const TChannelInfo&                   channel,
                               const std::string&                    value,
                               std::chrono::system_clock::time_point timestamp,
                               bool                                  retain) = 0;
};

class IChannelVisitor
{
public:
    virtual ~IChannelVisitor() = default;

    virtual void ProcessChannel(PChannelInfo channel) = 0;
};

/**
 * @brief An interface for storages. All methods must be threadsafe
 */
class IStorage
{
public:
    virtual ~IStorage() = default;

    virtual PChannelInfo CreateChannel(const TChannelName& channelName) = 0;

    /**
     * @brief Set channel's precision. One must call Commit to finalize writing to storage.
     */
    virtual void SetChannelPrecision(TChannelInfo& channelInfo, double precision) = 0;

    /**
     * @brief Write channel data into storage. One must call Commit to finalize writing.
     */
    virtual void WriteChannel(TChannelInfo&                         channelInfo,
                              const std::string&                    value,
                              const std::string&                    minimum,
                              const std::string&                    maximum,
                              bool                                  retained,
                              std::chrono::system_clock::time_point time) = 0;

    /**
     * @brief Save all modifications
     */
    virtual void Commit() = 0;

    /**
     * @brief Get records from storage according to constraints, call visitors ProcessRecord for every
     *        record. The whole result set is divided into chunks by minInterval.
     *        All values in a chunk are averaged.
     *
     * @param visitor an object
     * @param channels get records only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting after the id
     * @param maxRecords maximum records to get from storage
     * @param minInterval averaging interval (minimum time between records), 0 - without averaging
     */
    virtual void GetRecordsWithAveragingInterval
        (IRecordsVisitor&                      visitor,
         const std::vector<TChannelName>&      channels,
         std::chrono::system_clock::time_point startTime,
         std::chrono::system_clock::time_point endTime,
         int64_t                               startId,
         uint32_t                              maxRecords,
         std::chrono::milliseconds             minInterval) = 0;

    /**
     * @brief Get records from storage according to constraints, call visitors ProcessRecord for every
     *        record. If result set has less than or equal to maxRecords records, it is returned as is.
     *        If more than maxRecords, it is divided into maxRecords chunks.
     *        All values in a chunk are averaged.
     *
     * @param visitor an object
     * @param channels get records only for these channels
     * @param startTime get records stored starting from the time
     * @param endTime get records stored before the time
     * @param startId get records stored starting after the id
     * @param maxRecords maximum records to get from storage
     * @param overallRecordsLimit maximum records count in whole interval between startTime and endTime, 
     *                            0 - without averaging
     */
    virtual void GetRecordsWithLimit
        (IRecordsVisitor&                      visitor,
         const std::vector<TChannelName>&      channels,
         std::chrono::system_clock::time_point startTime,
         std::chrono::system_clock::time_point endTime,
         int64_t                               startId,
         uint32_t                              maxRecords,
         size_t                                overallRecordsLimit) = 0;

    /**
     * @brief Get channels from storage
     */
    virtual void GetChannels(IChannelVisitor& visitor) = 0;

    virtual void DeleteRecords(TChannelInfo& channel, uint32_t count) = 0;
    virtual void DeleteRecords(const std::vector<std::reference_wrapper<TChannelInfo>>& channels, uint32_t count) = 0;

protected:
    PChannelInfo CreateChannelPrivate(uint64_t id, const std::string& device, const std::string& control);
    void SetRecordCount(TChannelInfo& channel, int recordCount);
    void SetLastRecordTime(TChannelInfo& channel, const std::chrono::system_clock::time_point& time);
    void SetPrecision(TChannelInfo& channel, double precision);
    const std::unordered_map<TChannelName, PChannelInfo>& GetChannelsPrivate() const;
    PChannelInfo FindChannel(const TChannelName& channelName) const;

private:
    std::unordered_map<TChannelName, PChannelInfo> Channels;
};
