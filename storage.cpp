#include "storage.h"

TChannelName::TChannelName(const std::string& device, const std::string& control)
    : Device(device), Control(control)
{
}

bool TChannelName::operator==(const TChannelName& rhs) const
{
    return std::tie(this->Device, this->Control) == std::tie(rhs.Device, rhs.Control);
}

std::ostream& operator<<(std::ostream& out, const TChannelName& name)
{
    out << name.Device << "/" << name.Control;
    return out;
}

TChannelInfo::TChannelInfo(int64_t id, const std::string& device, const std::string& control)
    : Id(id), RecordCount(0), Name(device, control), Precision(0.0)
{}

const TChannelName& TChannelInfo::GetName() const
{
    return Name;
}

int32_t TChannelInfo::GetRecordCount() const
{
    return RecordCount;
}

const std::chrono::system_clock::time_point& TChannelInfo::GetLastRecordTime() const
{
    return LastRecordTime;
}

int64_t TChannelInfo::GetId() const
{
    return Id;
}

double TChannelInfo::GetPrecision() const
{
    return Precision;
}

void TChannelInfo::SetRecordCount(int recordCount)
{
    RecordCount = (recordCount < 0) ? 0 : recordCount;
}

void TChannelInfo::SetLastRecordTime(const std::chrono::system_clock::time_point& time)
{
    LastRecordTime = time;
}

void TChannelInfo::SetPrecision(double precision)
{
    Precision = precision;
}

PChannelInfo IStorage::CreateChannelPrivate(uint64_t id, const std::string& device, const std::string& control)
{
    PChannelInfo p(new TChannelInfo(id, device, control));
    Channels.emplace(p->GetName(), p);
    return p;
}

const std::unordered_map<TChannelName, PChannelInfo>& IStorage::GetChannelsPrivate() const
{
    return Channels;
}

PChannelInfo IStorage::FindChannel(const TChannelName& channelName) const
{
    auto it = Channels.find(channelName);
    if (it != Channels.end()) {
        return it->second;
    }
    return {};
}
