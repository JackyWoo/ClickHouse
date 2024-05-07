#pragma once

#include <memory>
#include <Client/ConnectionPool.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/ThreadPool.h>

namespace DB
{

struct Progress;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

using setExceptionCallback = std::function<void(std::exception_ptr exception_)>;

class RemotePipelinesManager
{
public:
    struct ManagedNode
    {
        ManagedNode(const String & host_port_, const IConnectionPool::Entry & connection_)
            : is_finished(false), host_port(host_port_), connection(connection_) {}

        ManagedNode(const ManagedNode &other)
            : is_finished(other.is_finished.load()), host_port(other.host_port), connection(other.connection) {}

        std::atomic_bool is_finished;
        String host_port;
        IConnectionPool::Entry connection;
    };

    explicit RemotePipelinesManager(const StorageLimitsList & storage_limits_) : log(&Poco::Logger::get("RemotePipelinesManager"))
    {
        /// Remove leaf limits for remote pipelines manager.
        for (const auto & value : storage_limits_)
            storage_limits.emplace_back(StorageLimits{value.local_limits, {}});
    }

    ~RemotePipelinesManager();

    void setManagedNode(const std::unordered_map<String, IConnectionPool::Entry> & host_connection)
    {
        for (const auto & [host, connection] : host_connection)
        {
            managed_nodes.emplace_back(ManagedNode{host, connection});
        }
    }

    void asyncReceiveReports();

    void setExceptionCallback(setExceptionCallback exception_callback_) { exception_callback = exception_callback_; }

    /// Set callback for progress. It will be called on Progress packet.
    void setProgressCallback(ProgressCallback callback, QueryStatusPtr process_list_element)
    {
        read_progress_callback = std::make_unique<ReadProgressCallback>();
        read_progress_callback->setProgressCallback(callback);
        read_progress_callback->setProcessListElement(process_list_element);
    }

    /// Set callback for profile info. It will be called on ProfileInfo packet.
    void setProfileInfoCallback(ProfileInfoCallback callback) { profile_info_callback = std::move(callback); }

    void waitFinish();
    bool allFinished();

    void cancel();

private:
    void receiveReportFromRemoteServers(ThreadGroupPtr thread_group);

    void processPacket(Packet & packet, ManagedNode & node);

    Poco::Logger * log;

    StorageLimitsList storage_limits;

    ReadProgressCallbackPtr read_progress_callback;
    ProfileInfoCallback profile_info_callback;

    std::vector<ManagedNode> managed_nodes;

    ThreadFromGlobalPool receive_reporter_thread;

    DB::setExceptionCallback exception_callback;
    std::atomic_bool cancelled = false;

    Poco::Event finish_event{false};
};

}
