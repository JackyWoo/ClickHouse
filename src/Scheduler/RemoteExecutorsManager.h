#pragma once

#include <Client/ConnectionPool.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/ThreadPool.h>

namespace DB
{

struct Progress;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

struct ProfileInfo;
using ProfileInfoCallback = std::function<void(const ProfileInfo & info)>;

using SetExceptionCallback = std::function<void(std::exception_ptr exception_)>;


/**
 * Manage the query state for the non-root servers. It will collect the log,
 * exception, progress, profiles and send to the client.
 */
class RemoteExecutorsManager
{
public:
    struct ManagedNode
    {
        ManagedNode(const String & host_port_, const IConnectionPool::Entry & connection_)
            : is_finished(false), has_exception(false), cancellation_sent(false), host_port(host_port_), connection(connection_)
        {
        }

        ManagedNode(const ManagedNode & other)
            : is_finished(other.is_finished.load())
            , has_exception(other.has_exception.load())
            , cancellation_sent(other.cancellation_sent.load())
            , host_port(other.host_port)
            , connection(other.connection)
        {
        }

        std::atomic_bool is_finished;
        std::atomic_bool has_exception;
        std::atomic_bool cancellation_sent; /// whether cancellation has sent
        size_t cancellation_retry_times = 0;
        String host_port;
        IConnectionPool::Entry connection;
    };

    explicit RemoteExecutorsManager(const String & query_id_, const StorageLimitsList & storage_limits_) : query_id(query_id_), log(&Poco::Logger::get("RemoteExecutorsManager"))
    {
        /// Remove leaf limits for remote pipelines manager.
        for (const auto & value : storage_limits_)
            storage_limits.emplace_back(StorageLimits{value.local_limits, {}});
    }

    ~RemoteExecutorsManager();

    void setManagedNode(const std::unordered_map<String, IConnectionPool::Entry> & host_connection)
    {
        for (const auto & [host, connection] : host_connection)
        {
            managed_nodes.emplace_back(ManagedNode{host, connection});
        }
    }

    void receiveReportsAsync();

    void setExceptionCallback(SetExceptionCallback exception_callback_) { exception_callback = exception_callback_; }

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
    bool allFinished() const;

    void cancel();
    void cancel(const String & host_port);

private:
    void receiveReportFromRemoteServers(ThreadGroupPtr thread_group);
    void processPacket(Packet & packet, ManagedNode & node, bool quiet) const;


    String query_id;
    StorageLimitsList storage_limits;

    ReadProgressCallbackPtr read_progress_callback;
    ProfileInfoCallback profile_info_callback;

    std::vector<ManagedNode> managed_nodes;

    ThreadFromGlobalPool receive_reports_thread;

    SetExceptionCallback exception_callback;
    std::atomic_bool cancelled = false;

    Poco::Event finish_event{false};
    Poco::Event drain_finish_event{false};

    Poco::Logger * log;
};

using RemoteExecutorsManagerPtr = std::shared_ptr<RemoteExecutorsManager>;

class RemoteExecutorsManagerContainer
{
public:
    using RemoteExecutorsManagersPtr = std::shared_ptr<RemoteExecutorsManagerContainer>;

    static RemoteExecutorsManagerContainer & getInstance()
    {
        static RemoteExecutorsManagerContainer managers;
        return managers;
    }

    RemoteExecutorsManagerPtr find(const String & query_id);

    void add(const String & query_id, const RemoteExecutorsManagerPtr & remote_executors_manager);
    void remove(const String & query_id);

private:
    std::mutex mutex;
    std::unordered_map<String, RemoteExecutorsManagerPtr> managers;
};

}
