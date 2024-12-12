#include <Scheduler/RemoteExecutorsManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentThread.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYSTEM_ERROR;
extern const int UNKNOWN_PACKET_FROM_SERVER;
}

void RemoteExecutorsManager::receiveReportFromRemoteServers(ThreadGroupPtr thread_group)
{
    setThreadName("rcvRmtRpt");

    if (thread_group)
        CurrentThread::attachToGroup(thread_group);

    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

    try
    {
        while (!cancelled.load())
        {
            /// TODO use epoll
            for (auto & node : managed_nodes)
            {
                if (node.is_finished)
                    continue;

                if (node.has_exception)
                {
                    try
                    {
                        LOG_DEBUG(log, "Sending cancellation to node {}", node.host_port);
                        node.connection->sendCancel();
                        node.cancellation_sent = true;
                    }
                    catch (...)
                    {
                        LOG_DEBUG(log, "Fail to send cancellation to node {}", node.host_port);
                        node.is_finished = true;
                        node.connection->disconnect();
                    }
                }

                Packet packet;
                try
                {
                    packet = node.connection->receivePacket();
                }
                catch (...)
                {
                    LOG_DEBUG(log, "Error when receiving query status info from node {}", node.host_port);
                    node.has_exception = true;
                    continue;
                }
                processPacket(packet, node, false);
            }

            if (allFinished())
            {
                LOG_DEBUG(log, "All nodes finished for query {}", query_id);
                finish_event.set();
                drain_finish_event.set();
                return;
            }
        }
    }
    catch (...)
    {
        exception_callback(std::current_exception());
    }
}

void RemoteExecutorsManager::processPacket(Packet & packet, ManagedNode & node, bool quiet) const
{
    switch (packet.type)
    {
        case Protocol::Server::Exception: {
            LOG_TRACE(log, "Got exception from {}", node.host_port); // TODO we should send cancel to it?
            exception_callback(std::make_exception_ptr(*packet.exception));
            node.has_exception = true;
            break;
        }
        case Protocol::Server::EndOfStream: {
            LOG_TRACE(log, "Got EndOfStream from {}", node.host_port);
            node.is_finished = true;
            break;
        }
        case Protocol::Server::ProfileInfo: {
            LOG_TRACE(log, "Got ProfileInfo {}", node.host_port);
            if (profile_info_callback)
                profile_info_callback(packet.profile_info);
            break;
        }
        case Protocol::Server::Log: {
            LOG_TRACE(log, "Got Log {}", node.host_port);
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
            break;
        }
        case Protocol::Server::Progress: {
            LOG_TRACE(log, "Got Progress {}", node.host_port);
            if (read_progress_callback)
            {
                LOG_DEBUG(log, "{} update progress read_rows {}", node.host_port, packet.progress.read_rows);
                LOG_DEBUG(log, "{} update progress elapsed_ns {}", node.host_port, packet.progress.elapsed_ns);

                if (packet.progress.total_rows_to_read)
                    read_progress_callback->addTotalRowsApprox(packet.progress.total_rows_to_read);

                if (!read_progress_callback->onProgress(packet.progress.read_rows, packet.progress.read_bytes, storage_limits))
                    LOG_WARNING(log, "Check Limit failed");
            }
            break;
        }
        case Protocol::Server::ProfileEvents: {
            LOG_TRACE(log, "Got ProfileEvents from {}", node.host_port);
            /// Pass profile events from remote server to client
            if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                if (!profile_queue->emplace(std::move(packet.block)))
                    throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
            break;
        }
        default:
            if (!quiet)
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
                    "Unknown packet {} from node {}",
                    packet.type,
                    node.host_port);
            }
    }
}

bool RemoteExecutorsManager::allFinished() const
{
    for (const auto & node : managed_nodes)
        if (!node.is_finished)
            return false;
    return true;
}


void RemoteExecutorsManager::receiveReportsAsync()
{
    auto func = [this, thread_group = CurrentThread::getGroup()]() { receiveReportFromRemoteServers(thread_group); };
    receive_reports_thread = ThreadFromGlobalPool(std::move(func));
}


void RemoteExecutorsManager::waitFinish()
{
    if (!allFinished())
        finish_event.wait();
}

void RemoteExecutorsManager::cancel(const String & host_port)
{
    if (cancelled)
        return;

    for (auto & node : managed_nodes)
    {
        if (node.host_port == host_port)
        {
            if (node.is_finished || node.cancellation_sent)
                return;

            LOG_DEBUG(log, "Canceling node {}", host_port);
            try
            {
                node.connection->sendCancel();
                node.cancellation_sent = true;
            }
            catch (...)
            {
                LOG_DEBUG(log, "Failed to send cancellation to node {}", node.host_port);
                node.cancellation_retry_times++;
                if (node.cancellation_retry_times > 3)
                {
                    LOG_DEBUG(log, "Failed to send cancellation to node {} for 3 times, will close connection", node.host_port);
                    node.connection->disconnect();
                    node.cancellation_sent = true;
                }
            }
            LOG_DEBUG(log, "canceling node {}", host_port);
            return;
        }
    }
    /// receive_reports_thread will drain the data after cancel
}

void RemoteExecutorsManager::cancel()
{
    if (cancelled)
        return;

    LOG_DEBUG(log, "canceling query {}", query_id);
    cancelled = true;

    /// send cancellation
    for (auto & node : managed_nodes)
    {
        if (!node.is_finished && !node.cancellation_sent)
        {
            try
            {
                LOG_DEBUG(log, "sending cancellation to node {}", node.host_port);
                node.connection->sendCancel();
                node.cancellation_sent = true;
            }
            catch (...)
            {
                LOG_DEBUG(log, "Failed to send cancellation to node {}", node.host_port);
                node.cancellation_retry_times++;
                if (node.cancellation_retry_times > 3)
                {
                    LOG_DEBUG(log, "Failed to send cancellation to node {} for 3 times, will close connection", node.host_port);
                    node.connection->disconnect();
                    node.cancellation_sent = true;
                }
            }
        }
    }

    /// wait receive_reports_thread draining packets
    if (receive_reports_thread.joinable())
        receive_reports_thread.join();

    finish_event.set();
    LOG_DEBUG(log, "cancelled query {}", query_id);
}


RemoteExecutorsManager::~RemoteExecutorsManager()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("RemoteExecutorsManager");
    }
}

RemoteExecutorsManagerPtr RemoteExecutorsManagerContainer::find(const String & query_id)
{
    std::lock_guard lock(mutex);
    return managers.contains(query_id) ? managers[query_id] : nullptr;
}

void RemoteExecutorsManagerContainer::add(const String & query_id, const RemoteExecutorsManagerPtr & remote_executors_manager)
{
    std::lock_guard lock(mutex);
    managers[query_id] = remote_executors_manager;
}

void RemoteExecutorsManagerContainer::remove(const String & query_id)
{
    std::lock_guard lock(mutex);
    managers.erase(query_id);
}

}
