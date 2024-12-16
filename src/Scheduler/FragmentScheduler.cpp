#include <Scheduler/FragmentScheduler.h>

#include <ranges>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Scheduler/Exchange/ExchangeDataStep.h>
#include <Scheduler/Fragments/FragmentBuilder.h>
#include <Scheduler/DistributedTablesInfo.h>
#include <Scheduler/fragmentsToPipelines.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYSTEM_ERROR;
extern const int CLUSTER_DOESNT_EXIST;
}

namespace Setting
{
extern const SettingsSeconds max_execution_time;
extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
}


namespace
{
ConnectionTimeouts getConnectionTimeouts(const Settings & settings)
{
    return ConnectionTimeouts::getTCPTimeoutsWithFailover(settings).getSaturated(settings[Setting::max_execution_time]);
}
}

FragmentScheduler::FragmentScheduler(const FragmentPtr & root_fragment_, const ContextMutablePtr & context_, const String & query_)
    : context(context_), query(query_), root_fragment(root_fragment_), log(&Poco::Logger::get("FragmentScheduler"))
{
    std::stack<FragmentPtr> stack;
    stack.push(root_fragment);
    while (!stack.empty())
    {
        auto fragment = stack.top();
        stack.pop();
        fragments.push_back(fragment);
        id_to_fragment[fragment->getID()] = fragment;
        for (const auto & child : fragment->getChildren())
            stack.push(child);
    }
}

void FragmentScheduler::schedule()
{
    assignFragmentToHost();

    for (const auto & [host, fragments_for_host] : host_fragments)
    {
        String joined_segments;
        for (const auto & fragment : fragments_for_host)
            joined_segments += std::to_string(fragment->getID()) + ", ";
        LOG_DEBUG(log, "Assign segments [{}] to host {}", joined_segments, host);
    }

    buildLocalPipelines();

    /// TODO multi-threads?
    Stopwatch watch;
    sendFragments();
    LOG_DEBUG(log, "Send fragments to remote hosts, elapsed {}ms", watch.elapsed() / 1000);
    sendBeginExecutePipelines();
    LOG_DEBUG(log, "Scheduling the query, elapsed {}ms", watch.elapsed() / 1000);
}

FragmentPipelines && FragmentScheduler::extractPipelines()
{
    return std::move(pipelines);
}

void FragmentScheduler::explainPipelines()
{
    assignFragmentToHost();
    buildLocalPipelines(true);
}

PoolBase<Connection>::Entry FragmentScheduler::getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name) const
{
    const auto & current_settings = context->getSettingsRef();
    auto try_results = shard_info.pool->getManyChecked(getConnectionTimeouts(context->getSettingsRef()), current_settings, PoolMode::GET_ONE, table_name);
    return try_results[0].entry;
}

PoolBase<Connection>::Entry FragmentScheduler::getConnection(const Cluster::ShardInfo & shard_info) const
{
    auto current_settings = context->getSettingsRef();
    auto try_results = shard_info.pool->getMany(getConnectionTimeouts(context->getSettingsRef()), current_settings, PoolMode::GET_ONE);
    return try_results[0];
}

void FragmentScheduler::assignLeafFragment(const FragmentPtr & fragment)
{
    const auto & custer_name = context->getDistributedTablesInfo().cluster_name;
    if (!context->getClusters().contains(custer_name))
        throw Exception(
            ErrorCodes::CLUSTER_DOESNT_EXIST, "Cluster {} does not exist", custer_name);

    const auto & cluster = context->getClusters().find(custer_name)->second;

    for (const auto & shard_info : cluster->getShardsInfo())
    {
        PoolBase<Connection>::Entry connection;
        String host_port;
        if (shard_info.isLocal())
        {
            local_host = shard_info.local_addresses[0].readableString();
            host_port = local_host;
            /// the connection of local host is empty

            // const auto & node = *any_scan_it;
            // /// right now we only support ReadFromMergeTree
            // if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(node.step.get()))
            // {
            //     const auto table_name = read_step->getStorageSnapshot()->storage.getStorageID().getQualifiedName();
            //     /// whether we use local table or remote table
            //     if (!isUpToDate(table_name))
            //     {
            //         connection = getConnection(shard_info, table_name);
            //         host_port = connection->getHostPort();
            //     }
            // }
        }
        else
        {
            /// we can have multiple leaf fragments but we only need one connection for one host.
            if (shard_num_to_host.contains(shard_info.shard_num))
            {
                host_port = shard_num_to_host[shard_info.shard_num];
                connection = host_to_connection[host_port];
            }
            else
            {
                connection = getConnection(shard_info);
                host_port = connection->getReadableHostPort();
                shard_num_to_host[shard_info.shard_num] = host_port;
            }
        }

        if (!host_to_connection.contains(host_port))
            host_to_connection[host_port] = connection;

        fragment_hosts[fragment->getID()].emplace_back(host_port);
        host_fragments[host_port].emplace_back(fragment);
    }
}

void FragmentScheduler::assignFragmentToHost()
{
    struct Frame
    {
        FragmentPtr node;
        size_t visited_child_size = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{root_fragment, 0});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (frame.node->getChildren().empty())
        {
            /// leaf node
            assignLeafFragment(frame.node);
            stack.pop();
        }
        else
        {
            if (frame.visited_child_size < frame.node->getChildren().size())
            {
                stack.push(Frame{frame.node->getChildren()[frame.visited_child_size], 0});
                ++frame.visited_child_size;
            }
            else
            {
                /// scheduled by the first child node
                ///             node
                ///             /  \
                ///      Exchange  Exchange
                ///            |    |
                ///          node  node
                chassert(!frame.node->getChildren().empty());
                auto & child_fragment = frame.node->getChildren()[0];
                auto & child_hosts = fragment_hosts[child_fragment->getID()];

                if (typeid_cast<ExchangeDataStep *>(child_fragment->getDestExchangeNode()->step.get())->isSingleton())
                {
                    if (!frame.node->hasDestFragment()) /// root fragment
                    {
                        host_fragments[local_host].emplace_back(frame.node);
                        fragment_hosts[frame.node->getID()].emplace_back(local_host);
                    }
                    else
                    {
                        /// multiple node -> single node, choose the first node, we can also use local_host
                        const auto & host = child_hosts[0];
                        host_fragments[host].emplace_back(frame.node);
                        fragment_hosts[frame.node->getID()].emplace_back(host);
                    }
                }
                else /// Hashed or Replicated
                {
                    /// Assign the fragment to all hosts, there are two cases:
                    ///     1. multiple node -> multiple node
                    ///     2. single node -> multiple node
                    ///
                    /// For single node -> multiple node, we can not use child_hosts directly, because the child_hosts contains only one host.
                    ///             |
                    ///        CreatingSets
                    ///        /         \
                    ///     Scan        CreatingSet
                    ///                   |
                    ///                 ExchangeData(Replicated)
                    ///                   |
                    ///                 Limit
                    ///                   |
                    ///                 ExchangeData(Singleton)
                    ///                   |
                    ///                 Scan
                    ///
                    /// Here we can use host_fragments, for all hosts are added to it when we handle leaf fragments.
                    for (const auto & [host, _] : host_fragments)
                    {
                        host_fragments[host].emplace_back(frame.node);
                        fragment_hosts[frame.node->getID()].emplace_back(host);
                    }
                }
                stack.pop();
            }
        }
    }
}

bool FragmentScheduler::isUpToDate(const QualifiedTableName & table_name) const
{
    const auto resolved_id = context->tryResolveStorageID({table_name.database, table_name.table});
    const StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
    if (!table)
        return false;

    TableStatus status;
    if (const auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        status.is_replicated = true;
        status.absolute_delay = static_cast<UInt32>(replicated_table->getAbsoluteDelay());
    }
    else
        status.is_replicated = false;

    bool is_up_to_date;
    UInt64 max_allowed_delay = context->getSettingsRef()[Setting::max_replica_delay_for_distributed_queries];
    if (!max_allowed_delay)
    {
        is_up_to_date = true;
        return is_up_to_date;
    }

    UInt32 delay = status.absolute_delay;

    if (delay < max_allowed_delay)
        is_up_to_date = true;
    else
        is_up_to_date = false;
    return is_up_to_date;
}

std::unordered_map<UInt32, FragmentRequest> FragmentScheduler::buildFragmentRequest()
{
    std::unordered_map<UInt32, FragmentRequest> fragment_requests;

    /// assign fragment id
    for (auto & [fragment_id, _] : fragment_hosts)
    {
        auto & request = fragment_requests[fragment_id];
        request.fragment_id = fragment_id;
    }

    /// assign data to and data from
    for (auto & [fragment_id, hosts] : fragment_hosts)
    {
        auto & request = fragment_requests[fragment_id];

        auto fragment = id_to_fragment[fragment_id];
        auto dest_fragment = id_to_fragment[fragment->getDestFragmentID()];

        Destinations data_to;
        if (dest_fragment)
        {
            auto dest_exchange_id = fragment->getDestExchangeID();
            auto dest_fragment_id = dest_fragment->getID();
            data_to = fragment_hosts[dest_fragment_id];

            /// dest_fragment exchange data_from is current fragment hosts
            auto & dest_request = fragment_requests[dest_fragment_id];
            auto & exchange_data_from = dest_request.data_from[dest_exchange_id];
            exchange_data_from.insert(exchange_data_from.begin(), hosts.begin(), hosts.end());
        }

        request.data_to = data_to;
    }

    return fragment_requests;
}


void FragmentScheduler::buildLocalPipelines(bool only_analyze)
{
    LOG_DEBUG(log, "Building local pipelines");
    const auto fragment_requests = buildFragmentRequest();

    FragmentsRequest fragments_request;
    for (const auto & fragment : host_fragments[local_host])
    {
        const auto & [_, request] = *fragment_requests.find(fragment->getID());
        fragments_request.fragments_request.emplace_back(request);
    }

    auto cluster = context->getClusters().find(context->getDistributedTablesInfo().cluster_name)->second;
    pipelines = fragmentsToPipelines(
        fragments, fragments_request.fragmentsRequest(), context->getCurrentQueryId(), context->getSettingsRef(), cluster, only_analyze);
}

void FragmentScheduler::sendFragments()
{
    LOG_DEBUG(log, "Sending query and fragments to others to prepare pipelines. Query: {}", query);

    ClientInfo modified_client_info = context->getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    const auto fragment_requests = buildFragmentRequest();

    /// send fragments
    for (const auto & [host, fragments_for_send] : host_fragments)
    {
        FragmentsRequest fragments_request;
        for (const auto & fragment : fragments_for_send)
        {
            const auto & [_, request] = *fragment_requests.find(fragment->getID());
            fragments_request.fragments_request.emplace_back(request);
        }

        if (host != local_host)
        {
            host_to_connection[host]->sendFragments(
                getConnectionTimeouts(context->getSettingsRef()),
                query,
                context->getQueryParameters(),
                context->getCurrentQueryId(),
                QueryProcessingStage::Complete,
                &context->getSettingsRef(),
                &modified_client_info,
                fragments_request,
                context->getDistributedTablesInfo());
        }
    }

    /// receive ready
    for (auto & [host, _] : host_fragments)
    {
        if (host != local_host)
        {
            while (true) /// TODO receive anyway when there is a exception
            {
                auto packet = host_to_connection[host]->receivePacket();

                if (packet.type == Protocol::Server::Exception)
                    packet.exception->rethrow();

                /// receive logs from remote and send it to client, used
                /// 1. when user set send_logs_level to higher level e.g. trace or debug
                /// 2. when remote fails to build pipeline, they will send the error log for logical exception but not exception
                /// when we read a log we continue reading the next packet but for the 2nd case we will get an eof error. TODO fix it
                if (packet.type == Protocol::Server::Log)
                {
                    if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                        log_queue->pushBlock(std::move(packet.block));
                    continue;
                }

                if (packet.type == Protocol::Server::PipelinesReady)
                    break;

                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Received {} but not PipelinesReady from {}",
                    Protocol::Server::toString(packet.type),
                    host);
            }
        }
    }
}

void FragmentScheduler::sendBeginExecutePipelines()
{
    for (const auto & [host, _] : host_fragments)
        if (host != local_host)
            host_to_connection[host]->sendBeginExecutePipelines(context->getCurrentQueryId());
}

std::unordered_map<String, IConnectionPool::Entry> FragmentScheduler::getRemoteHostConnection()
{
    std::unordered_map<String, IConnectionPool::Entry> res;
    for (const auto & [host, _] : host_fragments)
        if (host != local_host)
            res.emplace(host, host_to_connection[host]);
    return res;
}

}
