#include <QueryCoordination/Coordinator.h>

#include <ranges>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/QueryCoordinationMetaInfo.h>
#include <QueryCoordination/fragmentsToPipelines.h>
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


Coordinator::Coordinator(const FragmentPtr & root_fragment_, const ContextMutablePtr & context_, const String & query_)
    : context(context_), query(query_), root_fragment(root_fragment_), log(&Poco::Logger::get("Coordinator"))
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

void Coordinator::schedule()
{
    assignFragmentToHost();

    for (auto & [host, fragments_for_host] : host_fragments)
        for (auto & fragment : fragments_for_host)
            LOG_DEBUG(log, "host_fragment_ids: host {}, fragment {}", host, fragment->getID());

    buildLocalPipelines();
    sendFragments();
    sendBeginExecutePipelines();
}

Pipelines && Coordinator::extractPipelines()
{
    return std::move(pipelines);
}

void Coordinator::explainPipelines()
{
    assignFragmentToHost();
    buildLocalPipelines(true);
}

// PoolBase<Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name)
// {
//     const auto & current_settings = context->getSettingsRef();
//     auto timeouts
//         = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);
//     auto try_results = shard_info.pool->getManyChecked(timeouts, current_settings, PoolMode::GET_ONE, table_name);
//     return try_results[0].entry;
// }

PoolBase<Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info)
{
    auto current_settings = context->getSettingsRef();
    auto timeouts
        = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);

    if (shard_num_to_host.contains(shard_info.shard_num))
        return shard_info.pool->getOne(timeouts, current_settings, shard_num_to_host.at(shard_info.shard_num));

    auto try_results = shard_info.pool->getMany(timeouts, current_settings, PoolMode::GET_ONE);
    shard_num_to_host[shard_info.shard_num] = try_results[0]->getHostPort();

    return try_results[0];
}

void Coordinator::assignSourceFragment(const FragmentPtr & fragment)
{
    /// Some fragments maight have multiple scan steps, we need only assign the segment based on one of the scan steps.
    ///
    ///            union
    ///           /     \
    ///  expression     expression
    ///         /         \
    ///      scan         scan

    const auto & custer_name = context->getQueryCoordinationMetaInfo().cluster_name;
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
            local_host = shard_info.local_addresses[0].toString();
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
            connection = getConnection(shard_info);
            host_port = connection->getHostPort();
        }

        auto fragment_id = fragment->getID();
        host_connection[host_port] = connection;
        fragment_hosts[fragment_id].emplace_back(host_port);
        host_fragments[host_port].emplace_back(fragment);
    }
}

void Coordinator::assignFragmentToHost()
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
            assignSourceFragment(frame.node);
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
                    for (const auto & host : child_hosts)
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

// bool Coordinator::isUpToDate(const QualifiedTableName & table_name) const
// {
//     const auto resolved_id = context->tryResolveStorageID({table_name.database, table_name.table});
//     const StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
//     if (!table)
//         return false;
//
//     TableStatus status;
//     if (const auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
//     {
//         status.is_replicated = true;
//         status.absolute_delay = static_cast<UInt32>(replicated_table->getAbsoluteDelay());
//     }
//     else
//         status.is_replicated = false;
//
//     bool is_up_to_date;
//     UInt64 max_allowed_delay = context->getSettingsRef()[Setting::max_replica_delay_for_distributed_queries];
//     if (!max_allowed_delay)
//     {
//         is_up_to_date = true;
//         return is_up_to_date;
//     }
//
//     UInt32 delay = status.absolute_delay;
//
//     if (delay < max_allowed_delay)
//         is_up_to_date = true;
//     else
//         is_up_to_date = false;
//     return is_up_to_date;
// }

std::unordered_map<UInt32, FragmentRequest> Coordinator::buildFragmentRequest()
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


void Coordinator::buildLocalPipelines(bool only_analyze)
{
    LOG_DEBUG(log, "Building local pipelines");

    const auto & current_settings = context->getSettingsRef();
    auto timeouts
        = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);

    ClientInfo modified_client_info = context->getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    const auto fragment_requests = buildFragmentRequest();

    FragmentsRequest fragments_request;
    for (const auto & fragment : host_fragments[local_host])
    {
        const auto & [_, request] = *fragment_requests.find(fragment->getID());
        fragments_request.fragments_request.emplace_back(request);
    }

    auto cluster = context->getClusters().find(context->getQueryCoordinationMetaInfo().cluster_name)->second;
    pipelines = fragmentsToPipelines(
        fragments, fragments_request.fragmentsRequest(), context->getCurrentQueryId(), context->getSettingsRef(), cluster, only_analyze);
}

void Coordinator::sendFragments()
{
    LOG_DEBUG(log, "Sending query and fragments to others to prepare pipelines. Query: {}", query);

    const auto & current_settings = context->getSettingsRef();
    auto timeouts
        = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);

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
            host_connection[host]->sendFragments(
                timeouts,
                query,
                context->getQueryParameters(),
                context->getCurrentQueryId(),
                QueryProcessingStage::Complete,
                &context->getSettingsRef(),
                &modified_client_info,
                fragments_request,
                context->getQueryCoordinationMetaInfo());
        }
    }

    /// receive ready
    for (auto & [host, _] : host_fragments)
    {
        if (host != local_host)
        {
            while (true) /// TODO receive anyway when there is a exception
            {
                auto packet = host_connection[host]->receivePacket();

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

void Coordinator::sendBeginExecutePipelines()
{
    for (const auto & [host, _] : host_fragments)
        if (host != local_host)
            host_connection[host]->sendBeginExecutePipelines(context->getCurrentQueryId());
}

std::unordered_map<String, IConnectionPool::Entry> Coordinator::getRemoteHostConnection()
{
    std::unordered_map<String, IConnectionPool::Entry> res;
    for (const auto & [host, _] : host_fragments)
        if (host != local_host)
            res.emplace(host, host_connection[host]);
    return res;
}

}
