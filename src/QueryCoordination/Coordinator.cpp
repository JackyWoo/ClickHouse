#include <QueryCoordination/Coordinator.h>

#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/QueryCoordinationMetaInfo.h>
#include <QueryCoordination/fragmentsToPipelines.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <ranges>

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

Coordinator::Coordinator(const FragmentPtrs & fragments_, const ContextMutablePtr & context_, const String & query_)
    :context(context_), query(query_), fragments(fragments_), log(&Poco::Logger::get("Coordinator"))
{
    for (const auto & fragment : fragments)
    {
        auto fragment_id = fragment->getFragmentID();
        id_to_fragment[fragment_id] = fragment;
    }
}

void Coordinator::schedule()
{
    // If the fragment has a scan step, it is scheduled according to the cluster topology
    assignFragmentToHost();

    for (auto & [host, fragment_ids] : host_fragments)
        for (auto & fragment : fragment_ids)
            LOG_DEBUG(log, "host_fragment_ids: host {}, fragment {}", host, fragment->getFragmentID());

    buildLocalPipelines();
    sendFragmentsToPreparePipelines();
    sendBeginExecutePipelines();
}

void Coordinator::buildPipelines()
{
    assignFragmentToHost();
    buildLocalPipelines(true);
}

PoolBase<DB::Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name) const
{
    const auto & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);
    auto try_results = shard_info.pool->getManyChecked(timeouts, current_settings, PoolMode::GET_ONE, table_name);
    return try_results[0].entry;
}

PoolBase<DB::Connection>::Entry Coordinator::getConnection(const Cluster::ShardInfo & shard_info) const
{
    auto current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);
    auto try_results = shard_info.pool->getMany(timeouts, current_settings, PoolMode::GET_ONE);
    return try_results[0];
}

FragmentToHosts Coordinator::assignSourceFragment()
{
    FragmentToHosts scan_fragment_hosts;
    for (const auto & fragment : fragments)
    {
        /// Some fragments maight have multiple scan steps, we need only assign the segment based on one of the scan steps.
        ///
        ///            union
        ///           /     \
        ///  expression     expression
        ///         /         \
        ///      scan         scan
        auto any_scan_it = std::ranges::find_if(
            fragment->getNodes().cbegin(),
            fragment->getNodes().cend(),
            [](const auto & node) { return node.step->stepType() == StepType::Scan; });

        if (any_scan_it != fragment->getNodes().cend())
        {
            if (!context->getClusters().contains(context->getQueryCoordinationMetaInfo().cluster_name))
                throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "Cluster {} does not exist", context->getQueryCoordinationMetaInfo().cluster_name);

            const auto & cluster = context->getClusters().find(context->getQueryCoordinationMetaInfo().cluster_name)->second;

            for (const auto & shard_info : cluster->getShardsInfo())
            {
                PoolBase<DB::Connection>::Entry connection;
                String host_port;
                if (shard_info.isLocal())
                {
                    local_host = shard_info.local_addresses[0].toString();
                    host_port = local_host;

                    const auto& node = *any_scan_it;
                    if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(node.step.get()))
                    {
                        const auto & table_name = read_step->getStorageSnapshot()->storage.getStorageID().getQualifiedName();
                        if (!isUpToDate(table_name))
                        {
                            connection = getConnection(shard_info, table_name);
                            host_port = connection->getHostPort();
                        }
                    }
                }
                else
                {
                    connection = getConnection(shard_info);
                    host_port = connection->getHostPort();
                }

                auto fragment_id = fragment->getFragmentID();
                scan_fragment_hosts[fragment_id].emplace_back(host_port);
                host_connection[host_port] = connection;
                fragment_hosts[fragment_id].emplace_back(host_port);
                host_fragments[host_port].emplace_back(fragment);
            }
        }
    }

    return scan_fragment_hosts;
}

void Coordinator::assignFragmentToHost()
{
    /// Process leaf fragments
    const std::unordered_map<UInt32, std::vector<String>> scan_fragment_hosts = assignSourceFragment();

    // For a fragment with a scan step, process its destination (parent) fragment.

    auto process_other_fragment
        = [this](FragmentToHosts & fragment_hosts_) -> FragmentToHosts
    {
        FragmentToHosts this_fragment_hosts;
        for (const auto & [fragment_id, hosts] : fragment_hosts_)
        {
            const auto & fragment = id_to_fragment[fragment_id];
            auto dest_fragment_id = fragment->getDestFragmentID();

            if (!fragment->hasDestFragment())
                return this_fragment_hosts;

            auto dest_fragment = id_to_fragment[dest_fragment_id];

            /// dest_fragment scheduling by the first child node
            if (dest_fragment->getChildren().size() > 1)
            {
                if (fragment_id != dest_fragment->getChildren()[0]->getFragmentID())
                    continue;
            }

            if (fragment_hosts_.contains(dest_fragment_id))
                return this_fragment_hosts;

            if (typeid_cast<ExchangeDataStep *>(fragment->getDestExchangeNode()->step.get())->isSingleton())
            {
                if (!dest_fragment->hasDestFragment()) /// root fragment
                {
                    host_fragments[local_host].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment_id].emplace_back(local_host);
                    this_fragment_hosts[dest_fragment_id].emplace_back(local_host);
                }
                else
                {
                    const auto & host = hosts[0]; /// multiple node -> single node, choose the first node /// TODO use local host
                    host_fragments[host].emplace_back(dest_fragment);
                    fragment_hosts[dest_fragment_id].emplace_back(host);
                    this_fragment_hosts[dest_fragment_id].emplace_back(host);
                }
            }
            else  /// Hashed
            {
                for (const auto & host : hosts)
                {
                    auto & dest_hosts = fragment_hosts[dest_fragment_id];
                    if (!std::ranges::count(dest_hosts.begin(), dest_hosts.end(), host))
                    {
                        host_fragments[host].emplace_back(dest_fragment);
                        dest_hosts.emplace_back(host);
                        this_fragment_hosts[dest_fragment_id].emplace_back(host);
                    }
                }
            }

        }
        return this_fragment_hosts;
    };

    std::optional<FragmentToHosts> fragment_hosts_(scan_fragment_hosts);
    while (!fragment_hosts_->empty())
    {
        auto tmp = process_other_fragment(fragment_hosts_.value());
        fragment_hosts_->swap(tmp);
    }
}

bool Coordinator::isUpToDate(const QualifiedTableName & table_name) const
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
            auto dest_fragment_id = dest_fragment->getFragmentID();
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
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);

    ClientInfo modified_client_info = context->getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    const std::unordered_map<UInt32, FragmentRequest> & fragment_requests = buildFragmentRequest();

    FragmentsRequest fragments_request;
    for (const auto & fragment : host_fragments[local_host])
    {
        const auto & [_, request] = *fragment_requests.find(fragment->getFragmentID());
        fragments_request.fragments_request.emplace_back(request);
    }

    auto cluster = context->getClusters().find(context->getQueryCoordinationMetaInfo().cluster_name)->second;
    pipelines = fragmentsToPipelines(
        fragments, fragments_request.fragmentsRequest(), context->getCurrentQueryId(), context->getSettingsRef(), cluster, only_analyze);
}

void Coordinator::sendFragmentsToPreparePipelines()
{
    LOG_DEBUG(log, "Sending fragments to others to prepare pipelines.");

    const std::unordered_map<UInt32, FragmentRequest> & fragment_requests = buildFragmentRequest();
    for (const auto & [f_id, request] : fragment_requests)
        LOG_DEBUG(log, "Fragment {} request {}", f_id, request.toString());

    const auto & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings[Setting::max_execution_time]);

    ClientInfo modified_client_info = context->getClientInfo();
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    /// send
    for (const auto& [host, fragments_for_send] : host_fragments)
    {
        FragmentsRequest fragments_request;
        for (const auto & fragment : fragments_for_send)
        {
            const auto & [_, request] = *fragment_requests.find(fragment->getFragmentID());
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
            auto package = host_connection[host]->receivePacket();

            if (package.type == Protocol::Server::Exception)
                package.exception->rethrow();

            if (package.type != Protocol::Server::PipelinesReady)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Received {} but not PipelinesReady from {}",
                    Protocol::Server::toString(package.type),
                    host);
        }
    }
}

void Coordinator::sendBeginExecutePipelines()
{
    for (const auto& [host, _] : host_fragments)
        if (host != local_host)
            host_connection[host]->sendBeginExecutePipelines(context->getCurrentQueryId());
}

std::unordered_map<String, IConnectionPool::Entry> Coordinator::getRemoteHostConnection()
{
    std::unordered_map<String, IConnectionPool::Entry> res;
    for (const auto& [host, _] : host_fragments)
        if (host != local_host)
            res.emplace(host, host_connection[host]);
    return res;
}

}
