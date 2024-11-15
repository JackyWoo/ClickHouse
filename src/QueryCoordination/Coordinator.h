#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentRequest.h>
#include <QueryCoordination/Pipelines/Pipelines.h>

namespace DB
{

using Hosts = std::vector<String>;
using FragmentID = UInt32;

using HostToFragments = std::unordered_map<String, FragmentPtrs>;
using FragmentToHosts = std::unordered_map<FragmentID, Hosts>;


class Coordinator
{
public:
    explicit Coordinator(const FragmentPtrs & fragments_, const ContextMutablePtr & context_, const String & query_);

    void schedule();

    /// build local pipelines, only used for explain pipeline
    void buildPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

    Pipelines pipelines;

private:
    void assignFragmentToHost();

    FragmentToHosts assignSourceFragment();

    PoolBase<DB::Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name) const;
    PoolBase<DB::Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info) const;

    bool isUpToDate(const QualifiedTableName & table_name) const;

    void buildLocalPipelines(bool only_analyze = false);
    void sendFragmentsToPreparePipelines();
    void sendBeginExecutePipelines();

    std::unordered_map<UInt32, FragmentRequest> buildFragmentRequest();

    ContextMutablePtr context;
    String query;

    const FragmentPtrs & fragments;
    std::unordered_map<FragmentID, FragmentPtr> id_to_fragment;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;

    // all destinations
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    String local_host;

    Poco::Logger * log;
};

}
