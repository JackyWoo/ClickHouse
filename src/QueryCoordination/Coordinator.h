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
    Pipelines && extractPipelines();

    /// build local pipelines, only used for explaining pipeline
    void explainPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

private:
    void assignFragmentToHost();

    FragmentToHosts assignSourceFragment();

    PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name) const;
    PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info) const;

    bool isUpToDate(const QualifiedTableName & table_name) const;

    void buildLocalPipelines(bool only_analyze = false);
    void sendFragments();
    void sendBeginExecutePipelines();

    std::unordered_map<UInt32, FragmentRequest> buildFragmentRequest();

    ContextMutablePtr context;
    String query;

    const FragmentPtrs & fragments;
    std::unordered_map<FragmentID, FragmentPtr> id_to_fragment;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;

    /// Local pipelines, every fragment will be translated to a pipeline.
    Pipelines pipelines;

    // all destinations
    std::unordered_map<String, IConnectionPool::Entry> host_connection;

    String local_host;

    Poco::Logger * log;
};

}
