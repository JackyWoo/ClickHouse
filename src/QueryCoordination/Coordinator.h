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
    explicit Coordinator(const FragmentPtr & root_fragment_, const ContextMutablePtr & context_, const String & query_);

    void schedule();
    Pipelines && extractPipelines();

    /// build local pipelines, only used for explaining pipeline
    void explainPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

private:
    void assignFragmentToHost();

    void assignSourceFragment(const FragmentPtr & fragment);

    [[ maybe_unused ]]PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name);
    PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info);

    [[ maybe_unused ]]bool isUpToDate(const QualifiedTableName & table_name) const;

    void buildLocalPipelines(bool only_analyze = false);
    void sendFragments();
    void sendBeginExecutePipelines();

    std::unordered_map<UInt32, FragmentRequest> buildFragmentRequest();

    ContextMutablePtr context;
    String query;

    FragmentPtrs fragments;
    FragmentPtr root_fragment;
    std::unordered_map<FragmentID, FragmentPtr> id_to_fragment;

    HostToFragments host_fragments;
    FragmentToHosts fragment_hosts;

    /// Local pipelines, every fragment will be translated to a pipeline.
    Pipelines pipelines;

    // all destinations
    std::unordered_map<String, IConnectionPool::Entry> host_connection;
    std::unordered_map<UInt32, String> shard_num_to_host;

    String local_host;

    Poco::Logger * log;
};

}
