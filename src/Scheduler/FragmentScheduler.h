#pragma once

#include <Interpreters/Cluster.h>
#include <Scheduler/Fragments/Fragment.h>
#include <Scheduler/Fragments/FragmentRequest.h>
#include <Scheduler/Pipelines/FragmentPipelines.h>

namespace DB
{

using Hosts = std::vector<String>;
using FragmentID = UInt32;

using HostToFragments = std::unordered_map<String, FragmentPtrs>;
using FragmentToHosts = std::unordered_map<FragmentID, Hosts>;


class FragmentScheduler
{
public:
    explicit FragmentScheduler(const FragmentPtr & root_fragment_, const ContextMutablePtr & context_, const String & query_);

    void schedule();
    FragmentPipelines && extractPipelines();

    void explainPipelines();

    std::unordered_map<String, IConnectionPool::Entry> getRemoteHostConnection();

private:
    void assignFragmentToHost();

    void assignLeafFragment(const FragmentPtr & fragment);

    [[ maybe_unused ]]PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info, const QualifiedTableName & table_name) const;
    PoolBase<Connection>::Entry getConnection(const Cluster::ShardInfo & shard_info) const;

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
    FragmentPipelines pipelines;

    // all destinations
    std::unordered_map<String, IConnectionPool::Entry> host_to_connection;
    std::unordered_map<UInt32, String> shard_num_to_host;

    String local_host;

    Poco::Logger * log;
};

}
