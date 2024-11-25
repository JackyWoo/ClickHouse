#pragma once

#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <Scheduler/Fragments/Fragment.h>
#include <Scheduler/Pipelines/FragmentPipelines.h>

namespace DB
{

class FragmentPipelinesBuilder
{
public:
    FragmentPipelinesBuilder(
        const String & query_id_, const Settings & settings_, const ClusterPtr & cluster_, const DistributedFragments & distributed_fragments_)
        : log(&Poco::Logger::get("FragmentPipelinesBuilder"))
        , query_id(query_id_)
        , settings(settings_)
        , cluster(cluster_)
        , distributed_fragments(distributed_fragments_)
    {
    }

    FragmentPipelines build(bool only_analyze = false);

private:
    Poco::Logger * log;

    String query_id;
    const Settings & settings;
    ClusterPtr cluster;
    DistributedFragments distributed_fragments;
};

}
