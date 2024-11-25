#include <Core/Settings.h>
#include <Scheduler/Fragments/FragmentBuilder.h>
#include <Scheduler/Pipelines/FragmentPipelinesBuilder.h>
#include <Scheduler/fragmentsToPipelines.h>


namespace DB
{

FragmentPipelines fragmentsToPipelines(
    const FragmentPtrs & all_fragments,
    const std::vector<FragmentRequest> & plan_fragment_requests,
    const String & query_id,
    const Settings & settings,
    const ClusterPtr & cluster,
    bool only_analyze)
{
    DistributedFragmentBuilder builder(all_fragments, plan_fragment_requests);
    const auto distributed_fragments = builder.build();

    FragmentPipelinesBuilder pipelines_builder(query_id, settings, cluster, distributed_fragments);
    return pipelines_builder.build(only_analyze);
}

}
