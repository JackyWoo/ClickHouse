#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Scheduler/FragmentPipelinesExecutor.h>
#include <Scheduler/Pipelines/FragmentPipelines.h>
#include <Scheduler/RemoteExecutorsManager.h>

#include <algorithm>

namespace DB
{

void FragmentPipelines::assignThreadNum(size_t max_threads)
{
    std::vector<Float64> threads_weight;
    Float64 total_weight = 0;

    for (const auto & query_pipeline : non_root_pipelines)
    {
        Float64 weight = query_pipeline.pipeline.getProcessors().size();
        total_weight += weight;
        threads_weight.emplace_back(weight);
    }

    if (root_pipeline.pipeline.initialized())
    {
        Float64 weight = root_pipeline.pipeline.getProcessors().size();
        total_weight += weight;
        threads_weight.emplace_back(weight);
    }

    for (size_t i = 0; i < threads_weight.size(); ++i)
    {
        if (root_pipeline.pipeline.initialized() && (i == threads_weight.size() - 1))
        {
            size_t num_threads = std::max(size_t(1), static_cast<size_t>((threads_weight[i] / total_weight) * max_threads));
            LOG_DEBUG(&Poco::Logger::get("FragmentPipelines"), "Fragment {} pipeline num_threads {}", root_pipeline.fragment_id, num_threads);
            root_pipeline.pipeline.setNumThreads(num_threads);
        }
        else
        {
            size_t num_threads = std::max(size_t(1), static_cast<size_t>((threads_weight[i] / total_weight) * max_threads));
            LOG_DEBUG(
                &Poco::Logger::get("FragmentPipelines"), "Fragment {} pipeline num_threads {}", non_root_pipelines[i].fragment_id, num_threads);
            non_root_pipelines[i].pipeline.setNumThreads(num_threads);
        }
    }
}

FragmentPipelinesExecutorPtr
FragmentPipelines::createFragmentPipelinesExecutor(QueryPipeline & root_pipeline_, const StorageLimitsList & storage_limits_, size_t interactive_timeout_ms)
{
    chassert(!non_root_pipelines.empty());
    std::vector<UInt32> fragment_ids;
    std::vector<QueryPipeline> pipelines;
    for (auto & query_pipeline : non_root_pipelines)
    {
        pipelines.emplace_back(std::move(query_pipeline.pipeline));
        fragment_ids.emplace_back(query_pipeline.fragment_id);
    }

    auto non_root_executor = std::make_shared<NonRootPipelinesExecutor>(pipelines, fragment_ids);
    auto remote_pipelines_manager = std::make_shared<RemoteExecutorsManager>(storage_limits_);
    /// TODO set nodes

    if (root_pipeline_.pulling())
        return std::make_shared<FragmentPipelinesExecutor>(
            std::make_shared<PullingAsyncPipelineExecutor>(root_pipeline_), non_root_executor, remote_pipelines_manager);

    if (root_pipeline_.completed())
        return std::make_shared<FragmentPipelinesExecutor>(
            std::make_shared<CompletedPipelineExecutor>(root_pipeline_), non_root_executor, remote_pipelines_manager, interactive_timeout_ms);
    
    UNREACHABLE();
}

NonRootPipelinesExecutorPtr FragmentPipelines::createNonRootPipelinesExecutor()
{
    std::vector<UInt32> fragment_ids;
    std::vector<QueryPipeline> pipelines;
    for (auto & query_pipeline : non_root_pipelines)
    {
        pipelines.emplace_back(std::move(query_pipeline.pipeline));
        fragment_ids.emplace_back(query_pipeline.fragment_id);
    }

    return std::make_shared<NonRootPipelinesExecutor>(pipelines, fragment_ids);
}

void FragmentPipelines::addRootPipeline(UInt32 fragment_id, QueryPipeline root_pipeline_)
{
    root_pipeline = {.fragment_id = fragment_id, .pipeline = std::move(root_pipeline_)};
}

void FragmentPipelines::addNonRootPipeline(UInt32 fragment_id, QueryPipeline non_root_pipeline)
{
    non_root_pipelines.emplace_back(fragment_id, std::move(non_root_pipeline));
}

}
