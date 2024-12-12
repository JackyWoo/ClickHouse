#pragma once

#include <Scheduler/FragmentPipelinesExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

/**
 * As to the query plan for query coordination, we will get a fragment tree.
 *
 * To execute the fragment tree:
 *    1. every fragment will be translated to a pipeline
 *    2. because every server may have multiple fragments, so it will contain multiple pipelines
 *    3. every piepline will have an executor
 *
 * Please note that we will detach the root pipeline to BlockIO::pipeline to reduce code changes. TODO fix it.
 */
class FragmentPipelines
{
public:
    struct PipelineAndFragmentID
    {
        UInt32 fragment_id = 0;
        QueryPipeline pipeline;
    };

    void assignThreadNum(size_t max_threads);
    QueryPipeline detachRootPipeline() { return std::move(root_pipeline.pipeline); }

    /// Create executor for the initial server which will contain a root executor and some non-root executors.
    FragmentPipelinesExecutorPtr
    createFragmentPipelinesExecutor(const String & query_id, QueryPipeline & root_pipeline_, const StorageLimitsList & storage_limits_, size_t interactive_timeout_ms);

    /// Create executor for the non-initial server which will only contain some non-root executors.
    NonRootPipelinesExecutorPtr createNonRootPipelinesExecutor();

    void addRootPipeline(UInt32 fragment_id, QueryPipeline root_pipeline_);
    void addNonRootPipeline(UInt32 fragment_id, QueryPipeline non_root_pipeline);

private:
    /// pipeline for the root fragment, will be detached
    PipelineAndFragmentID root_pipeline;

    /// pipelines which are all completed pipelines for the other fragments in one server
    std::vector<PipelineAndFragmentID> non_root_pipelines;
};

}
