#pragma once
#include <functional>
#include <memory>
#include <Common/logger_useful.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

namespace DB
{

class QueryPipeline;
class Block;
class Chunk;
class LazyOutputFormat;
struct ProfileInfo;
class RemotePipelinesManager;
class PullingAsyncPipelineExecutor;

using setExceptionCallback = std::function<void(std::exception_ptr exception_)>;

/// Includes 3 parts of logic
/// main PullingAsyncPipelineExecutor::pull
/// QueryStatusManager receive exception and progress, exception to PullingAsyncPipelineExecutor::Data
/// local CompletedPipelinesExecutor exception report to PullingAsyncPipelineExecutor::Data
class QueryCoordinationExecutor
{
public:
    /// For tcp handler
    QueryCoordinationExecutor(
        std::shared_ptr<PullingAsyncPipelineExecutor> pulling_root_executor_,
        std::shared_ptr<CompletedPipelinesExecutor> sources_pipelines_executor_,
        std::shared_ptr<RemotePipelinesManager> remote_pipelines_manager_);

    /// For http handler
    QueryCoordinationExecutor(
        std::shared_ptr<CompletedPipelineExecutor> completed_root_executor_,
        std::shared_ptr<CompletedPipelinesExecutor> sources_pipelines_executor_,
        std::shared_ptr<RemotePipelinesManager> remote_pipelines_manager_,
        size_t interactive_timeout_ms_);

    ~QueryCoordinationExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    /// Methods return false if query is finished.
    /// If milliseconds > 0, returns empty object and `true` after timeout exceeded. Otherwise method is blocking.
    /// You can use any pull method.
    bool pull(Block & block, uint64_t milliseconds = 0);

    /// Methods return false if query is finished.
    /// If milliseconds > 0, returns empty object and `true` after timeout exceeded. Otherwise method is blocking.
    /// You can use any pull method.
    void execute();

    /// Stop execution of all processors. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    /// Stop processors which only read data from source.
    void cancelReading();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Chunk getTotals();
    Chunk getExtremes();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Block getTotalsBlock();
    Block getExtremesBlock();

    /// Get query profile info.
    ProfileInfo & getProfileInfo();

    std::shared_ptr<RemotePipelinesManager> getRemotePipelinesManager() { return remote_pipelines_manager; }

    /// Internal executor data.
    struct Data;

private:
    using CancelFunc = std::function<void()>;

    void cancelWithExceptionHandling(CancelFunc && cancel_func);
    void setException(std::exception_ptr exception_);
    void rethrowExceptionIfHas();

    Poco::Logger * log;

    /// root pipeline
    std::shared_ptr<PullingAsyncPipelineExecutor> pulling_root_executor;
    std::shared_ptr<CompletedPipelineExecutor> completed_root_executor;

    /// other pipelines executor
    std::shared_ptr<CompletedPipelinesExecutor> sources_pipelines_executor;

    /// remote pipelines manager
    std::shared_ptr<RemotePipelinesManager> remote_pipelines_manager;


    std::mutex mutex;
    std::exception_ptr exception;
    bool has_exception = false;

    std::atomic_bool has_begun = false;
    std::atomic_bool is_canceled = false;
};

}
