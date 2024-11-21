#pragma once
#include <functional>
#include <memory>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryCoordination/Pipelines/RemoteExecutorsManager.h>

namespace DB
{

class PullingAsyncPipelineExecutor;

using SetExceptionCallback = std::function<void(std::exception_ptr exception_)>;

/**
 * Executor for query coordination
 */
class QueryCoordinationExecutor
{
public:
    /// For tcp handler
    QueryCoordinationExecutor(
    const std::shared_ptr<PullingAsyncPipelineExecutor> & tcp_root_executor_,
    const NonRootPipelinesExecutorPtr & non_root_executor_,
    const RemoteExecutorsManagerPtr & remote_executors_manager_);

    /// For http handler
    QueryCoordinationExecutor(
        const std::shared_ptr<CompletedPipelineExecutor> & http_root_executor_,
        const NonRootPipelinesExecutorPtr & non_root_executor_,
        const RemoteExecutorsManagerPtr & remote_executors_manager_,
        size_t interactive_timeout_ms_);

    ~QueryCoordinationExecutor();

    /// Get structure of returned block or chunk.
    const Block & getHeader() const;

    /// Methods return false if query is finished.
    /// If milliseconds > 0, returns empty object and `true` after timeout exceeded. Otherwise method is blocking.
    /// You can use any pull method.
    bool pull(Block & block, uint64_t milliseconds = 0);

    void execute();

    /// Stop execution of all processors. It is not necessary, but helps to stop execution before executor is destroyed.
    void cancel();

    /// Stop processors which only read data from source.
    void cancelReading();

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Chunk getTotals() const;
    Chunk getExtremes() const;

    /// Get totals and extremes. Returns empty chunk if doesn't have any.
    Block getTotalsBlock() const;
    Block getExtremesBlock() const;

    /// Get query profile info.
    ProfileInfo & getProfileInfo() const;

    RemoteExecutorsManagerPtr getRemoteExecutorsManager() const { return remote_executors_manager; }

    /// Internal executor data.
    struct Data;

private:
    using CancelFunc = std::function<void()>;

    void cancelWithExceptionHandling(CancelFunc && cancel_func);
    void setException(std::exception_ptr exception_);
    void rethrowExceptionIfHas();

    /// root pipeline
    std::shared_ptr<PullingAsyncPipelineExecutor> tcp_root_executor;
    std::shared_ptr<CompletedPipelineExecutor> http_root_executor;

    /// other pipelines
    std::shared_ptr<NonRootPipelinesExecutor> non_root_executor;

    /// remote executors manager
    std::shared_ptr<RemoteExecutorsManager> remote_executors_manager;


    std::mutex mutex;
    std::exception_ptr exception;
    bool has_exception = false;

    std::atomic_bool has_begun = false;
    std::atomic_bool is_canceled = false;

    Poco::Logger * log;
};

using QueryCoordinationExecutorPtr = std::shared_ptr<QueryCoordinationExecutor>;

}
