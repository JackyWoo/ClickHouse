#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryCoordination/Pipelines/NonRootPipelinesExecutor.h>
#include <QueryCoordination/Pipelines/RemoteExecutorsManager.h>
#include <QueryCoordination/QueryCoordinationExecutor.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct QueryCoordinationExecutor::Data
{
    PipelineExecutorPtr executor;
    LazyOutputFormat * lazy_format = nullptr;
    std::atomic_bool is_finished = false;
    ThreadFromGlobalPool thread;
    Poco::Event finish_event;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }
};

QueryCoordinationExecutor::QueryCoordinationExecutor(
    const std::shared_ptr<PullingAsyncPipelineExecutor> & tcp_root_executor_,
    const NonRootPipelinesExecutorPtr & non_root_executor_,
    const RemoteExecutorsManagerPtr & remote_executors_manager_)
    : tcp_root_executor(tcp_root_executor_)
    , non_root_executor(non_root_executor_)
    , remote_executors_manager(remote_executors_manager_)
    , log(&Poco::Logger::get("QueryCoordinationExecutor"))
{
}

QueryCoordinationExecutor::QueryCoordinationExecutor(
    const std::shared_ptr<CompletedPipelineExecutor> & http_root_executor_,
    const NonRootPipelinesExecutorPtr & non_root_executor_,
    const RemoteExecutorsManagerPtr & remote_executors_manager_,
    size_t interactive_timeout_ms_)
    : http_root_executor(http_root_executor_)
    , non_root_executor(non_root_executor_)
    , remote_executors_manager(remote_executors_manager_)
    , log(&Poco::Logger::get("QueryCoordinationExecutor"))
{

    auto cancel_callback = [this]() { return is_canceled.load(); };
    if (http_root_executor)
        http_root_executor->setCancelCallback(cancel_callback, interactive_timeout_ms_);
}

QueryCoordinationExecutor::~QueryCoordinationExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("QueryCoordinationExecutor");
    }
}

const Block & QueryCoordinationExecutor::getHeader() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getHeader();
}


bool QueryCoordinationExecutor::pull(Block & block, uint64_t milliseconds)
{
    if (!has_begun)
    {
        auto exception_callback = [this](std::exception_ptr exception_) { setException(exception_); };

        if (non_root_executor)
        {
            non_root_executor->setExceptionCallback(exception_callback);
            non_root_executor->asyncExecute();
        }

        if (remote_executors_manager)
        {
            remote_executors_manager->setExceptionCallback(exception_callback);
            remote_executors_manager->asyncReceiveReports();
        }
        has_begun = true;
    }

    rethrowExceptionIfHas();

    bool is_execution_finished = !tcp_root_executor->pull(block, milliseconds);

    if (is_execution_finished)
    {
        if (non_root_executor)
            non_root_executor->waitFinish();

        if (remote_executors_manager)
            remote_executors_manager->waitFinish();
    }

    return !is_execution_finished;
}

void QueryCoordinationExecutor::execute()
{
    has_begun = true;
    auto exception_callback = [this](std::exception_ptr exception_) { setException(exception_); };

    if (non_root_executor)
    {
        non_root_executor->setExceptionCallback(exception_callback);
        non_root_executor->asyncExecute();
    }

    if (remote_executors_manager)
    {
        remote_executors_manager->setExceptionCallback(exception_callback);
        remote_executors_manager->asyncReceiveReports();
    }

    rethrowExceptionIfHas();
    http_root_executor->execute();
}

void QueryCoordinationExecutor::cancel()
{
    LOG_DEBUG(log, "cancel");

    /// Cancel execution if it wasn't finished.
    cancelWithExceptionHandling(
        [&]()
        {
            if (tcp_root_executor)
                tcp_root_executor->cancel();
        });

    /// send cancel signal to completed_root_executor
    cancelWithExceptionHandling(
        [&]()
        {
            is_canceled = true;
        });

    cancelWithExceptionHandling(
        [&]()
        {
            if (non_root_executor)
                non_root_executor->cancel();
        });

    cancelWithExceptionHandling(
        [&]()
        {
            if (remote_executors_manager)
                remote_executors_manager->cancel();
        });

    LOG_DEBUG(log, "cancelled");

    /// Rethrow exception to not swallow it in destructor.
    rethrowExceptionIfHas();
}

void QueryCoordinationExecutor::cancelReading()
{
    //    if (!data)
    //        return;
    //
    //    /// Stop reading from source if pipeline wasn't finished.
    //    cancelWithExceptionHandling([&]()
    //    {
    //        if (!data->is_finished && data->executor)
    //            data->executor->cancelReading();
    //    });
}

void QueryCoordinationExecutor::cancelWithExceptionHandling(CancelFunc && cancel_func)
{
    try
    {
        cancel_func();
    }
    catch (...)
    {
        /// Store exception only of during query execution there was no
        /// exception, since only one exception can be re-thrown.
        setException(std::current_exception());
    }
}

Chunk QueryCoordinationExecutor::getTotals() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getTotals();
}

Chunk QueryCoordinationExecutor::getExtremes() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getExtremes();
}

Block QueryCoordinationExecutor::getTotalsBlock() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getTotalsBlock();
}

Block QueryCoordinationExecutor::getExtremesBlock() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getExtremesBlock();
}

ProfileInfo & QueryCoordinationExecutor::getProfileInfo() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getProfileInfo();
}

void QueryCoordinationExecutor::setException(std::exception_ptr exception_)
{
    std::lock_guard lock(mutex);
    if (!has_exception)
    {
        has_exception = true;
        exception = exception_;
    }
}

void QueryCoordinationExecutor::rethrowExceptionIfHas()
{
    std::lock_guard lock(mutex);
    if (has_exception)
    {
        has_exception = false;
        std::rethrow_exception(exception);
    }
}

}
