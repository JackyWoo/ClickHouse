#include <Scheduler/FragmentPipelinesExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Scheduler/RemoteExecutorsManager.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct FragmentPipelinesExecutor::Data
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

FragmentPipelinesExecutor::FragmentPipelinesExecutor(
    const std::shared_ptr<PullingAsyncPipelineExecutor> & tcp_root_executor_,
    const NonRootPipelinesExecutorPtr & non_root_executor_,
    const RemoteExecutorsManagerPtr & remote_executors_manager_)
    : tcp_root_executor(tcp_root_executor_)
    , non_root_executor(non_root_executor_)
    , remote_executors_manager(remote_executors_manager_)
    , log(&Poco::Logger::get("FragmentPipelinesExecutor"))
{
}

FragmentPipelinesExecutor::FragmentPipelinesExecutor(
    const std::shared_ptr<CompletedPipelineExecutor> & http_root_executor_,
    const NonRootPipelinesExecutorPtr & non_root_executor_,
    const RemoteExecutorsManagerPtr & remote_executors_manager_,
    size_t interactive_timeout_ms_)
    : http_root_executor(http_root_executor_)
    , non_root_executor(non_root_executor_)
    , remote_executors_manager(remote_executors_manager_)
    , log(&Poco::Logger::get("FragmentPipelinesExecutor"))
{

    auto cancel_callback = [this]() { return is_canceled.load(); };
    if (http_root_executor)
        http_root_executor->setCancelCallback(cancel_callback, interactive_timeout_ms_);
}

FragmentPipelinesExecutor::~FragmentPipelinesExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

const Block & FragmentPipelinesExecutor::getHeader() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getHeader();
}


bool FragmentPipelinesExecutor::pull(Block & block, uint64_t milliseconds)
{
    if (!has_begun)
    {
        auto exception_callback = [this](std::exception_ptr exception_) { setException(exception_); };

        if (non_root_executor)
        {
            non_root_executor->setExceptionCallback(exception_callback);
            non_root_executor->executeAsync();
        }

        if (remote_executors_manager)
        {
            remote_executors_manager->setExceptionCallback(exception_callback);
            remote_executors_manager->receiveReportsAsync();
        }
        has_begun = true;
    }
    rethrowExceptionIfHas();

    bool is_execution_finished = !tcp_root_executor->pull(block, milliseconds);

    if (is_execution_finished)
    {
        Stopwatch watch;
        if (non_root_executor)
        {
            if (!non_root_executor->isFinished())
            {
                LOG_DEBUG(log, "non root executor is not finished, canceling");
                non_root_executor->cancel();
                non_root_executor->waitFinish();
            }
        }

        if (remote_executors_manager)
        {
            if (!remote_executors_manager->isFinished())
            {
                LOG_DEBUG(log, "remote executors manager is not finished, canceling");
                remote_executors_manager->cancel();
                remote_executors_manager->waitFinish();
            }
        }
        LOG_DEBUG(log, "Finalizing the query, elapsed {}ms", watch.elapsed() / 1000);
    }

    return !is_execution_finished;
}

void FragmentPipelinesExecutor::execute()
{
    has_begun = true;
    auto exception_callback = [this](std::exception_ptr exception_) { setException(exception_); };

    if (non_root_executor)
    {
        non_root_executor->setExceptionCallback(exception_callback);
        non_root_executor->executeAsync();
    }

    if (remote_executors_manager)
    {
        remote_executors_manager->setExceptionCallback(exception_callback);
        remote_executors_manager->receiveReportsAsync();
    }

    rethrowExceptionIfHas();
    http_root_executor->execute();
}

void FragmentPipelinesExecutor::cancel()
{
    LOG_DEBUG(log, "canceling fragment pipelines executor");

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

void FragmentPipelinesExecutor::cancelReading()
{
    /// Stop reading from source if pipeline wasn't finished.
    LOG_DEBUG(log, "cancelReading"); /// TODO implement this
    cancel();
}

void FragmentPipelinesExecutor::cancelWithExceptionHandling(CancelFunc && cancel_func)
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

Chunk FragmentPipelinesExecutor::getTotals() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getTotals();
}

Chunk FragmentPipelinesExecutor::getExtremes() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getExtremes();
}

Block FragmentPipelinesExecutor::getTotalsBlock() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getTotalsBlock();
}

Block FragmentPipelinesExecutor::getExtremesBlock() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getExtremesBlock();
}

ProfileInfo & FragmentPipelinesExecutor::getProfileInfo() const
{
    chassert(tcp_root_executor != nullptr);
    return tcp_root_executor->getProfileInfo();
}

void FragmentPipelinesExecutor::setException(std::exception_ptr exception_)
{
    std::lock_guard lock(mutex);
    if (!has_exception)
    {
        has_exception = true;
        exception = exception_;
    }
}

void FragmentPipelinesExecutor::rethrowExceptionIfHas()
{
    std::lock_guard lock(mutex);
    if (has_exception)
    {
        has_exception = false;
        std::rethrow_exception(exception);
    }
}



struct NonRootPipelinesExecutor::Data
{
    Int32 fragment_id;
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_finished{false};
    std::atomic_bool has_exception{false};
    ThreadFromGlobalPool thread;
    /// notify waitFinish
    std::function<void()> finish_callback;

    Data() = default;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }
};


struct NonRootPipelinesExecutor::Datas
{
    std::vector<std::shared_ptr<Data>> datas;

    Poco::Event finish_event{false};
    std::mutex mutex;

    void finishCallBack()
    {
        std::lock_guard lock(mutex);
        if (isFinished())
            finish_event.set();
    }

    bool isFinished() const
    {
        for (const auto & data : datas)
            if (!data->is_finished)
                return false;
        return true;
    }

    void cancel() const
    {
        for (const auto & data : datas)
        {
            if (!data->is_finished && data->executor)
            {
                try
                {
                    data->executor->cancel();
                }
                catch (...)
                {
                    if (!data->has_exception)
                    {
                        data->exception = std::current_exception();
                        data->has_exception = true;
                    }
                }
                data->is_finished = true;
            }
        }
    }

    void join() const
    {
        for (const auto & data : datas)
        {
            if (data->thread.joinable())
                data->thread.join();
        }
    }

    size_t size() const { return datas.size(); }

    void rethrowFirstExceptionIfHas() const
    {
        for (const auto & data : datas)
        {
            if (data->has_exception)
                std::rethrow_exception(data->exception);
        }
    }
};

static void threadFunction(NonRootPipelinesExecutor::Data & data, ThreadGroupPtr thread_group, size_t num_threads, Poco::Logger * log)
{
    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
    setThreadName("QCompPipesEx"); /// TODO bytes > 15 can be used to test query cancel

    try
    {
        if (thread_group)
            CurrentThread::attachToGroup(thread_group);

        LOG_DEBUG(log, "Fragment {} begin to execute", data.fragment_id);
        data.executor->execute(num_threads, true);
    }
    catch (...)
    {
        data.exception = std::current_exception();
        data.has_exception = true;
    }

    data.is_finished = true;
    data.finish_callback();

    LOG_DEBUG(log, "Fragment {} finished", data.fragment_id);
}

NonRootPipelinesExecutor::NonRootPipelinesExecutor(std::vector<QueryPipeline> & pipelines_, std::vector<UInt32> & fragment_ids_)
    : pipelines(std::move(pipelines_)), fragment_ids(std::move(fragment_ids_)), log(&Poco::Logger::get("NonRootPipelinesExecutor"))
{
    for (auto & pipeline : pipelines)
        if (!pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for NonRootPipelinesExecutor must be completed");
}

void NonRootPipelinesExecutor::setCancelCallback(const std::function<bool()> & cancel_callback_, size_t interactive_timeout_ms_)
{
    cancel_callback = cancel_callback_;
    interactive_timeout_ms = interactive_timeout_ms_;
}

void NonRootPipelinesExecutor::executeAsync()
{
    auto func = [this, thread_group = CurrentThread::getGroup()]
    {
        SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

        setThreadName("ComPipAsyncExec");

        if (thread_group)
            CurrentThread::attachToGroup(thread_group);

        try
        {
            execute();
        }
        catch (...)
        {
            exception_callback(std::current_exception());
        }
    };

    thread = ThreadFromGlobalPool(std::move(func));
    datas_init.wait(); /// avoid data thread join before data thread init
}

void NonRootPipelinesExecutor::execute()
{
    datas = std::make_unique<Datas>();

    for (size_t i = 0; i < pipelines.size(); ++i)
    {
        std::lock_guard lock(datas->mutex);
        auto data = std::make_shared<Data>();
        data->finish_callback = [&]() { datas->finishCallBack(); };
        data->fragment_id = fragment_ids[i];
        datas->datas.emplace_back(data);
    }

    for (size_t i = 0; i < datas->size(); ++i)
    {
        auto data = datas->datas[i];
        data->executor = std::make_shared<PipelineExecutor>(pipelines[i].processors, pipelines[i].process_list_element);
        data->executor->setReadProgressCallback(pipelines[i].getReadProgressCallback());

        /// Avoid passing this to lambda, copy ptr to data instead.
        /// Destructor of unique_ptr copy raw ptr into local variable first, only then calls object destructor.
        auto func
            = [data_ptr = data.get(), num_threads = pipelines[i].getNumThreads(), thread_group = CurrentThread::getGroup(), log_ = log]
        { threadFunction(*data_ptr, thread_group, num_threads, log_); };

        data->thread = ThreadFromGlobalPool(std::move(func));
    }

    datas_init.set();

    if (interactive_timeout_ms)
    {
        while (!datas->isFinished())
        {
            if (datas->finish_event.tryWait(interactive_timeout_ms))
                break;

            if (cancel_callback())
            {
                LOG_DEBUG(log, "canceling");
                cancel();
            }
        }
    }
    else
    {
        datas->finish_event.wait();
    }

    datas->rethrowFirstExceptionIfHas();
}

void NonRootPipelinesExecutor::waitFinish() const
{
    datas->finish_event.wait();
}

bool NonRootPipelinesExecutor::isFinished() const
{
    return datas && datas->isFinished();
}

void NonRootPipelinesExecutor::cancel()
{
    if (cancelled)
        return;

    LOG_DEBUG(log, "canceling non root pipelines executor");

    cancelled = true;

    if (datas && !datas->isFinished())
    {
        datas->cancel();
        /// Join thread here to wait for possible exception.
        datas->join();
        datas->finish_event.set();
    }

    datas_init.set();
    if (thread.joinable())
        thread.join();

    LOG_DEBUG(log, "cancelled non root pipelines executor");
}

NonRootPipelinesExecutor::~NonRootPipelinesExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

}
