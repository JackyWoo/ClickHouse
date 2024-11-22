#include <Processors/Executors/PipelineExecutor.h>
#include <QueryCoordination/Pipelines/NonRootPipelinesExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Poco/Event.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct NonRootPipelinesExecutor::Data
{
    Int32 fragment_id;
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_finished{false};
    std::atomic_bool has_exception{false};
    ThreadFromGlobalPool thread;
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
            if (!data->is_finished && data->executor)
                data->executor->cancel(); /// TODO if finished call cancel() will hang?
    }

    void join() const
    {
        for (const auto & data : datas)
            if (!data->is_finished && data->thread.joinable())
                data->thread.join();
    }

    size_t size() const { return datas.size(); }

    void rethrowFirstExceptionIfHas() const
    {
        for (auto & data : datas)
            if (data->has_exception)
                std::rethrow_exception(data->exception);
    }
};

static void threadFunction(NonRootPipelinesExecutor::Data & data, ThreadGroupPtr thread_group, size_t num_threads, Poco::Logger * log)
{
    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
    setThreadName("QCompPipesEx"); /// TODO bytes > 15 can be used test query cancel

    try
    {
        if (thread_group)
            CurrentThread::attachToGroup(thread_group);

        LOG_DEBUG(log, "Fragment {} begin execute", data.fragment_id);

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

void NonRootPipelinesExecutor::asyncExecute()
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
                LOG_DEBUG(log, "is_cancelled_callback try cancel");
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

void NonRootPipelinesExecutor::waitFinish()
{
    datas->finish_event.wait();
}

void NonRootPipelinesExecutor::cancel()
{
    if (cancelled)
        return;

    LOG_DEBUG(log, "canceling");

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

    LOG_DEBUG(log, "cancelled");
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
