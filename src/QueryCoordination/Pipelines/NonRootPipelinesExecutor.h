#pragma once

#include <QueryPipeline/QueryPipeline.h>
#include <Common/ThreadPool.h>

namespace DB
{

class NonRootPipelinesExecutor
{
public:
    using SetExceptionCallback = std::function<void(std::exception_ptr exception_)>;

    NonRootPipelinesExecutor(std::vector<QueryPipeline> & pipelines_, std::vector<UInt32> & fragment_ids_);
    ~NonRootPipelinesExecutor();

    /// This callback will be called each interactive_timeout_ms.
    /// If returns true, query would be cancelled.
    void setCancelCallback(const std::function<bool()> & cancel_callback_, size_t interactive_timeout_ms_);

    void setExceptionCallback(const SetExceptionCallback & exception_callback_) { exception_callback = exception_callback_; }

    void execute();
    void asyncExecute();

    void waitFinish();
    void cancel();

    struct Data;
    struct Datas;

private:
    std::vector<QueryPipeline> pipelines;
    std::vector<UInt32> fragment_ids;

    size_t interactive_timeout_ms = 0;

    std::unique_ptr<Datas> datas;
    Poco::Event datas_init;

    ThreadFromGlobalPool thread;

    std::function<bool()> cancel_callback;
    SetExceptionCallback exception_callback;

    std::atomic_bool cancelled = false;
    std::atomic_bool cancelled_reading = false;

    Poco::Logger * log;
};

using NonRootPipelinesExecutorPtr = std::shared_ptr<NonRootPipelinesExecutor>;

}
