#include <Scheduler/Exchange/ExchangeDataSource.h>
#include <Scheduler/RemoteExecutorsManager.h>
#include <Common/logger_useful.h>

namespace DB
{

void ExchangeDataSource::receive(Block block)
{
    {
        std::unique_lock lk(mutex);
        block_list.push_back(std::move(block));
    }
    cv.notify_one();
}

void ExchangeDataSource::receive(std::exception_ptr exception)
{
    {
        std::unique_lock lk(mutex);
        receive_data_exception = exception;
    }
    cv.notify_one();
}

IProcessor::Status ExchangeDataSource::prepare()
{

    const auto status = ISource::prepare();

    if (executor_finished)
        return Status::Finished;

    if (status == Status::Finished)
    {
        need_drain = true;
        return Status::Ready;
    }

    return status;
}

void ExchangeDataSource::work()
{
    if (need_drain)
    {
        LOG_DEBUG(log, "We do not need more data commonly because of limit reached, sending cancellation to node");
        executor_finished = true;
        cancelRemote();
        return;
    }
    ISource::work();
}

void ExchangeDataSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        LOG_DEBUG(log, "Stop reading for the output port is finished.");
        cancelRemote();
    }
}

std::optional<Chunk> ExchangeDataSource::tryGenerate()
{
    if (isCancelled())
    {
        LOG_DEBUG(log, "cancelled or finished, cancelled: {}, finished: {}", isCancelled(), finished);
        return {};
    }

    std::unique_lock lk(mutex);
    cv.wait(lk, [this] { return !block_list.empty() || isCancelled() || finished || receive_data_exception; });

    if (unlikely(receive_data_exception))
        std::rethrow_exception(receive_data_exception);

    if (block_list.empty())
    {
        LOG_DEBUG(log, "does not get a block");
        cancelRemote();
        return {};
    }

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        LOG_DEBUG(log, "Receive empty block");
        cancelRemote();
        return {};
    }

    /// E.g select count() from t1 where col='some_value'
    /// Filter has empty header. ExchangeData has empty header. But there is real data, we construct a fake header.
    size_t rows;
    if (unlikely(!getPort().getHeader()))
    {
        rows = block.getByName("_empty_header_rows").column->get64(0);
        block.clear();
    }
    else
        rows = block.rows();

    LOG_TRACE(log, "Receive {} rows, bucket_num {}", rows, block.info.bucket_num);
    num_rows += rows;

    Chunk chunk(block.getColumns(), rows);

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.getChunkInfos().add(std::move(info));
    }

    return chunk;
}

void ExchangeDataSource::onCancel() noexcept
{
    LOG_DEBUG(log, "on cancel");
    cancelRemote();
}

void ExchangeDataSource::cancelRemote() const
{
    /// we need to cancel the remote executor
    /// TODO here we cancel the all non-root reomte executors, we'd better cancel the remote executor which is sending data to us
    if (const auto remote_executors_manager = RemoteExecutorsManagerContainer::getInstance().find(query_id))
        remote_executors_manager->cancel(source);
}

}
