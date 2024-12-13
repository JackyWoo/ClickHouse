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
        finish(false);
        return;
    }
    ISource::work();
}

void ExchangeDataSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        LOG_DEBUG(log, "Stop reading for the output port is finished, sending cancellation to node.");
        finish(false);
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
        finish(false);
        return {};
    }

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        LOG_DEBUG(log, "Receive empty block");
        finish(false);
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
    finish(true);
}

void ExchangeDataSource::finish(bool need_generate_empty_block)
{
    /// We need to cancel the upstream remote executors, because we do not need more data
    /// There are 2 cases:
    ///     1. We do not need more data because of limit reached
    ///     2. The the upstream remote executor sends empty block to me which means it has finished
    ///
    /// The exchange maybe local or remote, if it is remote, we need to cancel the remote executor, if it is local, we need to cancel the source.

    /// Only the initial node has remote_executors_manager, for the other nodes, they should waiting for the cancellation from the initial node.
    if (const auto remote_executors_manager = RemoteExecutorsManagerContainer::getInstance().find(query_id))
    {
        /// TODO here we cancel the all non-root reomte executors, we'd better cancel the remote executor which is sending data to us
        if (!remote_executors_manager->cancel(source))
            /// TODO need a better way to identify whether the exchange is local or remote
            if (need_generate_empty_block)
                receive(Block());
    }
    else
    {
        if (need_generate_empty_block)
            receive(Block());
    }
}

}
