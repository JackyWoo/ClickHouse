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

    if (status == Status::Finished)
    {
        LOG_DEBUG(log, "We have read {} rows and do not need more data commonly because of limit reached, we will finish the source", num_rows);
        // executor_finished = true;
        return Status::Finished;
    }

    return status;
}

void ExchangeDataSource::onUpdatePorts()
{
    // if (getPort().isFinished())
    // {
    //     LOG_DEBUG(log, "We have read {} rows, and we need to stop reading for the output port is finished, we will finish the source.", num_rows);
    //     executor_finished = true;
    // }
}

std::optional<Chunk> ExchangeDataSource::tryGenerate()
{
    // if (isCancelled() || finished || executor_finished)
    // {
    //     LOG_DEBUG(log, "cancelled or finished, cancelled: {}, finished: {}, executor_finished: {}", isCancelled(), finished, executor_finished);
    //     return {};
    // }

    Block block;
    {
        std::unique_lock lk(mutex);
        cv.wait(lk, [this] { return !block_list.empty() || isCancelled() || finished || receive_data_exception; });

        if (unlikely(receive_data_exception))
            std::rethrow_exception(receive_data_exception);

        if (block_list.empty())
        {
            LOG_DEBUG(log, "block_list is empty");
            return {};
        }
        block = std::move(block_list.front());
        block_list.pop_front();
    }

    if (!block)
    {
        LOG_DEBUG(log, "Receive empty block");
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
    receive(Block());
}

// void ExchangeDataSource::finish(bool need_generate_empty_block)
// {
//     /// We need to cancel the upstream remote executors, because we do not need more data
//     /// There are 2 cases:
//     ///     1. We do not need more data because of limit reached
//     ///     2. The the upstream remote executor sends empty block to me which means it has finished
//     ///
//     /// The exchange maybe local or remote, if it is remote, we need to cancel the remote executor, if it is local, we need to cancel the source.
//
//     /// Only the initial node has remote_executors_manager, for the other nodes, they should waiting for the cancellation from the initial node.
//     if (const auto remote_executors_manager = RemoteExecutorsManagerContainer::getInstance().find(query_id))
//     {
//         /// TODO here we cancel the all non-root remote executors in a node, we'd better cancel the remote executor which is sending data to us
//         remote_executors_manager->cancel(source);
//         // receive(Block());
//     }
//
//     // if (need_generate_empty_block)
//     // {
//     //     LOG_DEBUG(log, "We should stop generating data by provide empty block, now we have read {}", num_rows);
//     //     receive(Block());
//     // }
// }

}
