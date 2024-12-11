#include <Scheduler/Exchange/ExchangeDataSource.h>
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
    return ISource::prepare();
}

void ExchangeDataSource::onUpdatePorts()
{
    /// We do not need more data commonly because of limit and we should cancel the sources.
    if (getPort().isFinished())
    {
        /// TODO cancel the source
    }
}

std::optional<Chunk> ExchangeDataSource::tryGenerate()
{
    if (isCancelled())
        return {};

    std::unique_lock lk(mutex);
    cv.wait(lk, [this] { return !block_list.empty() || finished || isCancelled() || receive_data_exception; });

    if (unlikely(receive_data_exception))
        std::rethrow_exception(receive_data_exception);

    if (block_list.empty())
        return {};

    Block block = std::move(block_list.front());
    block_list.pop_front();

    if (!block)
    {
        LOG_DEBUG(
            log,
            "Fragment {} exchange id {} receive empty block from {}",
            fragment_id,
            plan_id,
            source);
        /// TODO cancel the source
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

    LOG_TRACE(
        log,
        "Fragment {} exchange id {} receive {} rows from {} bucket_num {}",
        fragment_id,
        plan_id,
        rows,
        source,
        block.info.bucket_num);
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
    LOG_DEBUG(log, "Fragment {} exchange id {} on cancel", fragment_id, plan_id);
    receive(Block());

    // TODO cancel the source
    query_id;
}

}
