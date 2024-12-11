#pragma once

#include <condition_variable>
#include <list>
#include <mutex>

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

class ExchangeDataSource;
using ExchangeDataSourcePtr = std::shared_ptr<ExchangeDataSource>;

class ExchangeDataSource final : public ISource, public std::enable_shared_from_this<ExchangeDataSource>
{
public:
    ExchangeDataSource(const Header & output_header_, UInt32 fragment_id_, UInt32 plan_id_, const String & source_)
        : ISource(output_header_, false)
        , fragment_id(fragment_id_)
        , plan_id(plan_id_)
        , source(source_)
        , add_aggregation_info(true)
        , log(&Poco::Logger::get("ExchangeDataSource(" + source_ + ")"))
    {
    }

    ~ExchangeDataSource() override = default;

    void receive(Block block);
    void receive(std::exception_ptr exception);

    Status prepare() override;
    String getName() const override { return "ExchangeDataSource"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr /*counter*/) override { }
    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> &) override { }

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    UInt32 getPlanId() const { return plan_id; }

    String getSource() const { return source; }
    Block getHeader() const { return getPort().getHeader(); }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() noexcept override;

private:
    std::condition_variable cv;
    std::mutex mutex;

    BlocksList block_list;

    UInt32 fragment_id;
    UInt32 plan_id;

    String source;

    bool add_aggregation_info;
    size_t num_rows = 0;

    // bool is_async_state = false;

    std::exception_ptr receive_data_exception{};

    Poco::Logger * log;
};

}
