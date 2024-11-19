#pragma once

#include <Optimizer/PhysicalProperty.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

class ExchangeDataStep final : public ISourceStep
{
public:
    ExchangeDataStep(
        const Header & output_header_,
        size_t max_block_size_,
        const Distribution & distribution_,
        const Sorting & sorting_ = {},
        bool exchange_sink_merge = false,
        bool exchange_source_merge = false)
        : ISourceStep(output_header_)
        , max_block_size(max_block_size_)
        , distribution(distribution_)
        , sorting(sorting_)
        , sink_merge(exchange_sink_merge)
        , source_merge(exchange_source_merge)
    {
        setStepDescription("distributed by " + distribution.toString() + ", sorted by " + sorting.toString());
    }

    String getName() const override { return "ExchangeData"; }
    StepType stepType() const override { return StepType::Exchange; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override;
    void describePipeline(FormatSettings & settings) const override;

    void mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);

    void setPlanNodeID(UInt32 plan_id_) { plan_id = plan_id_; }
    void setSources(const std::vector<String> & sources_) { sources = sources_; }
    void setFragmentID(UInt32 fragment_id_) { fragment_id = fragment_id_; }

    Distribution::Type getDistributionType() const { return distribution.type; }
    const Distribution & getDistribution() const { return distribution; }
    const SortDescription & getSortDescription() const override { return sorting.sort_description; }
    Sorting::Scope getSortScope() const { return sorting.sort_scope; }

    bool isSingleton() const { return distribution.type == Distribution::Singleton; }
    bool sinkMerge() const { return sink_merge; }

private:
    UInt32 fragment_id;
    UInt32 plan_id;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    std::vector<String> sources;

    size_t max_block_size;

    Distribution distribution;

    /// Output sorting of me
    Sorting sorting;

    bool sink_merge;
    bool source_merge;
};

}
