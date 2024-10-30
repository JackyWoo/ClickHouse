#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>


namespace DB
{

class TopNStep final : public ITransformingStep
{
public:
    enum Phase
    {
        Unknown,
        Preliminary,
        Final,
    };

    TopNStep(QueryPlanStepPtr sorting_step_, QueryPlanStepPtr limit_step_);

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void updateOutputHeader() override;

    String getName() const override { return "TopN"; }

    Phase getPhase() const { return phase; }
    void setPhase(Phase phase_) { phase = phase_; }

    bool isPreliminary() const { return phase == Preliminary; }
    bool hasPartitions() const
    {
        auto * sorting_step_casted = typeid_cast<SortingStep *>(sorting_step.get());
        return sorting_step_casted->hasPartitions();
    }

    size_t getLimitForSorting() const
    {
        auto * limit = typeid_cast<LimitStep *>(limit_step.get());
        assert(limit != nullptr);
        if (limit->getLimit() > std::numeric_limits<UInt64>::max() - limit->getOffset())
            return 0;

        return limit->getLimit() + limit->getOffset();
    }

    std::shared_ptr<TopNStep> makePreliminary(bool exact_rows_before_limit);
    std::shared_ptr<TopNStep> makeFinal(const Header & input_header, size_t max_block_size, bool exact_rows_before_limit);

    StepType stepType() const override
    {
        return TopN;
    }

    SortingStep::Type sortType() const
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getType();
    }

    const SortDescription & getPrefixDescription() const
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getPrefixDescription();
    }

    const SortDescription & getSortDescription() const override
    {
        auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
        assert(sorting != nullptr);
        return sorting->getSortDescription();
    }

private:
    QueryPlanStepPtr sorting_step;
    QueryPlanStepPtr limit_step;

    Phase phase = Unknown;
};

}
