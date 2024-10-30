#include <Processors/QueryPlan/TopNStep.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = false,
                .preserves_sorting = false,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
}

TopNStep::TopNStep(QueryPlanStepPtr sorting_step_, QueryPlanStepPtr limit_step_)
    : ITransformingStep(sorting_step_->getInputHeaders()[0], limit_step_->getOutputHeader(), getTraits())
    , sorting_step(sorting_step_)
    , limit_step(limit_step_)
{
    setStepDescription("One stage TopN");
}

void TopNStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    sorting->transformPipeline(pipeline, {});
    limit->transformPipeline(pipeline, {});
}

void TopNStep::updateOutputHeader()
{
    output_header = limit_step->getOutputHeader();
}


std::shared_ptr<TopNStep> TopNStep::makePreliminary(bool exact_rows_before_limit)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    auto pre_sort = sorting->clone();
    auto pre_limit = std::make_shared<LimitStep>(
        pre_sort->getOutputHeader(),
        limit->getLimitForSorting(),
        0,
        exact_rows_before_limit);

    auto pre_topn = std::make_shared<TopNStep>(pre_sort, pre_limit);
    pre_topn->phase = TopNStep::Phase::Preliminary;
    pre_topn->setStepDescription("Preliminary");
    return pre_topn;
}

std::shared_ptr<TopNStep> TopNStep::makeFinal(const Header & input_header, size_t max_block_size, bool exact_rows_before_limit)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    auto merging_sorted = std::make_shared<SortingStep>(
        input_header, sorting->getSortDescription(), max_block_size, sorting->getLimit(), exact_rows_before_limit);

    auto final_limit = std::make_shared<LimitStep>(
        merging_sorted->getOutputHeader(),
        limit->getLimit(),
        limit->getOffset(),
        exact_rows_before_limit,
        limit->withTies(),
        limit->getSortDescription());

    auto final_topn = std::make_shared<TopNStep>(merging_sorted, final_limit);
    final_topn->phase = TopNStep::Phase::Final;
    final_topn->setStepDescription("Final");
    return final_topn;
}

}
