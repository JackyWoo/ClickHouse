#include <Optimizer/DeriveRequiredChildProps.h>

#include <Core/Settings.h>
#include <Optimizer/Group.h>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 group_by_two_level_threshold;
extern const SettingsUInt64 group_by_two_level_threshold_bytes;
}


AlternativeChildProperties DeriveRequiredChildProps::visit(const QueryPlanStepPtr & step)
{
    if (group_node->hasRequiredChildProperties())
        return group_node->getAlternativeRequiredChildProperties();
    return Base::visit(step);
}

AlternativeChildProperties DeriveRequiredChildProps::visitDefault(IQueryPlanStep & step)
{
    if (step.stepType() == StepType::Scan)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Engine {} not implemented with CBO optimizer, please disable it by setting allow_experimental_query_coordination to 0",
            step.getName());

    ChildProperties required_child_props;
    for (size_t i = 0; i < group_node->childSize(); ++i)
    {
        required_child_props.push_back({.distribution = {.type = Distribution::Singleton}});
    }

    return {required_child_props};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(ReadFromMergeTree & /*step*/)
{
    AlternativeChildProperties res;
    res.emplace_back(); /// ReadFromMergeTree has no child
    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(AggregatingStep & step)
{
    AlternativeChildProperties res;

    /// TODO add required sorting properties

    if (!step.isPreliminary()) /// one stage aggregating
    {
        ChildProperties required_singleton_prop;
        required_singleton_prop.push_back({.distribution = {.type = Distribution::Singleton}});
        res.emplace_back(required_singleton_prop);

        ChildProperties required_hashed_prop; /// can not be chosen right now
        PhysicalProperty hashed_prop;
        hashed_prop.distribution.type = Distribution::Hashed;
        hashed_prop.distribution.keys = step.getParams().keys;
        required_hashed_prop.push_back(hashed_prop);
        res.emplace_back(required_hashed_prop);
    }
    else /// the first stage of two stage aggreagting
    {
        ChildProperties required_child_prop;
        required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
        res.emplace_back(required_child_prop);
    }
    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(MergingAggregatedStep & step)
{
    /// The second aggregating stage

    AlternativeChildProperties res;
    ChildProperties required_child_prop;

    /// Explicit require property, not enumerations
    auto statistics = group_node->getGroup().getStatistics();

    bool estimate_two_level_agg = false;
    auto group_by_two_level_threshold = context->getSettingsRef()[Setting::group_by_two_level_threshold];
    auto group_by_two_level_threshold_bytes = context->getSettingsRef()[Setting::group_by_two_level_threshold_bytes];
    if ((group_by_two_level_threshold && statistics.getOutputRowSize() >= group_by_two_level_threshold)
        || (group_by_two_level_threshold_bytes && statistics.getDataSize() >= group_by_two_level_threshold_bytes))
        estimate_two_level_agg = true;

    /// If cube, rollup or totals step.isFinal() == false
    if (step.getParams().keys.empty() || !step.isFinal() || !estimate_two_level_agg)
    {
        required_child_prop.push_back({.distribution = {.type = Distribution::Singleton}});
    }
    else
    {
        PhysicalProperty hashed_prop;
        hashed_prop.distribution.type = Distribution::Hashed;
        hashed_prop.distribution.keys = step.getParams().keys;
        hashed_prop.distribution.distributed_by_bucket_num = true;
        required_child_prop.push_back(hashed_prop);
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(ExpressionStep & /*step*/)
{
    PhysicalProperty required_child_prop;
    required_child_prop.distribution = {.type = Distribution::Any};
    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(FilterStep & /*step*/)
{
    PhysicalProperty required_child_prop;
    required_child_prop.distribution = {.type = Distribution::Any};
    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(TopNStep & step)
{
    PhysicalProperty required_child_prop;
    if (step.sortType() == SortingStep::Type::FinishSorting)
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getPrefixDescription();
    }
    else if (step.sortType() == SortingStep::Type::MergingSorted)
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getSortDescription();
    }

    if (step.getPhase() == TopNStep::Phase::Preliminary)
        required_child_prop.distribution = {.type = Distribution::Any};
    else
        required_child_prop.distribution = {.type = Distribution::Singleton};
    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(SortingStep & step)
{
    PhysicalProperty required_child_prop;
    if (step.getType() == SortingStep::Type::FinishSorting)
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getPrefixDescription();
    }
    else if (step.getType() == SortingStep::Type::MergingSorted)
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getSortDescription();
    }
    if (step.getPhase() == SortingStep::Phase::Preliminary)
        required_child_prop.distribution = {.type = Distribution::Any};
    else
        required_child_prop.distribution = {.type = Distribution::Singleton};
    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(DistinctStep & step)
{
    PhysicalProperty required_child_prop;
    const auto & distinct_sort_desc = step.getSortDescription();
    /// Preliminary distinct step may preserve sorting
    if (step.isPreliminary() && step.getDataStreamTraits().preserves_sorting && !distinct_sort_desc.empty())
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = distinct_sort_desc;
    }

    if (step.isPreliminary())
        required_child_prop.distribution = {.type = Distribution::Any};
    else
        required_child_prop.distribution = {.type = Distribution::Singleton};

    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(LimitStep & step)
{
    AlternativeChildProperties res;
    ChildProperties required_child_prop;

    if (step.getPhase() == LimitStep::Phase::Preliminary)
        required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
    else
        required_child_prop.push_back({.distribution = {.type = Distribution::Singleton}});

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(JoinStep & step)
{
    AlternativeChildProperties res;

    /// broadcast join
    ChildProperties broadcast_join_properties;
    broadcast_join_properties.push_back({.distribution = {.type = Distribution::Any}});
    broadcast_join_properties.push_back({.distribution = {.type = Distribution::Replicated}});
    res.emplace_back(broadcast_join_properties);

    /// shuffle join
    JoinPtr join = step.getJoin();

    if (join->pipelineType() != JoinPipelineType::FillRightFirst)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Not support join pipeline type, please specify the join algorithm as hash or parallel_hash or grace_hash");

    const TableJoin & table_join = join->getTableJoin();
    if (table_join.getClauses().size() == 1
        && table_join.strictness() != JoinStrictness::Asof) /// broadcast join. Asof support != condition
    {
        auto join_clause = table_join.getOnlyClause(); /// must be equals condition
        ChildProperties shaffle_join_prop;
        shaffle_join_prop.push_back({.distribution = {.type = Distribution::Hashed, .keys = join_clause.key_names_left}});
        shaffle_join_prop.push_back({.distribution = {.type = Distribution::Hashed, .keys = join_clause.key_names_right}});
        res.emplace_back(shaffle_join_prop);
    }

    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(ExchangeDataStep & /*step*/)
{
    ChildProperties required_child_prop;
    required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
    return {required_child_prop};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(CreatingSetStep & /*step*/)
{
    ChildProperties required_child_prop;
    required_child_prop.push_back({.distribution = {.type = Distribution::Replicated}});
    return {required_child_prop};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(CreatingSetsStep & step)
{
    bool all_column_aligned = true;
    const auto & header = step.getInputHeaders().front();
    const auto & step_sort_desc = step.getSortDescription(); /// TODO update sort description in applyOrder.cpp
    for (const auto & sort_column : step_sort_desc)
    {
        if (!header.has(sort_column.column_name))
        {
            all_column_aligned = false;
            break;
        }
    }

    Sorting required_sort_prop;
    if (all_column_aligned && !step_sort_desc.empty())
    {
        required_sort_prop.sort_description = step_sort_desc;
        required_sort_prop.sort_scope = Sorting::Scope::Stream; /// TODO add sort scope to QueryPlanStep
    }

    ChildProperties required_child_prop;

    /// Ensure that CreatingSetsStep and the left table scan are assigned to the same fragment.
    required_child_prop.push_back({.distribution = {.type = Distribution::Any}, .sorting = required_sort_prop});
    for (size_t i = 1; i < group_node->childSize(); ++i)
        required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
    return {required_child_prop};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(UnionStep & /*step*/)
{
    ChildProperties required_child_prop;
    required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
    required_child_prop.push_back({.distribution = {.type = Distribution::Any}});
    return {required_child_prop};
}

}
