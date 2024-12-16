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
            "CBO optimizer does not support {}, please disable it by setting allow_experimental_query_coordination to 0",
            step.getName());

    ChildProperties required_child_props;
    for (size_t i = 0; i < group_node->childSize(); ++i)
        required_child_props.push_back({.distribution = {.type = Distribution::Singleton}});

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

        ChildProperties required_hashed_prop;
        PhysicalProperty hashed_prop;
        hashed_prop.distribution.type = Distribution::Hashed;
        hashed_prop.distribution.keys = step.getParams().keys;
        required_hashed_prop.push_back(hashed_prop);
        res.emplace_back(required_hashed_prop);
    }
    else /// the first stage of two stage aggreagting
    {
        ChildProperties required_child_prop;
        required_child_prop.push_back({.distribution = {.type = Distribution::Distributed}});
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
    /// Deriving soring
    if (step.getPhase() == TopNStep::Phase::Preliminary || step.getPhase() == TopNStep::Phase::Unknown)
    {
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
        else
        {
            /// no requirement for full sorting
        }
    }
    else
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getSortDescription();
    }

    /// Deriving distribution
    if (step.getPhase() == TopNStep::Phase::Preliminary)
        required_child_prop.distribution = {.type = Distribution::Distributed};
    else
        required_child_prop.distribution = {.type = Distribution::Singleton};
    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(SortingStep & step)
{
    PhysicalProperty required_child_prop;
    /// Deriving soring
    if (step.getPhase() == SortingStep::Phase::Preliminary || step.getPhase() == SortingStep::Phase::Unknown)
    {
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
        else
        {
            /// no requirement for full sorting
        }
    }
    else
    {
        required_child_prop.sorting.sort_scope = Sorting::Scope::Stream;
        required_child_prop.sorting.sort_description = step.getSortDescription();
    }

    /// Deriving distribution
    if (step.getPhase() == SortingStep::Phase::Preliminary)
        required_child_prop.distribution = {.type = Distribution::Distributed};
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
        required_child_prop.distribution = {.type = Distribution::Distributed};
    else
        required_child_prop.distribution = {.type = Distribution::Singleton};

    return {{required_child_prop}};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(LimitStep & step)
{
    AlternativeChildProperties res;
    ChildProperties required_child_prop;

    if (step.getPhase() == LimitStep::Phase::Preliminary)
        required_child_prop.push_back({.distribution = {.type = Distribution::Distributed}});
    else
        required_child_prop.push_back({.distribution = {.type = Distribution::Singleton}});

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildProperties DeriveRequiredChildProps::visit(JoinStep & step)
{
    AlternativeChildProperties res;

    // /// singleton join
    // ChildProperties singleton_join_properties;
    // singleton_join_properties.push_back({.distribution = {.type = Distribution::Singleton}});
    // singleton_join_properties.push_back({.distribution = {.type = Distribution::Singleton}});
    // res.emplace_back(singleton_join_properties);

    /// broadcast join
    ChildProperties broadcast_join_properties;
    broadcast_join_properties.push_back({.distribution = {.type = Distribution::Distributed}});
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
    ChildProperties replicated_child_prop;
    ChildProperties singleton_child_prop;
    replicated_child_prop.push_back({.distribution = {.type = Distribution::Replicated}});
    singleton_child_prop.push_back({.distribution = {.type = Distribution::Singleton}});
    return {replicated_child_prop, singleton_child_prop};
}

/// The first child is the main plan branch, the second is the CreatingSetStep.
/// The required child of CreatingSetStep must be Replicated or Singleton.
/// 1. Replicated when in where:
///   SELECT a, sum(b) FROM t1_d WHERE a in (SELECT a FROM t1_d WHERE b < 10)  GROUP BY a
///
///          CreatingSets
///         /          \
///  Expression       CreatingSet
///      |                 |
///    Scan           ExchangeData(Replicated)
///                        |
///                    Expression
///                        |
///                       Scan
///
/// 2. Singleton when in having:
///   SELECT a, sum(b) FROM t1_d GROUP BY a HAVING a in (SELECT a FROM t1_d WHERE b < 10) ORDER BY a
///
///         CreatingSets
///         /           \
///  Expression         CreatingSet
///      |                   |
///  Expression          ExchangeData(Singleton)
///      |                   |
///  Aggregating         Expression
///      |                   |
/// ExchangeData(singleton) Scan
///      |
///  Expression
///      |
///    Scan
///
/// Please notice that:
/// 1. Not all of them are valid, please see the second example, only the Singleton one is valid.
///    We discard the invalid one when calculating output property for CreatingSetsStep.
/// 2. For the first example actually only the Singleton one is valid. The following is the query
///    plan. The prepared set is used in the first scan step which is not in the same fragment with
///    CreatingSets, so CreatingSets can not work properly. And the fragment of the first scan will
///    use the prepared set before it is really prepared.
///
///              CreatingSets
///               /          \
///ExchangeData(Singleton)  CreatingSet
///              |            |
///          Expression    ExchangeData(Singleton)
///              |            |
///            Scan        Expression
///                           |
///                        Expression
///                           |
///                          Scan
AlternativeChildProperties DeriveRequiredChildProps::visit(CreatingSetsStep & /*step*/)
{
    ChildProperties singleton_required_props;
    /// Ensure that CreatingSetsStep is assigned to the same fragment with the left table scan and CreatingSetStep.
    singleton_required_props.push_back({.distribution = {.type = Distribution::Any}, .sorting = required_prop.sorting});
    for (size_t i = 1; i < group_node->childSize(); ++i)
        singleton_required_props.push_back({.distribution = {.type = Distribution::Singleton}});

    ChildProperties replicated_required_props;
    /// Ensure that CreatingSetsStep is assigned to the same fragment with the left table scan and CreatingSetStep.
    replicated_required_props.push_back({.distribution = {.type = Distribution::Any}, .sorting = required_prop.sorting});
    for (size_t i = 1; i < group_node->childSize(); ++i)
        replicated_required_props.push_back({.distribution = {.type = Distribution::Replicated}});
    return {singleton_required_props, replicated_required_props};
}

AlternativeChildProperties DeriveRequiredChildProps::visit(UnionStep & step)
{
    ChildProperties required_child_properties;
    /// union can have more than 2 children
    for (size_t i = 0; i < step.getInputHeaders().size(); i++)
        required_child_properties.push_back({.distribution = {.type = Distribution::Any}});
    ChildProperties distributed_union;
    return {required_child_properties};
}

}
