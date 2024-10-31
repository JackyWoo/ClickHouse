#include <Optimizer/DeriveOutputProp.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Optimizer/GroupNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>


namespace DB
{
namespace Setting
{
extern const SettingsBool optimize_query_coordination_sharding_key;
}


namespace
{

ExpressionActionsPtr buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(false);
}

}

DeriveOutputProp::DeriveOutputProp(
    const PhysicalProperty & required_prop_,
    const std::vector<PhysicalProperty> & children_prop_,
    ContextPtr context_)
    : required_prop(required_prop_), child_properties(children_prop_), context(context_)
{
}

PhysicalProperty DeriveOutputProp::visit(const QueryPlanStepPtr & step)
{
    return Base::visit(step);
}

PhysicalProperty DeriveOutputProp::visitDefault(IQueryPlanStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;

    if (step.stepType() == StepType::Scan)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Step {} not implemented with CBO optimizer", step.getName());

    /// preserve sort properties for transforming step
    auto * transforming_step = dynamic_cast<ITransformingStep *>(&step);
    if (transforming_step && transforming_step->getDataStreamTraits().preserves_sorting)
    {
        const auto & transforming_step_sorting = child_properties[0].sorting;
        /// There are some cases where sort desc is misaligned with the header,
        /// and in this case it is not required to keep the sort prop.
        /// E.g. select * from aaa_all where name in (select name from bbb_all where name like '%d%') order by id limit 13 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1;
        /// CreatingSetStep header is id_0, name_1 but its sort desc is id
        SortDescription sort_desc;
        for (const auto & sort_column : transforming_step_sorting.sort_description)
            if (step.getOutputHeader().has(sort_column.column_name))
                sort_desc.emplace_back(sort_column.column_name);
            else
                break;
        res.sorting.sort_description = sort_desc;
        /// The output sorting scop maybe stream or global we use the lower level one.
        res.sorting.sort_scope = Sorting::Scope::Stream;
    }

    return res;
}

PhysicalProperty DeriveOutputProp::visit(UnionStep & step)
{
    SortDescription common_sort_description = std::move(child_properties[0].sorting.sort_description);
    auto sort_scope = child_properties[0].sorting.sort_scope;

    for (size_t i = 1; i < step.getInputHeaders().size(); ++i)
    {
        common_sort_description = commonPrefix(common_sort_description, child_properties[i].sorting.sort_description);
        sort_scope = std::min(sort_scope, child_properties[i].sorting.sort_scope);
    }

    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;// TODO really?
    res.sorting = {.sort_description = std::move(common_sort_description), .sort_scope = sort_scope};
    return res;
}

PhysicalProperty DeriveOutputProp::visit(ReadFromMergeTree & step)
{
    PhysicalProperty res;
    res.sorting.sort_description = step.getSortDescription();
    if (res.sorting.sort_description.empty())
        res.sorting.sort_scope = Sorting::Scope::None;
    else
        res.sorting.sort_scope = Sorting::Scope::Stream;

    /// Optimize distribution by sharding key

    if (!context->getSettingsRef()[Setting::optimize_query_coordination_sharding_key])
    {
        res.distribution = {.type = Distribution::Any};
        return res;
    }

    const auto & meta_info = context->getQueryCoordinationMetaInfo();
    const auto & storage_id = step.getStorageID();

    auto table_it = std::find(meta_info.storages.begin(), meta_info.storages.end(), storage_id);
    if (table_it == meta_info.storages.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Not found {}.{} in {}", storage_id.database_name, storage_id.table_name, meta_info.toString());

    /// distribute by any integer type value. TODO need to distinguish which functions have a clear distribution of data. e.g rand() is not clear, hash is clear and single column is clear
    /// TODO Enabling optimize_query_coordination_sharding_key may cause incorrect results, such as sharding key is cityHash64(xid + zid), which is not sharding with xid and zid.
    size_t table_idx = std::distance(meta_info.storages.begin(), table_it);
    const String & sharding_key = meta_info.sharding_keys[table_idx];

    if (sharding_key.empty())
    {
        res.distribution = {.type = Distribution::Any};
        return res;
    }

    const char * begin = sharding_key.data();
    const char * end = begin + sharding_key.size();

    ParserExpression expression_parser;
    ASTPtr expression = parseQuery(expression_parser, begin, end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    ExpressionActionsPtr sharding_key_expr
        = buildShardingKeyExpression(expression, context, step.getStorageMetadata()->columns.getAllPhysical());

    if (sharding_key_expr->getRequiredColumns().empty())
    {
        res.distribution = {.type = Distribution::Any};
        return res;
    }

    /// Suppose the columns is combined hash
    const auto & output_names = step.getOutputHeader().getNames();
    for (const auto & key : sharding_key_expr->getRequiredColumns())
    {
        if (std::count(output_names.begin(), output_names.end(), key) != 1)
        {
            res.distribution = {.type = Distribution::Any};
            return res;
        }
    }

    res.distribution.type = Distribution::Hashed;
    res.distribution.keys = sharding_key_expr->getRequiredColumns();
    return res;
}

PhysicalProperty DeriveOutputProp::visit(FilterStep & step)
{
    const auto & expr = step.getExpression();
    const ActionsDAG::Node * out_to_skip = nullptr;
    if (step.removesFilterColumn())
    {
        out_to_skip = expr.tryFindInOutputs(step.getFilterColumnName());
        if (!out_to_skip)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
                step.getFilterColumnName(),
                expr.dumpDAG());
    }

    SortDescription sort_desc;
    applyActionsToSortDescription(sort_desc, expr, out_to_skip);

    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    res.sorting.sort_description = sort_desc;
    res.sorting.sort_scope = Sorting::Scope::Stream;

    return res;
}

PhysicalProperty DeriveOutputProp::visit(TopNStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    res.sorting.sort_description = step.getSortDescription();
    res.sorting.sort_scope = step.hasPartitions() ? Sorting::Scope::Stream : Sorting::Scope::Global;
    return res;
}

PhysicalProperty DeriveOutputProp::visit(SortingStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    res.sorting.sort_description = step.getSortDescription();
    res.sorting.sort_scope = step.hasPartitions() ? Sorting::Scope::Stream : Sorting::Scope::Global;
    return res;
}

PhysicalProperty DeriveOutputProp::visit(ExchangeDataStep & step)
{
    PhysicalProperty res;
    res.distribution = step.getDistribution();
    res.sorting.sort_description = step.getSortDescription();
    res.sorting.sort_scope = step.getSortScope();
    return res;
}

PhysicalProperty DeriveOutputProp::visit(ExpressionStep & step)
{
    PhysicalProperty res;

    /// Try to reserve sorting property
    res.sorting = child_properties[0].sorting;
    applyActionsToSortDescription(res.sorting.sort_description, step.getExpression());

    if (child_properties[0].distribution.type == Distribution::Hashed)
    {
        const auto & distribution_keys = child_properties[0].distribution.keys;
        res.distribution.type = Distribution::Hashed;
        res.distribution.keys.resize(distribution_keys.size());

        /// If actions will keep the original input keys keep the distributed keys info
        FindOutputByName finder(step.getExpression());
        for (size_t i = 0; i < distribution_keys.size(); ++i)
        {
            const auto & original_column = distribution_keys[i];/// TODO enhance
            const auto * node = finder.find(original_column);
            if (node)
                res.distribution.keys[i] = node->result_name;
            else
                res.distribution.type = Distribution::Any;
        }
    }
    else
    {
        res.distribution = child_properties[0].distribution;
    }

    return res;
}

PhysicalProperty DeriveOutputProp::visit(AggregatingStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    if (step.inOrder())
        res.sorting = {.sort_description = step.getSortDescription(), .sort_scope = Sorting::Scope::Global};
    return res;
}

PhysicalProperty DeriveOutputProp::visit(MergingAggregatedStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    if (step.memoryBoundMergingWillBeUsed())
        res.sorting = {.sort_description = step.getGroupBySortDescription(), .sort_scope = Sorting::Scope::Global};
    return res;
}

PhysicalProperty DeriveOutputProp::visit(DistinctStep & step)
{
    PhysicalProperty res;
    res.distribution = child_properties[0].distribution;
    /// Sorting order of distinct step is properly set in applyOrder.cpp
    if (!step.getSortDescription().empty())
        res.sorting.sort_description = step.getSortDescription();

    /// Distinct never breaks global order
    if (child_properties[0].sorting.sort_scope == Sorting::Scope::Global)
        res.sorting.sort_scope = child_properties[0].sorting.sort_scope;

    /// Preliminary Distinct also does not break stream order
    if (step.isPreliminary() && child_properties[0].sorting.sort_scope == Sorting::Scope::Stream)
        res.sorting.sort_scope = child_properties[0].sorting.sort_scope;

    /// TODO Try to reserve hashed distribution
    return res;
}
}
