#pragma once

#include <Optimizer/GroupNode.h>
#include <Optimizer/PlanStepVisitor.h>

namespace DB
{

class DeriveRequiredChildProps : public PlanStepVisitor<AlternativeChildProperties>
{
public:
    explicit DeriveRequiredChildProps(
        const GroupNodePtr & group_node_, const PhysicalProperty & required_prop_, const ContextPtr & context_)
        : group_node(group_node_), required_prop(required_prop_), context(context_)
    {
    }

    using Base = PlanStepVisitor<AlternativeChildProperties>;

    AlternativeChildProperties visit(const QueryPlanStepPtr & step) override;

    AlternativeChildProperties visitDefault(IQueryPlanStep & step) override;

    AlternativeChildProperties visit(ReadFromMergeTree & step) override;

    AlternativeChildProperties visit(AggregatingStep & step) override;

    AlternativeChildProperties visit(MergingAggregatedStep & step) override;

    AlternativeChildProperties visit(ExpressionStep & step) override;

    AlternativeChildProperties visit(SortingStep & step) override;

    AlternativeChildProperties visit(LimitStep & step) override;

    AlternativeChildProperties visit(JoinStep & step) override;

    AlternativeChildProperties visit(ExchangeDataStep & step) override;

    AlternativeChildProperties visit(CreatingSetStep & step) override;

    AlternativeChildProperties visit(TopNStep & step) override;

    AlternativeChildProperties visit(DistinctStep & step) override;

    AlternativeChildProperties visit(CreatingSetsStep & step) override;

    AlternativeChildProperties visit(FilterStep & step) override;

    AlternativeChildProperties visit(UnionStep & step) override;

private:
    GroupNodePtr group_node;
    /// required properties for group_node
    PhysicalProperty required_prop;
    ContextPtr context;
};

}
