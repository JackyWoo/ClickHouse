#pragma once

#include <Optimizer/PhysicalProperty.h>
#include <Optimizer/PlanStepVisitor.h>

#include "GroupNode.h"

namespace DB
{

class DeriveOutputProp : public PlanStepVisitor<PhysicalProperty>
{
public:
    using Base = PlanStepVisitor<PhysicalProperty>;

    DeriveOutputProp(
        const PhysicalProperty & required_prop_,
        const ChildProperties & children_prop_,
        ContextPtr context_);

    PhysicalProperty visit(const QueryPlanStepPtr & step) override;

    PhysicalProperty visitDefault(IQueryPlanStep & step) override;

    PhysicalProperty visit(ReadFromMergeTree & step) override;

    PhysicalProperty visit(FilterStep & step) override;

    PhysicalProperty visit(SortingStep & step) override;

    PhysicalProperty visit(ExchangeDataStep & step) override;

    PhysicalProperty visit(ExpressionStep & step) override;

    PhysicalProperty visit(TopNStep & step) override;

    PhysicalProperty visit(UnionStep & step) override;

    PhysicalProperty visit(AggregatingStep & step) override;

    PhysicalProperty visit(MergingAggregatedStep & step) override;

    PhysicalProperty visit(DistinctStep & step) override;

private:
    PhysicalProperty required_prop;
    ChildProperties child_properties;

    ContextPtr context;
};

}
