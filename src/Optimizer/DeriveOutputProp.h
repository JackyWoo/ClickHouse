#pragma once

#include <Optimizer/PhysicalProperty.h>
#include <Optimizer/PlanStepVisitor.h>

#include "GroupNode.h"

namespace DB
{

/// Derive the actual output property for node based on the child properties.
/// Return blank if the child properties are not compatible, see DeriveOutputProp::Distribution::checkInputDistributions.
class DeriveOutputProp : public PlanStepVisitor<std::optional<PhysicalProperty>>
{
public:
    using Base = PlanStepVisitor<std::optional<PhysicalProperty>>;

    DeriveOutputProp(
        const PhysicalProperty & required_prop_,
        const ChildProperties & children_prop_,
        ContextPtr context_);

    std::optional<PhysicalProperty> visit(const QueryPlanStepPtr & step) override;

    std::optional<PhysicalProperty> visitDefault(IQueryPlanStep & step) override;

    std::optional<PhysicalProperty> visit(ReadFromMergeTree & step) override;

    std::optional<PhysicalProperty> visit(FilterStep & step) override;

    std::optional<PhysicalProperty> visit(SortingStep & step) override;

    std::optional<PhysicalProperty> visit(ExchangeDataStep & step) override;

    std::optional<PhysicalProperty> visit(ExpressionStep & step) override;

    std::optional<PhysicalProperty> visit(TopNStep & step) override;

    std::optional<PhysicalProperty> visit(CreatingSetsStep & step) override;

    std::optional<PhysicalProperty> visit(CreatingSetStep & step) override;

    std::optional<PhysicalProperty> visit(UnionStep & step) override;

    std::optional<PhysicalProperty> visit(AggregatingStep & step) override;

    std::optional<PhysicalProperty> visit(MergingAggregatedStep & step) override;

    std::optional<PhysicalProperty> visit(DistinctStep & step) override;

private:
    [[maybe_unused]] const PhysicalProperty & required_prop;
    const ChildProperties & child_properties;

    ContextPtr context;
};

}
