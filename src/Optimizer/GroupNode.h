#pragma once

#include <Optimizer/Cost/Cost.h>
#include <Optimizer/PhysicalProperty.h>
#include <Optimizer/PlanStepVisitor.h>
#include <Optimizer/Rule/RuleSet.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using ChildProperties = PhysicalProperties;
using AlternativeChildProperties = std::vector<ChildProperties>;

class Group;
class GroupNode;
using GroupNodePtr = std::shared_ptr<GroupNode>;

class GroupNode final : public std::enable_shared_from_this<GroupNode>
{
public:
    struct ChildPropertiesAndCost
    {
        ChildProperties child_prop;
        Cost cost;
    };

    struct HashFunction
    {
        UInt64 operator()(const GroupNodePtr & group_node) const;
    };

    struct EqualsFunction
    {
        bool operator()(const GroupNodePtr & lhs, const GroupNodePtr & rhs) const;
    };

    /// output property -> best corresponding child properties
    using BestPropertiesMapping = std::unordered_map<PhysicalProperty, ChildPropertiesAndCost, PhysicalProperty::HashFunction>;

    GroupNode(GroupNode &&) noexcept;
    GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_, bool is_enforce_node_ = false);

    ~GroupNode();

    const QueryPlanStepPtr & getStep() const;

    void addChild(Group & child);
    size_t childSize() const;
    std::vector<Group *> getChildren() const;

    Group & getGroup() const;
    void setGroup(Group * group_);

    bool updateBest(const PhysicalProperty & property, const ChildProperties & child_properties, const Cost & child_cost);
    std::optional<const ChildProperties> tryGetBest(const PhysicalProperty & property);

    bool hasRequiredChildProperties() const;
    AlternativeChildProperties & getAlternativeRequiredChildProperties();
    void addRequiredChildProperties(ChildProperties & required_child_prop);

    bool isEnforceNode() const;

    String toString() const;
    String getDescription() const;

    UInt32 getId() const;
    void setId(UInt32 id_);

    void setStatsDerived();
    bool hasStatsDerived() const;

    bool hasApplied(size_t rule_id) const;

    void setApplied(size_t rule_id);

    template <class Visitor>
    typename Visitor::ResultType accept(Visitor & visitor)
    {
        return visitor.visit(step);
    }

private:
    UInt32 id;
    QueryPlanStepPtr step;

    Group * group;
    std::vector<Group *> children;

    bool is_enforce_node;

    BestPropertiesMapping best_properties;
    AlternativeChildProperties required_child_props;

    bool stats_derived;

    std::bitset<CostBasedOptimizerRules::RULES_SIZE> rule_masks;
};

}
