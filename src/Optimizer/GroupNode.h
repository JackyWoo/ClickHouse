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

class GroupNode final : public std::enable_shared_from_this<GroupNode>
{
public:
    struct ChildPropertiesAndCost
    {
        ChildProperties child_prop;
        Cost cost;
    };

    /// output(required) property -> best corresponding child properties
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

    bool updateBestChild(const PhysicalProperty & property, const ChildProperties & child_properties, const Cost & child_cost);
    const ChildProperties & getBestChildProperties(const PhysicalProperty & property);

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

using GroupNodePtr = std::shared_ptr<GroupNode>;

struct GroupNodeHash
{
    std::size_t operator()(const GroupNodePtr & group_node) const
    {
        if (!group_node)
            return 0;

        size_t hash = 0;
        /// TODO implement
        //        size_t hash = s->getStep()->hash();
        //        hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(child_groups.size()));
        //        for (auto child_group : child_groups)
        //        {
        //            hash = MurmurHash3Impl64::combineHashes(hash, child_group);
        //        }
        return hash;
    }
};

struct GroupNodeEquals
{
    /// TODO implement
    bool operator()(const GroupNodePtr & /*lhs*/, const GroupNodePtr & /*rhs*/) const { return false; }
};

}
