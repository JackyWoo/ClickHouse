#include <Optimizer/Group.h>
#include <Optimizer/GroupNode.h>

namespace DB
{

GroupNode::GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_, bool is_enforce_node_)
    : id(0), step(std::move(step_)), group(nullptr), children(children_), is_enforce_node(is_enforce_node_), stats_derived(false)
{
}

GroupNode::~GroupNode() = default;
GroupNode::GroupNode(GroupNode &&) noexcept = default;

void GroupNode::addChild(Group & child)
{
    children.emplace_back(&child);
}

size_t GroupNode::childSize() const
{
    return children.size();
}

std::vector<Group *> GroupNode::getChildren() const
{
    return children;
}

const QueryPlanStepPtr & GroupNode::getStep() const
{
    return step;
}

Group & GroupNode::getGroup() const
{
    return *group;
}

void GroupNode::setGroup(Group * group_)
{
    group = group_;
}

bool GroupNode::updateBestChild(const PhysicalProperty & property, const PhysicalProperties & child_properties, const Cost & child_cost)
{
    if (!best_properties.contains(property) || child_cost < best_properties[property].cost)
    {
        best_properties[property] = {child_properties, child_cost};
        return true;
    }
    return false;
}

const ChildProperties & GroupNode::getBestChildProperties(const PhysicalProperty & property)
{
    return best_properties[property].child_prop;
}

void GroupNode::addRequiredChildProperties(ChildProperties & required_child_prop)
{
    required_child_props.emplace_back(required_child_prop);
}

AlternativeChildProperties & GroupNode::getAlternativeRequiredChildProperties()
{
    return required_child_props;
}

bool GroupNode::hasRequiredChildProperties() const
{
    return !required_child_props.empty();
}

bool GroupNode::isEnforceNode() const
{
    return is_enforce_node;
}

UInt32 GroupNode::getId() const
{
    return id;
}

void GroupNode::setId(UInt32 id_)
{
    id = id_;
}

void GroupNode::setStatsDerived()
{
    stats_derived = true;
}

bool GroupNode::hasStatsDerived() const
{
    return stats_derived;
}

bool GroupNode::hasApplied(size_t rule_id) const
{
    return rule_masks.test(rule_id);
}

void GroupNode::setApplied(size_t rule_id)
{
    rule_masks.set(rule_id);
}

String GroupNode::getDescription() const
{
    String res;
    res += "node " + std::to_string(getId()) + "(";
    res += step->getName() + ")";
    return res;
}

String GroupNode::toString() const
{
    String res = getDescription();

    res += " children: ";
    if (children.empty())
    {
        res += "none";
    }
    else
    {
        res += std::to_string(children[0]->getId());
        for (size_t i = 1; i < children.size(); i++)
        {
            res += "/";
            res += std::to_string(children[i]->getId());
        }
    }

    res += ", best properties: ";
    String prop_map;

    if (best_properties.empty())
    {
        prop_map = "none";
    }
    else
    {
        size_t num = 1;
        for (const auto & [output_prop, child_prop_cost] : best_properties)
        {
            prop_map += output_prop.distribution.toString() + "=";
            if (child_prop_cost.child_prop.empty())
            {
                prop_map += "none";
            }
            else
            {
                prop_map += child_prop_cost.child_prop[0].distribution.toString();
                for (size_t i = 1; i < child_prop_cost.child_prop.size(); i++)
                {
                    prop_map += "/";
                    prop_map += child_prop_cost.child_prop[i].distribution.toString();
                }
            }

            if (num != best_properties.size())
                prop_map += "-";
            num++;
        }
    }

    res += prop_map;
    return res;
}

}
