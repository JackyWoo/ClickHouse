#pragma once

#include <Optimizer/Group.h>
#include <Optimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class Memo
{
public:
    Memo(QueryPlan && plan, ContextPtr context_);

    /// Initialize memo by a query plan
    Group & buildGroup(const QueryPlan::Node & node);

    GroupNodePtr addPlanNodeToGroup(const QueryPlan::Node & node, Group * target_group);

    void dump() const;

    Group & rootGroup() const;

    QueryPlan extractPlan();
    SubQueryPlan extractPlan(Group & group, const PhysicalProperty & required_prop);

    UInt32 fetchAddGroupNodeId() { return ++group_node_id_counter; }

private:
    UInt32 group_id_counter{0};
    UInt32 group_node_id_counter{0};

    std::list<Group> groups;
    Group * root_group;

    std::unordered_set<GroupNodePtr, GroupNodeHash, GroupNodeEquals> all_group_nodes;

    ContextPtr context;
    Poco::Logger * log = &Poco::Logger::get("Memo");
};

}
