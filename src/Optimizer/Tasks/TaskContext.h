#pragma once

#include <Optimizer/Cost/Cost.h>
#include <Optimizer/Cost/CostSettings.h>
#include <Optimizer/PhysicalProperty.h>
#include <Optimizer/Tasks/OptimizeContext.h>

namespace DB
{

class Group;
class OptimizeTask;
using OptimizeTaskPtr = std::unique_ptr<OptimizeTask>;

class TaskContext
{
public:
    TaskContext(Group & group_, const PhysicalProperty & required_property_, OptimizeContextPtr optimize_context_);
    TaskContext(Group & group_, const PhysicalProperty & required_property_, OptimizeContextPtr optimize_context_, const Cost & upper_bound_cost_);

    Group & getCurrentGroup() const;
    const PhysicalProperty & getRequiredProp() const;

    OptimizeContextPtr getOptimizeContext() const;
    ContextPtr getQueryContext() const;

    Memo & getMemo();

    Cost getUpperBoundCost() const;
    void setUpperBoundCost(const Cost & upper_bound_cost_);

    void pushTask(OptimizeTaskPtr task);

private:
    Group & group;

    PhysicalProperty required_property;

    Cost upper_bound_cost;

    OptimizeContextPtr optimize_context;
};

using TaskContextPtr = std::shared_ptr<TaskContext>;

}
