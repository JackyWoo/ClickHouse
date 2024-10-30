#include <Optimizer/Group.h>
#include <Optimizer/Tasks/OptimizeTask.h>
#include <Optimizer/Tasks/TaskContext.h>

namespace DB
{

TaskContext::TaskContext(Group & group_, const PhysicalProperty & required_propertys_, OptimizeContextPtr optimize_context_)
    : group(group_), required_property(required_propertys_), optimize_context(optimize_context_)
{
    upper_bound_cost = Cost::infinite(CostSettings::fromContext(optimize_context_->getQueryContext()).getCostWeight());
}

TaskContext::TaskContext( Group & group_, const PhysicalProperty & required_property_, OptimizeContextPtr optimize_context_, Cost upper_bound_cost_)
    : group(group_), required_property(required_property_), upper_bound_cost(upper_bound_cost_), optimize_context(optimize_context_)
{
}

Group & TaskContext::getCurrentGroup()
{
    return group;
}

const PhysicalProperty & TaskContext::getRequiredProp() const
{
    return required_property;
}

OptimizeContextPtr TaskContext::getOptimizeContext()
{
    return optimize_context;
}

ContextPtr TaskContext::getQueryContext() const
{
    return optimize_context->getQueryContext();
}

Memo & TaskContext::getMemo()
{
    return optimize_context->getMemo();
}

void TaskContext::pushTask(OptimizeTaskPtr task)
{
    optimize_context->pushTask(std::move(task));
}

Cost TaskContext::getUpperBoundCost() const
{
    return upper_bound_cost;
}

void TaskContext::setUpperBoundCost(const Cost & upper_bound_cost_)
{
    upper_bound_cost = upper_bound_cost_;
}

}
