#include <Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

OptimizeTask::OptimizeTask(const TaskContextPtr & task_context_) : task_context(task_context_)
{
}

OptimizeTask::~OptimizeTask() = default;

void OptimizeTask::pushTask(OptimizeTaskPtr task) const
{
    task_context->pushTask(std::move(task));
}

ContextPtr OptimizeTask::getQueryContext() const
{
    return task_context->getQueryContext();
}

}
