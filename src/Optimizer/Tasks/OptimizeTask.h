#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Tasks/OptimizeContext.h>
#include <Optimizer/Tasks/TaskContext.h>

namespace DB
{

class Memo;

class OptimizeTask
{
public:
    explicit OptimizeTask(const TaskContextPtr & task_context_);
    virtual ~OptimizeTask();

    virtual void execute() = 0;
    virtual String getDescription() = 0;

protected:
    void pushTask(std::unique_ptr<OptimizeTask> task) const;
    ContextPtr getQueryContext() const;

    TaskContextPtr task_context;
};

using OptimizeTaskPtr = std::unique_ptr<OptimizeTask>;

}
