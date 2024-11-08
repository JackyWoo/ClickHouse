#pragma once

#include <Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

class OptimizeGroup final : public OptimizeTask
{
public:
    OptimizeGroup(const TaskContextPtr & task_context_);

    void execute() override;

    String getDescription() override;
};

}
