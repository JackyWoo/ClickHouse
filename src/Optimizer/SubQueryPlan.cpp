#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <Optimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/JSONBuilder.h>


namespace DB
{

void SubQueryPlan::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();
    if (root)
    {
        const auto & root_header = root->step->getOutputHeader();

        if (!step->getInputHeaders().empty())
        {
            const auto & step_header = step->getInputHeaders().front();
            if (!blocksHaveEqualStructure(root_header, step_header))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot add step {} to SubQueryPlan because it has incompatible header with root step {} root header: {} step header: "
                    "{}",
                    step->getName(),
                    root->step->getName(),
                    root_header.dumpStructure(),
                    step_header.dumpStructure());
        }

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
    }
    else
    {
        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
    }
}

void SubQueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<SubQueryPlanPtr> plans)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlan is already initialized");

    const auto & inputs = step->getInputHeaders();
    size_t num_inputs = step->getInputHeaders().size();
    if (num_inputs != plans.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite QueryPlans using {} because step has different number of inputs. Has {} plans and {} inputs",
            step->getName(),
            plans.size(),
            num_inputs);

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i];
        const auto & plan_header = plans[i]->getCurrentHeader();
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite QueryPlans using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan->nodes));

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        resources = std::move(plan->resources);
    }
}

}
