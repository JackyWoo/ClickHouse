#include <Optimizer/Tasks/OptimizeInputs.h>
#include "Processors/Transforms/SelectByIndicesTransform.h"

#include <Optimizer/Cost/CostCalculator.h>
#include <Optimizer/DeriveOutputProp.h>
#include <Optimizer/Group.h>
#include <Optimizer/GroupNode.h>
#include <Optimizer/Memo.h>
#include <Optimizer/Tasks/OptimizeGroup.h>
#include <Optimizer/Tasks/OptimizeTask.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsUInt64 max_block_size;
}

OptimizeInputs::OptimizeInputs(const GroupNodePtr & group_node_, const TaskContextPtr & task_context_, std::unique_ptr<Frame> frame_)
    : OptimizeTask(task_context_), group_node(group_node_), frame(std::move(frame_))
{
    const auto & group = task_context->getCurrentGroup();
    log = &Poco::Logger::get(
        fmt::format("OptimizeInputs-#{}#{}({})", group.getId(), group_node->getId(), group_node->getStep()->getName()));
}

void OptimizeInputs::execute()
{
    if (group_node->isEnforceNode())
    {
        LOG_TRACE(log, "Skip enforce node.");
        return;
    }

    auto & group = task_context->getCurrentGroup();
    LOG_TRACE(log, "Upper bound cost of current group is {}", task_context->getUpperBoundCost().toString());

    const auto & required_prop = task_context->getRequiredProp();
    LOG_TRACE(log, "Required properties is {}", required_prop.toString());

    std::vector<Stats> children_statistics;
    for (auto & child_group : group_node->getChildren())
        children_statistics.emplace_back(child_group->getStatistics());

    /// If frame is null, means it is the first time optimize the node.
    if (!frame)
    {
        frame = std::make_unique<Frame>(group_node, required_prop, task_context->getQueryContext());
        LOG_TRACE(log, "Derived required child properties {}", PhysicalProperty::toString(frame->alternative_child_props));
    }

    /// TODO we do not calculate source step here.

    /// Each alternative children properties is actually a child problem.
    for (; frame->prop_idx < static_cast<Int32>(frame->alternative_child_props.size()); ++frame->prop_idx)
    {
        auto & required_child_props = frame->alternative_child_props[frame->prop_idx];

        if (frame->newAlternativeCalc())
        {
            LOG_TRACE(log, "Evaluate a new alternative children properties {}", PhysicalProperty::toString(required_child_props));
            /// Calculate cost by required_child_props is not a good way, but we shuold calculate it before optimie children.
            CostCalculator cost_calc(group.getStatistics(), task_context, children_statistics, required_child_props);
            frame->local_cost = group_node->accept(cost_calc);
            frame->total_cost = frame->local_cost;
            LOG_TRACE(log, "Cost calculated, local cost {}, total cost {}", frame->local_cost.toString(), frame->total_cost.toString());
        }

        const auto & child_groups = group_node->getChildren();
        for (; frame->child_idx < static_cast<Int32>(child_groups.size()); ++frame->child_idx)
        {
            auto required_child_prop = required_child_props[frame->child_idx];
            auto & child_group = *child_groups[frame->child_idx];

            auto best_node = child_group.tryGetBest(required_child_prop);
            if (!best_node) /// TODO add evaluated flag to GroupNode to identify whether it has no solution or is not evaluated
            {
                if (frame->pre_child_idx >= frame->child_idx)
                {
                    /// child problem no solution for properties required_child_prop
                    LOG_ERROR(
                        log,
                        "Child group {} has no solution for required prop {}, so current node has no solution",
                        child_group.getId(),
                        required_child_prop.toString());
                    break;
                }

                LOG_TRACE(
                    log,
                    "Child group {} has no best plan for required prop {} now, will evaluate it first.",
                    child_group.getId(),
                    required_child_prop.toString());

                frame->pre_child_idx = frame->child_idx;
                auto child_upper_bound_cost = task_context->getUpperBoundCost() - frame->total_cost;

                if (child_upper_bound_cost.get() <= 0.0)
                    LOG_INFO(
                        log,
                        "Upper bound cost of child group {} is {}, which is lower than 0, which means we will discard it.",
                        child_group.getId(),
                        child_upper_bound_cost.get());// TODO return;

                /// Push current task frame to the task stack.
                pushTask(clone());

                /// Child group is not optimized, push child task to the task stack.
                TaskContextPtr child_task_context = std::make_shared<TaskContext>(
                    child_group, required_child_prop, task_context->getOptimizeContext(), child_upper_bound_cost);
                pushTask(std::make_unique<OptimizeGroup>(child_task_context));
                return;
            }

            frame->total_cost += best_node->second.cost;
            if (frame->total_cost >= task_context->getUpperBoundCost())
            {
                /// one of child problems has bigger cost, this alternative plan is not the best
                LOG_INFO(
                    log,
                    "Total cost {} is large than the group upper bound {}, it is not the best",
                    frame->total_cost.get(),
                    task_context->getUpperBoundCost().get());
                break;
            }

            frame->actual_children_prop.emplace_back(best_node->first);
        }

        /// All child problem has solution and this is the best plan
        if (frame->child_idx == static_cast<Int32>(group_node->getChildren().size()))
        {
            /// Derive output prop by required_prop and children_prop
            DeriveOutputProp output_prop_visitor(required_prop, frame->actual_children_prop, task_context->getQueryContext());
            auto output_prop = group_node->accept(output_prop_visitor);

            if (!output_prop.has_value())
            {
                LOG_TRACE(
                    log,
                    "Can not derive valid output property by child properties {} and required {}, discard the solution",
                    PhysicalProperty::toString(frame->actual_children_prop),
                    required_prop.toString());
                frame->resetAlternativeState();
                continue;
            }
            LOG_TRACE(log, "Derived output property {}", output_prop->toString());

            auto child_cost = frame->total_cost - frame->local_cost;
            if (group_node->updateBest(*output_prop, frame->actual_children_prop, child_cost))
            {
                LOG_TRACE(
                    log,
                    "GroupNode update best for property {} to {}, children cost: {}",
                    output_prop->toString(),
                    PhysicalProperty::toString(frame->actual_children_prop),
                    child_cost.toString());
            }

            if (group.updateBest(*output_prop, group_node, frame->total_cost))
            {
                LOG_TRACE(
                    log,
                    "Group update best node for property {} to {}, total cost: {}",
                    output_prop->toString(),
                    group_node->getId(),
                    frame->total_cost.toString());
            }

            /// Currently, it only deals with distribution cases
            if (!output_prop->satisfySorting(required_prop))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sort property not satisfied, output sort prop {}, required sort prop {}",
                    output_prop->sorting.toString(),
                    required_prop.sorting.toString());

            if (!output_prop->satisfyDistribution(required_prop))
            {
                if (!output_prop->canBeEnforcedTo(required_prop))
                {
                    frame->resetAlternativeState();
                    continue;
                }
                /// Use two-level-hash algorithm if distribution is hash and distributed_by_bucket_num is true.
                /// TODO not all keys support two-level-hash algorithm
                enforceTwoLevelAggIfNeed(required_prop);
                /// Enforce exchange node
                frame->total_cost = enforceGroupNode(required_prop, *output_prop);
            }

            /// Update group upper bound cost
            if (frame->total_cost < task_context->getUpperBoundCost())
                task_context->setUpperBoundCost(frame->total_cost);
        }

        frame->resetAlternativeState();
    }
}

void OptimizeInputs::enforceTwoLevelAggIfNeed(const PhysicalProperty & required_prop) const
{
    if (!(required_prop.distribution.type == Distribution::Type::Hashed && required_prop.distribution.distributed_by_bucket_num))
        return;

    auto * aggregate_step = typeid_cast<AggregatingStep *>(group_node->getStep().get());
    if (!aggregate_step || !aggregate_step->isPreliminary())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Required distribution by bucket num, but is not preliminary AggregatingStep");

    /// Distribution type is Hashed and use distributed_by_bucket_num need enforce two level aggregate.
    /// Prevent different nodes from using different aggregation policies which will cause incorrect result.
    LOG_TRACE(log, "Enforced two level aggregate");
    aggregate_step->enforceTwoLevelAgg();
}

Cost OptimizeInputs::enforceGroupNode(const PhysicalProperty & required_prop, const PhysicalProperty & output_prop) const
{
    LOG_TRACE(log, "Enforcing ExchangeData required_prop: {}, output_prop {}", required_prop.toString(), output_prop.toString());

    /// Because the ordering of data may be changed after adding Exchange in a distributed manner,
    /// we need to retain the order of data during exchange if there is a requirement for data sorting.
    Sorting sorting;
    bool merging_sort_when_sink = false;
    bool merging_sort_when_source = false;

    if (required_prop.sorting.sort_scope == Sorting::Scope::Stream
        && output_prop.sorting.sort_scope >= Sorting::Scope::Stream)
    {
        sorting = {.sort_description = required_prop.sorting.sort_description, .sort_scope = Sorting::Scope::Stream};
        merging_sort_when_sink = true;
    }
    else if (required_prop.sorting.sort_scope == Sorting::Scope::Global)
    {
        sorting = {.sort_description = required_prop.sorting.sort_description, .sort_scope = Sorting::Scope::Global};
        merging_sort_when_sink = true;
        merging_sort_when_source = true;
    }
    // else if (output_prop.sorting.sort_scope == Sorting::Scope::Chunk /// Not used right now
    //     && required_prop.distribution.type != Distribution::Type::Hashed)
    // {
    //     /// We can keep the sort info if distribution type is not hash.
    //     sorting = {.sort_description = required_prop.sorting.sort_description, .sort_scope = Sorting::Scope::Chunk};
    // }
    else
    {
    }

    size_t max_block_size = task_context->getQueryContext()->getSettingsRef()[Setting::max_block_size];
    auto exchange_step = std::make_shared<ExchangeDataStep>(
        task_context->getQueryContext()->getCurrentQueryId(),
        group_node->getStep()->getOutputHeader(),
        max_block_size,
        required_prop.distribution,
        sorting,
        merging_sort_when_sink,
        merging_sort_when_source);

    auto & group = task_context->getCurrentGroup();

    std::vector<Group *> children;
    children.emplace_back(&group);
    GroupNodePtr group_enforce_node = std::make_shared<GroupNode>(exchange_step, children, true);

    auto group_node_id = task_context->getMemo().fetchAddGroupNodeId();
    group.addGroupNode(group_enforce_node, group_node_id);

    auto child_cost = group.getLowestCost(output_prop);
    if (!child_cost)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find node and cost for {} in group {}", output_prop.toString(), group.getId());

    CostCalculator cost_calc(group.getStatistics(), task_context);
    auto cost = group_enforce_node->accept(cost_calc);
    Cost total_cost = cost + *child_cost;
    LOG_TRACE(log, "Enforced ExchangeData {} and now total cost is {}", group_enforce_node->getId(), total_cost.toString());

    DeriveOutputProp output_prop_visitor(required_prop, {output_prop}, task_context->getQueryContext());
    const auto & actual_output_prop = group_enforce_node->accept(output_prop_visitor);

    if (!actual_output_prop.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not derive valid output property for enforced node.");

    group_enforce_node->updateBest(*actual_output_prop, {output_prop}, *child_cost);
    LOG_TRACE(log, "Derived output property for enforced node {}", actual_output_prop->toString());

    group.updateBest(*actual_output_prop, group_enforce_node->shared_from_this(), total_cost);
    LOG_TRACE(
        log,
        "Enforced ExchangeData and now best plan node for {} is {}",
        actual_output_prop->toString(),
        group.tryGetBest(required_prop)->second.group_node->getId());

    return total_cost;
}

OptimizeTaskPtr OptimizeInputs::clone()
{
    return std::make_unique<OptimizeInputs>(group_node, task_context, std::move(frame));
}

String OptimizeInputs::getDescription()
{
    return "OptimizeInputs (" + group_node->getStep()->getName() + ")";
}

}
