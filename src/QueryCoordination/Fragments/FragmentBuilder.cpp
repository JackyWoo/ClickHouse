#include <QueryCoordination/Fragments/FragmentBuilder.h>
#include <Interpreters/Context.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>

namespace DB
{

namespace
{

void clearExchangeNodeChildren(const FragmentPtr & fragment)
{
    for (auto & child : fragment->getChildren())
        clearExchangeNodeChildren(child);
    if (fragment->hasDestFragment())
        fragment->clearDestExchangeNodeChildren();
}

void assignPlanNodeID(const FragmentPtr & fragment)
{
    for (auto & child : fragment->getChildren())
        assignPlanNodeID(child);
    fragment->assignPlanNodeID();
}

void buildFragmentsRelationship(const FragmentPtr & fragment, const FragmentPtr & parent)
{
    if (parent)
    {
        const auto * dest_exchange_node = fragment->getDestExchangeNode();
        chassert(dest_exchange_node != nullptr);
        auto * exchange_step = typeid_cast<ExchangeDataStep *>(dest_exchange_node->step.get());
        chassert(exchange_step != nullptr);
        exchange_step->setFragmentID(parent->getFragmentID());
        exchange_step->setPlanNodeID(dest_exchange_node->id);
    }

    for (const auto & child : fragment->getChildren())
        buildFragmentsRelationship(child, fragment);
}

}

FragmentBuilder::FragmentBuilder(QueryPlan & plan_, const ContextMutablePtr & context_) : plan(plan_), context(context_)
{
}

void FragmentBuilder::buildFragmentTree(const PlanNode * node, PlanNode * parent, const FragmentPtr & current_fragment, bool new_fragment_root)
{
    auto * added_node = current_fragment->addNode(*node);
    added_node->children.clear();

    if (parent)
        parent->children.emplace_back(added_node);

    if (typeid_cast<ExchangeDataStep *>(added_node->step.get()))
    {
        const auto fragment = std::make_shared<Fragment>(context);
        fragment->setDestExchangeNode(added_node);
        current_fragment->addChild(fragment);

        for (const auto * child : node->children)
            buildFragmentTree(child, added_node, fragment, true);
    }
    else
    {
        if (new_fragment_root)
            current_fragment->setRoot(added_node);

        for (const auto * child : node->children)
            buildFragmentTree(child, added_node, current_fragment, false);
    }
}

/// We hope that the fragment ID follows the pre-order traversal of the tree.
void FragmentBuilder::assignFragmentID(const FragmentPtr & fragment)
{
    for (const auto & child : fragment->getChildren())
        assignFragmentID(child);

    fragment->setId(context->getFragmentID());

    for (const auto & child : fragment->getChildren())
        child->setDestFragmentID(fragment->getFragmentID());
}

FragmentPtr FragmentBuilder::build()
{
    auto root_fragment = std::make_shared<Fragment>(context);
    buildFragmentTree(plan.getRootNode(), nullptr, root_fragment, true);
    assignFragmentID(root_fragment);
    clearExchangeNodeChildren(root_fragment);
    assignPlanNodeID(root_fragment);
    buildFragmentsRelationship(root_fragment, nullptr);
    return root_fragment;
}

}
