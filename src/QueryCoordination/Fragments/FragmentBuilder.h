#pragma once

#include <Optimizer/SubQueryPlan.h>
#include <QueryCoordination/Fragments/Fragment.h>

namespace DB
{

class FragmentBuilder
{
public:
    FragmentBuilder(QueryPlan & plan_, const ContextMutablePtr & context_);

    FragmentPtr build();

private:
    void buildFragmentTree(const PlanNode * node, PlanNode * parent, const FragmentPtr & current_fragment, bool new_fragment_root);
    void assignFragmentID(const FragmentPtr & fragment);

    QueryPlan & plan;
    ContextMutablePtr context;
};

}
