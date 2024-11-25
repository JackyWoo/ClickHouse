#pragma once

#include <Scheduler/Fragments/Fragment.h>
#include <Scheduler/Fragments/FragmentRequest.h>

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

class DistributedFragmentBuilder
{
public:
    DistributedFragmentBuilder(const FragmentPtrs & all_fragments_, const std::vector<FragmentRequest> & plan_fragment_requests_)
        : all_fragments(all_fragments_), plan_fragment_requests(plan_fragment_requests_)
    {
    }

    DistributedFragments build() const;

private:
    const FragmentPtrs & all_fragments;
    const std::vector<FragmentRequest> & plan_fragment_requests;
};

}
