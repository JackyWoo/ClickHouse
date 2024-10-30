#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryPlan;
class PreparedSets;
class ActionsDAG;

/// Build query plans for subqueries in 'in' clause
/// This will tipically add DelayedCreatingSetsStep to main query plan
void addBuildSubqueriesForSetsStep(
    QueryPlan & query_plan,
    ContextPtr context,
    PreparedSets & prepared_sets,
    const std::vector<const ActionsDAG *> & result_actions_to_execute);

}
