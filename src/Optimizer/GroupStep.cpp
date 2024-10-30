#include <Optimizer/GroupStep.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

GroupStep::GroupStep(const Header & output_header_, Group & group_) : group(group_)
{
    output_header = output_header_;
    setStepDescription("Group (" + std::to_string(group.getId()) + ")");
}

String GroupStep::getName() const
{
    return "Group (" + std::to_string(group.getId()) + ")";
}

QueryPipelineBuilderPtr GroupStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "'updatePipeline' is not implemented for group step.");
}

Group & GroupStep::getGroup() const
{
    return group;
}

}
