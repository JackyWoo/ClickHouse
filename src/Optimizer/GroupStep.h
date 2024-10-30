#pragma once

#include <Optimizer/Group.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class GroupStep final : public IQueryPlanStep
{
public:
    explicit GroupStep(const Header & output_header_, Group & group_);

    String getName() const override;

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    Group & getGroup() const;

    void updateOutputHeader() override {}

private:
    Group & group;
};

}
