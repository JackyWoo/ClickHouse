#include <Optimizer/Cost/CostSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
extern const SettingsFloat cost_cpu_weight;
extern const SettingsFloat cost_mem_weight;
extern const SettingsFloat cost_net_weight;
extern const SettingsFloat cost_pre_sorting_operation_weight;
extern const SettingsFloat cost_merge_agg_uniq_calculation_weight;
}

Cost::Weight CostSettings::getCostWeight() const
{
    return {cost_cpu_weight, cost_mem_weight, cost_net_weight};
}

CostSettings CostSettings::fromSettings(const Settings & from)
{
    CostSettings settings;
    settings.cost_cpu_weight = from[Setting::cost_cpu_weight];
    settings.cost_mem_weight = from[Setting::cost_mem_weight];
    settings.cost_net_weight = from[Setting::cost_net_weight];
    settings.cost_pre_sorting_operation_weight = from[Setting::cost_pre_sorting_operation_weight];
    settings.cost_merge_agg_uniq_calculation_weight = from[Setting::cost_merge_agg_uniq_calculation_weight];
    return settings;
}

CostSettings CostSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
