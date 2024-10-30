#include <Optimizer/CBOSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
extern const SettingsCBOStepExecutionMode cbo_aggregating_mode;
extern const SettingsCBOStepExecutionMode cbo_topn_mode;
extern const SettingsCBOStepExecutionMode cbo_sorting_mode;
extern const SettingsCBOStepExecutionMode cbo_limiting_mode;
extern const SettingsCBOJoinDistributionMode cbo_join_distribution_mode;
}

CBOSettings CBOSettings::fromSettings(const Settings & from)
{
    CBOSettings settings;
    settings.cbo_aggregating_mode = from[Setting::cbo_aggregating_mode];
    settings.cbo_topn_mode = from[Setting::cbo_topn_mode];
    settings.cbo_sorting_mode = from[Setting::cbo_sorting_mode];
    settings.cbo_limiting_mode = from[Setting::cbo_limiting_mode];
    settings.cbo_join_distribution_mode = from[Setting::cbo_join_distribution_mode];
    return settings;
}

CBOSettings CBOSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
