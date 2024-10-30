#include <Optimizer/Statistics/StatsSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
extern const SettingsFloat statistics_agg_unknown_column_first_key_coefficient;
extern const SettingsFloat statistics_agg_unknown_column_rest_key_coefficient;
extern const SettingsFloat statistics_agg_full_cardinality_coefficient;
}

StatsSettings StatsSettings::fromSettings(const Settings & from)
{
    StatsSettings settings;
    settings.statistics_agg_unknown_column_first_key_coefficient = from[Setting::statistics_agg_unknown_column_first_key_coefficient];
    settings.statistics_agg_unknown_column_rest_key_coefficient = from[Setting::statistics_agg_unknown_column_rest_key_coefficient];
    settings.statistics_agg_full_cardinality_coefficient = from[Setting::statistics_agg_full_cardinality_coefficient];
    return settings;
}

StatsSettings StatsSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
