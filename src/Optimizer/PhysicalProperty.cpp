#include <Optimizer/PhysicalProperty.h>

namespace DB
{

String Sorting::toString() const
{
    String ret;
    switch (sort_scope)
    {
        case Scope::None:
            ret += "None";
            break;
        case Scope::Stream:
            ret += "Stream";
            break;
        case Scope::Global:
            ret += "Global";
            break;
        case Scope::Chunk:
            ret += "Chunk";
            break;
    }

    if (!sort_description.empty())
    {
        ret += "(" + sort_description[0].column_name;
        for (size_t i = 1; i < std::min(2UL, sort_description.size()); ++i)
        {
            ret += "/";
            ret += sort_description[i].column_name;
        }
        if (sort_description.size() > 2UL)
            ret += "/...";
        ret += ")";
    }

    return ret;
}

String Distribution::toString() const
{
    switch (this->type)
    {
        case Any:
            return "Any";
        case Singleton:
            return "Singleton";
        case Replicated:
            return "Replicated";
        case Hashed:
            return "Hashed";
    }
}

bool PhysicalProperty::operator==(const PhysicalProperty & other) const
{
    if (sorting.sort_description.size() != other.sorting.sort_description.size())
        return false;

    if (sorting.sort_description.size() != commonPrefix(sorting.sort_description, other.sorting.sort_description).size())
        return false;

    if (sorting.sort_scope != other.sorting.sort_scope)
        return false;

    if (other.distribution.keys.size() != distribution.keys.size())
        return false;

    if (other.distribution.distributed_by_bucket_num != distribution.distributed_by_bucket_num)
        return false;

    for (const auto & key : distribution.keys)
        if (std::count(other.distribution.keys.begin(), other.distribution.keys.end(), key) != 1)
            return false;

    return distribution.type == other.distribution.type;
}

bool PhysicalProperty::satisfy(const PhysicalProperty & required) const
{
    bool satisfy_sorting = satisfySorting(required);
    bool satisfy_distribution = satisfyDistribution(required);

    return satisfy_sorting && satisfy_distribution;
}

bool PhysicalProperty::satisfySorting(const PhysicalProperty & required) const
{
    bool sort_description_satisfy = required.sorting.sort_description.size()
        == commonPrefix(sorting.sort_description, required.sorting.sort_description).size();

    if (!sort_description_satisfy)
        return false;

    bool sort_scope_satisfy = sorting.sort_scope >= required.sorting.sort_scope;

    if (!sort_scope_satisfy)
        return false;

    return true;
}

bool PhysicalProperty::satisfyDistribution(const PhysicalProperty & required) const
{
    if (required.distribution.type == Distribution::Any)
        return true;

    if (required.distribution.distributed_by_bucket_num != distribution.distributed_by_bucket_num)
        return false;

    for (const auto & key : distribution.keys)
        if (std::count(required.distribution.keys.begin(), required.distribution.keys.end(), key) != 1)
            return false;

    return distribution.type == required.distribution.type;
}

String PhysicalProperty::toString() const
{
    return distribution.toString() + "-" + sorting.toString();
}

String PhysicalProperty::toString(const PhysicalProperties & properties)
{
    if (properties.empty())
        return "[]";

    String ret = "[" + properties[0].toString();
    for (size_t i = 1; i < properties.size(); ++i)
    {
        ret += ",";
        ret += properties[i].toString();
    }
    ret += "]";
    return ret;
}

String PhysicalProperty::toString(const std::vector<PhysicalProperties> & properties)
{
    if (properties.empty())
        return "[]";

    String ret = "[" + toString(properties[0]);
    for (size_t i = 1; i < properties.size(); ++i)
    {
        ret += "/";
        ret += toString(properties[i]);
    }
    ret += "]";
    return ret;
}

}
