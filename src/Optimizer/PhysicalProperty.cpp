#include <ranges>

#include <Optimizer/PhysicalProperty.h>
#include "Processors/Transforms/SelectByIndicesTransform.h"

namespace DB
{


bool Sorting::operator==(const Sorting & other) const
{
    if (sort_description.size() != other.sort_description.size())
        return false;
    if (sort_description.size() != commonPrefix(sort_description, other.sort_description).size())
        return false;
    if (sort_scope != other.sort_scope)
        return false;
    return true;
}

bool Sorting::satisfy(const Sorting & required) const
{
    bool sort_description_satisfy = required.sort_description.size() == commonPrefix(sort_description, required.sort_description).size();

    if (!sort_description_satisfy)
        return false;

    if (sort_scope < required.sort_scope)
        return false;

    return true;
}

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
            ret += ",";
            ret += sort_description[i].column_name;
        }
        if (sort_description.size() > 2UL)
            ret += " ...";
        ret += ")";
    }

    return ret;
}

bool Distribution::satisfy(const Distribution & required) const
{
    if (required.type == Any)
        return true;

    if (required.type == Distributed && type != Singleton)
        return true;

    if (required.isHashed() && isHashed())
    {
        if (required.distributed_by_bucket_num != distributed_by_bucket_num)
            return false;

        if (required.keys.size() > keys.size())
            return false;

        for (size_t i = 0; i < required.keys.size(); i++)
            if (required.keys[i] != keys[i])
                return false;

        return true;
    }
    return type == required.type;
}

bool Distribution::canBeEnforcedTo(const Distribution & required) const
{
    if (!isSpecified())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Try to enforce from a not specified distribution {}", toString());
    if (satisfy(required))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No need to enforce, for distribution {} satisfies {}", toString(), required.toString());
    if (!required.isSpecified())
        return false;
    return true;
}

bool Distribution::operator==(const Distribution & other) const
{
    if (isHashed() && other.isHashed())
    {
        if (other.distributed_by_bucket_num != distributed_by_bucket_num)
            return false;

        if (other.keys.size() != keys.size())
            return false;

        for (size_t i = 0; i < keys.size(); i++)
            if (other.keys[i] != keys[i])
                return false;
    }

    return type == other.type;
}

String Distribution::toString() const
{
    String ret;
    switch (this->type)
    {
        case Any:
            ret = "Any";
            break;
        case Singleton:
            ret = "Singleton";
            break;
        case Distributed:
            ret = "Distributed";
            break;
        case Straight:
            ret = "Straight";
            break;
        case Replicated:
            ret = "Replicated";
            break;
        case Hashed:
            ret = "Hashed";
            break;
    }
    return ret;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
std::optional<Distribution> Distribution::deriveOutputDistribution(const Distribution & lhs, const Distribution & rhs)
{
    if (!lhs.isSpecified() || !rhs.isSpecified())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The input properties must be specified when deriving output property, but found {} and {}",
            lhs.toString(),
            rhs.toString());

    if (!checkInputDistributions(lhs, rhs))
        return {};

    if (lhs.type == Singleton && rhs.type == Singleton)
        return Distribution{Singleton};

    if (lhs.type == Replicated && rhs.type == Replicated)
        return Distribution{Replicated};

    if (lhs.type == Hashed && rhs.type == Hashed)
    {
        if (lhs.distributed_by_bucket_num == rhs.distributed_by_bucket_num)
        {
            if (lhs.keys.size() == rhs.keys.size())
            {
                for (size_t i = 0; i < lhs.keys.size(); i++)
                {
                    if (lhs.keys[i] != rhs.keys[i])
                        return Distribution{Straight};
                }
                return lhs;
            }
        }
    }

    return Distribution{Straight};
}
#pragma clang diagnostic pop

bool Distribution::checkInputDistributions(const Distribution & lhs, const Distribution & rhs)
{
    if ((lhs.type == Singleton && rhs.type != Singleton) || (lhs.type != Singleton && rhs.type == Singleton))
        return false;
    return true;
}

bool PhysicalProperty::operator==(const PhysicalProperty & other) const
{
    return sorting == other.sorting && distribution == other.distribution;
}

size_t PhysicalProperty::HashFunction::operator()(const PhysicalProperty & property) const
{
    SipHash hash;
    hash.update(property.distribution.type);
    hash.update(property.distribution.distributed_by_bucket_num);
    for (const auto & key : property.distribution.keys)
        hash.update(key);

    for (const auto & sort : property.sorting.sort_description)
        hash.update(sort.dump());
    hash.update(property.sorting.sort_scope);
    return hash.get64();
}

bool PhysicalProperty::satisfy(const PhysicalProperty & required) const
{
    bool satisfy_sorting = satisfySorting(required);
    bool satisfy_distribution = satisfyDistribution(required);

    return satisfy_sorting && satisfy_distribution;
}

bool PhysicalProperty::satisfySorting(const PhysicalProperty & required) const
{
    return sorting.satisfy(required.sorting);
}

bool PhysicalProperty::satisfyDistribution(const PhysicalProperty & required) const
{
    return distribution.satisfy(required.distribution);
}

bool PhysicalProperty::canBeEnforcedTo(const PhysicalProperty & required) const
{
    return distribution.canBeEnforcedTo((required.distribution));
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
