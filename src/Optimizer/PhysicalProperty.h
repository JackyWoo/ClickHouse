#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/SipHash.h>

namespace DB
{

struct Distribution
{
    enum Type : int8_t
    {
        Any = 1,
        Singleton = 2,
        Replicated = 3,
        Hashed = 4,
    };

    String toString() const;
    bool isHashed() const { return type == Hashed; }

    /// Calculate the output distribution based on the children inputs.
    /// Used for union step.
    static Distribution deriveOutputDistribution(const Distribution & lhs, const Distribution & rhs);

    /// Check whether the input distributions are valid.
    /// e.g. Replicated and Singleton are not valid.
    static bool checkInputDistributions(const Distribution & lhs, const Distribution & rhs);

    Type type;
    Names keys; /// keys for Hashed
    bool distributed_by_bucket_num = false;
};

struct Sorting
{
    enum class Scope : uint8_t
    {
        None = 0,   /// Not sorted
        Chunk = 1,  /// Each chunk is sorted
        Stream = 2, /// Each data steam is sorted
        // Server = 3, /// Data is sorted in a server
        Global = 4, /// Data is globally sorted
    };

    String toString() const;

    SortDescription sort_description = {};
    Scope sort_scope = Scope::None;
};

class PhysicalProperty;
using PhysicalProperties = std::vector<PhysicalProperty>;

class PhysicalProperty
{
public:
    bool operator==(const PhysicalProperty & other) const;

    struct HashFunction
    {
        size_t operator()(const PhysicalProperty & properties) const
        {
            SipHash hash;
            hash.update(int8_t(properties.distribution.type));
            for (const auto & key : properties.distribution.keys)
                hash.update(key);

            for (const auto & sort : properties.sorting.sort_description)
                hash.update(sort.dump());
            return hash.get64();
        }
    };

    bool satisfy(const PhysicalProperty & required) const;
    bool satisfySorting(const PhysicalProperty & required) const;
    bool satisfyDistribution(const PhysicalProperty & required) const;

    String toString() const;
    static String toString(const PhysicalProperties & properties);
    static String toString(const std::vector<PhysicalProperties> & properties);

    Distribution distribution = {.type = Distribution::Any};
    Sorting sorting;
};

}
