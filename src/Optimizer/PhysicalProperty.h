#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/SipHash.h>

namespace DB
{

struct Distribution
{
    /// Represent how a plan step distribute data to its parent step.
    ///
    /// Satisfying relationships:
    ///
    ///                        Any
    ///                      /     \
    ///           Distributed       Singleton
    ///          /     |      \
    ///  Straight  Replicated  Hashed
    ///
    ///  Note that Any and Distributed are not specific types who
    ///  can only be used when deriving required child properties
    ///  but can not be used in real output property.
    enum Type : int8_t
    {
        Any = 1,         /// can represent all others
        Singleton = 2,   /// all child nodes send data to one parent node.
        Distributed = 3, /// data will send to all parent nodes, can represent Straight, Replicated, Hashed
        Straight = 4,    /// every node send data to itself
        Replicated = 5,  /// every child node send data to all parent nodes.
        Hashed = 6,      /// every child node send data to all parent nodes by hashing.
    };

    bool satisfy(const Distribution & required) const;

    bool isHashed() const { return type == Hashed; }
    bool isSpecified() const { return type != Any && type != Distributed; }

    bool canBeEnforcedTo(const Distribution & required) const;

    bool operator==(const Distribution & other) const;

    String toString() const;

    /// Calculate the output distribution based on the children inputs.
    /// Used for union step.
    /// Return blank when lhs and rhs are not compatible.
    static std::optional<Distribution> deriveOutputDistribution(const Distribution & lhs, const Distribution & rhs);

    /// Check whether the input distributions are valid.
    /// e.g. Replicated and Singleton are not valid.
    static bool checkInputDistributions(const Distribution & lhs, const Distribution & rhs);

    Type type;

    /// For hashed
    Names keys;
    bool distributed_by_bucket_num = false;
};

struct Sorting
{
    enum class Scope : uint8_t
    {
        None = 0,   /// Not sorted
        Chunk = 1,  /// Each chunk is sorted
        Stream = 2, /// Each data steam is sorted
        /// Server = 3, /// Data is sorted in a server
        Global = 4, /// Data is globally sorted
    };

    String toString() const;
    bool satisfy(const Sorting & required) const;

    bool operator==(const Sorting & other) const;

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
        size_t operator()(const PhysicalProperty & property) const;
    };

    bool satisfy(const PhysicalProperty & required) const;
    bool satisfySorting(const PhysicalProperty & required) const;
    bool satisfyDistribution(const PhysicalProperty & required) const;

    /// Whether I can be enforced to the required
    bool canBeEnforcedTo(const PhysicalProperty & required) const;

    String toString() const;
    static String toString(const PhysicalProperties & properties);
    static String toString(const std::vector<PhysicalProperties> & properties);

    Distribution distribution = {.type = Distribution::Any};
    Sorting sorting;
};

}
