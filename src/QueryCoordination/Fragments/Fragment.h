#pragma once

#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Exchange/ExchangeDataSink.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

/**Fragment is a part of the whole distributed query plan.
 * It is split by ExchangeDataStep in the original query plan.
 *
 * The following is a query plan for a shuffle-hash-join
 *       Projection
 *          |
 *         Join
 *        /    \
 *  Exchange   Exchange
 *     /          \
 *    Scan       Scan
 *
 * Then we will have 3 fragments:
 *       Projection
 *          |
 *         Join
 *        /    \
 *  Exchange   Exchange         Scan        Scan
 */
class Fragment : public std::enable_shared_from_this<Fragment>
{
public:
    using Node = QueryPlan::Node;
    using Nodes = std::list<Node>;

    /// Explain options for distributed plan, only work with query coordination
    struct ExplainFragmentOptions
    {
        /// Add output header to step.
        bool header = false;
        /// Add description of step.
        bool description = true;
        /// Add detailed information about step actions.
        bool actions = false;
        /// Add information about indexes actions.
        bool indexes = false;
        /// Add information about sorting
        bool sorting = false;
        /// Add fragment and host mappings information
        bool host = false; /// TODO implement
    };

    struct ExplainFragmentPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    explicit Fragment(const ContextMutablePtr & context_);

    void setId(Int32 fragment_id_);
    const Header & getOutputHeader() const;

    Node * getRoot() const;
    void setRoot(Node * root_);

    const Nodes & getNodes() const;
    Node * addNode(const Node & node);

    UInt32 addAndFetchNodeID();

    void dumpPlan(WriteBufferFromOwnString & buffer, const ExplainFragmentOptions & options);
    void dumpPipeline(WriteBufferFromOwnString & buffer, const ExplainFragmentPipelineOptions & options) const;

    const FragmentPtrs & getChildren() const;
    void addChild(const FragmentPtr & child);

    UInt32 getFragmentID() const;

    /// Whether a fragment has a destination(parent), if not, means it is the root fragment.
    bool hasDestFragment() const;
    UInt32 getDestFragmentID() const;
    void setDestFragmentID(UInt32 dest_fragment_id_);

    UInt32 getDestExchangeID() const;
    const Node * getDestExchangeNode() const;
    void setDestExchangeNode(Node * dest_exchange_node_);

    void clearDestExchangeNodeChildren() const;

    void assignPlanNodeID();

    QueryPipeline buildQueryPipeline(std::vector<ExchangeDataSink::Channel> & channels, const String & local_host);
    void explainPipeline(WriteBuffer & buffer, const ExplainFragmentPipelineOptions & options = {.header = false}) const;

private:
    void explainPlan(WriteBuffer & buffer, const ExplainFragmentOptions & options);

    /// Build query pipeline for the fragment, note that it is a part of the whole distributed pipelines.
    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings, const BuildQueryPipelineSettings & build_pipeline_settings);

    UInt32 fragment_id;
    UInt32 node_id_counter;

    Nodes nodes;
    Node * root;

    /// parent
    Node * dest_exchange_node;
    /// start from 1, 0 means no parent or not initialized
    UInt32 dest_fragment_id;

    /// children fragments
    FragmentPtrs children;

    ContextMutablePtr context;
};

}
