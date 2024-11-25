#include <QueryCoordination/Fragments/Fragment.h>

#include <stack>
#include <Core/SortCursor.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 max_block_size;
}

Fragment::Fragment(const ContextMutablePtr & context_)
    : id(0), node_id_counter(0), root(nullptr), dest_exchange_node(nullptr), dest_fragment_id(0), context(context_)
{
}

void Fragment::setId(UInt32 id_)
{
    id = id_;
}

const Header & Fragment::getOutputHeader() const
{
    return root->step->getOutputHeader();
}

PlanNode * Fragment::getRoot() const
{
    return root;
}

void Fragment::setRoot(Fragment::Node * root_)
{
    if (root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} already has a root {}", id, root->step->getName());
    chassert(root_ != nullptr);
    root = root_;
}

const FragmentPtrs & Fragment::getChildren() const
{
    return children;
}

void Fragment::addChild(const FragmentPtr & child)
{
    children.push_back(child);
}

const Fragment::Nodes & Fragment::getNodes() const
{
    return nodes;
}

Fragment::Node * Fragment::addNode(const Node & node)
{
    nodes.push_back(node);
    return &nodes.back();
}

UInt32 Fragment::addAndFetchNodeID()
{
    return ++node_id_counter;
}

UInt32 Fragment::getID() const
{
    return id;
}

UInt32 Fragment::getDestFragmentID() const
{
    return dest_fragment_id;
}

void Fragment::setDestFragmentID(UInt32 dest_fragment_id_)
{
    dest_fragment_id = dest_fragment_id_;
}

bool Fragment::hasDestFragment() const
{
    return dest_exchange_node != nullptr;
}

UInt32 Fragment::getDestExchangeID() const
{
    if (!dest_exchange_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} does not have parent", id);
    return dest_exchange_node->id;
}

const Fragment::Node * Fragment::getDestExchangeNode() const
{
    return dest_exchange_node;
}

void Fragment::setDestExchangeNode(Node * dest_exchange_node_)
{
    dest_exchange_node = dest_exchange_node_;
}

void Fragment::clearDestExchangeNodeChildren() const
{
    if (dest_exchange_node)
        dest_exchange_node->children.clear();
}

void Fragment::assignPlanNodeID()
{
    struct Frame
    {
        Node * node;
        std::vector<Node *> children;
    };

    std::stack<Frame> stack;
    stack.push({.node = root});

    Node * last_visited_node = nullptr;

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (last_visited_node)
        {
            frame.children.push_back(last_visited_node);
            last_visited_node = nullptr;
        }

        if (frame.children.size() == frame.node->children.size())
        {
            frame.node->id = ++node_id_counter;
            last_visited_node = frame.node;
            stack.pop();
        }
        else
        {
            auto * next_child = frame.node->children[frame.children.size()];
            stack.push({.node = next_child});
        }
    }
}

QueryPipelineBuilderPtr Fragment::buildQueryPipeline(
    const QueryPlanOptimizationSettings & /*optimization_settings*/, const BuildQueryPipelineSettings & build_pipeline_settings)
{
    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines = {};
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});


    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr;
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size()) /// children belong next fragment
        {
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);

            stack.pop();
        }
        else
        {
            stack.push(Frame{.node = frame.node->children[next_child]});
        }
    }

    last_pipeline->setProgressCallback(build_pipeline_settings.progress_callback);
    last_pipeline->setProcessListElement(build_pipeline_settings.process_list_element);

    return last_pipeline;
}

QueryPipeline Fragment::buildQueryPipeline(std::vector<ExchangeDataSink::Channel> & channels, const String & local_host)
{
    auto builder
        = buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

    if (hasDestFragment())
    {
        String query_id;
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
            query_id = context->getCurrentQueryId();
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
            query_id = context->getInitialQueryId();

        auto * exchange_data_step = typeid_cast<ExchangeDataStep *>(dest_exchange_node->step.get());

        if (exchange_data_step->sinkMerge() && builder->getNumStreams() > 1)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                builder->getHeader(),
                builder->getNumStreams(),
                exchange_data_step->getSortDescription(),
                context->getSettingsRef()[Setting::max_block_size],
                /*max_block_size_bytes=*/0,
                SortingQueueStrategy::Batch,
                0,
                true);

            builder->addTransform(transform);
        }

        QueryPipeline pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

        auto sink = std::make_shared<ExchangeDataSink>(
            pipeline.getHeader(),
            channels,
            exchange_data_step->getDistribution(),
            local_host,
            query_id,
            getDestFragmentID(),
            dest_exchange_node->id);

        pipeline.complete(sink);

        return pipeline;
    }

    return QueryPipelineBuilder::getPipeline(std::move(*builder));
}

static void
explainStep(const IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings, const Fragment::ExplainFragmentOptions & options)
{
    std::string prefix(settings.offset, ' ');
    settings.out << prefix;
    settings.out << step.getName();

    const auto & description = step.getStepDescription();
    if (options.description && !description.empty())
        settings.out << " (" << description << ')';

    settings.out.write('\n');

    if (options.header)
    {
        settings.out << prefix;

        if (!step.hasOutputHeader())
            settings.out << "No header";
        else if (!step.getOutputHeader())
            settings.out << "Empty header";
        else
        {
            settings.out << "Header: ";
            bool first = true;

            for (const auto & elem : step.getOutputHeader())
            {
                if (!first)
                    settings.out << "\n" << prefix << "        ";

                first = false;
                elem.dumpNameAndType(settings.out);
            }
        }
        settings.out.write('\n');
    }

    if (options.sorting)
    {
        if (const auto & sort_description = step.getSortDescription(); !sort_description.empty())
        {
            settings.out << prefix << "Sorting: ";
            dumpSortDescription(sort_description, settings.out);
            settings.out.write('\n');
        }
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
        step.describeIndexes(settings);
}

void Fragment::dumpPlan(WriteBufferFromOwnString & buffer, const ExplainFragmentOptions & options)
{
    buffer.write('\n');
    std::string str("Fragment " + std::to_string(id));

    if (dest_exchange_node)
    {
        str += ", Data to:";
        str += std::to_string(dest_fragment_id);
    }
    buffer.write(str.c_str(), str.size());
    buffer.write('\n');

    explainPlan(buffer, options);

    for (const auto & child_fragment : children)
        child_fragment->dumpPlan(buffer, options);
}

void Fragment::dumpPipeline(WriteBufferFromOwnString & buffer, const ExplainFragmentPipelineOptions & options) const
{
    explainPipeline(buffer, options);
    for (const auto & child_fragment : children)
        child_fragment->dumpPipeline(buffer, options);

}

void Fragment::explainPlan(WriteBuffer & buffer, const ExplainFragmentOptions & options)
{
    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    std::unordered_set<Node *> all_nodes;
    for (auto & node : nodes)
        all_nodes.insert(&node);

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = (stack.size() - 1) * settings.indent;
            explainStep(*frame.node->step, settings, options);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            if (all_nodes.contains(frame.node->children[frame.next_child]))
                stack.push(Frame{frame.node->children[frame.next_child]});

            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")\n";

    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.indent;
}

void Fragment::explainPipeline(WriteBuffer & buffer, const ExplainFragmentPipelineOptions & options) const
{
    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        size_t offset = 0;
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = frame.offset;
            explainPipelineStep(*frame.node->step, settings);
            frame.offset = settings.offset;
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child], frame.offset});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

}
