#include <QueryCoordination/Fragments/DistributedFragmentBuilder.h>
#include <QueryCoordination/Fragments/Fragment.h>


namespace DB
{

DistributedFragments DistributedFragmentBuilder::build() const
{
    std::unordered_map<UInt32, FragmentRequest> id_fragments;
    for (const auto & request : plan_fragment_requests)
    {
        auto * log = &Poco::Logger::get("DistributedFragmentBuilder");
        LOG_TRACE(log, "Receive fragment {} from remote", request.fragment_id);
        id_fragments.emplace(request.fragment_id, request);
    }

    DistributedFragments res_fragments;

    for (const auto & fragment : all_fragments)
    {
        auto it = id_fragments.find(fragment->getID());
        if (it != id_fragments.end())
        {
            auto & request = it->second;
            res_fragments.emplace_back(DistributedFragment(fragment, request.data_to, request.data_from));
        }
    }

    return res_fragments;
}

}
