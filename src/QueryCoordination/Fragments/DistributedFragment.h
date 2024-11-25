#pragma once

#include <QueryCoordination/Exchange/ExchangeDataSource.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentRequest.h>

namespace DB
{

class DistributedFragment
{
public:
    DistributedFragment(const FragmentPtr & fragment_, const Destinations & data_to_, const Sources & data_from_)
        : fragment(fragment_), data_to(data_to_), data_from(data_from_)
    {
    }

    FragmentPtr getFragment() const { return fragment; }

    const Destinations & getDataTo() const { return data_to; }
    const Sources & getDataFrom() const { return data_from; }

private:
    FragmentPtr fragment;

    Destinations data_to;
    Sources data_from;
};

using DistributedFragments = std::vector<DistributedFragment>;

}
