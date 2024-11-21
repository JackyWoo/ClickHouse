#pragma once

#include <Client/ConnectionPool.h>
#include <QueryCoordination/Pipelines/Pipelines.h>

namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

struct QueryCoordinationState
{
    FragmentPtrs fragments;
    Pipelines pipelines;
    std::unordered_map<String, IConnectionPool::Entry> remote_connections;
    StorageLimitsList storage_limits;
};


}
