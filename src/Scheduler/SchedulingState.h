#pragma once

#include <Client/ConnectionPool.h>
#include <Scheduler/Pipelines/FragmentPipelines.h>

namespace DB
{

class Fragment;

using FragmentPtr = std::shared_ptr<Fragment>;
using FragmentPtrs = std::vector<FragmentPtr>;

struct SchedulingState
{
    FragmentPtrs fragments;
    FragmentPipelines pipelines;
    std::unordered_map<String, IConnectionPool::Entry> remote_connections;
    StorageLimitsList storage_limits;
};

}
