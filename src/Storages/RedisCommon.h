#pragma once

#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Array.h>

#include <base/BorrowedObjectPool.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

enum class RedisStorageType
{
    SIMPLE,
    HASH_MAP,
    UNKNOWN
};

String toString(RedisStorageType storage_type);
RedisStorageType toRedisStorageType(const String & storage_type);

struct RedisConfiguration
{
    String host;
    uint32_t port;
    uint32_t db_index;
    String password;
    RedisStorageType storage_type;
    uint32_t pool_size;
};

using RedisArray = Poco::Redis::Array;
using RedisCommand = Poco::Redis::Command;
using RedisBulkString = Poco::Redis::BulkString;

using RedisClientPtr = std::unique_ptr<Poco::Redis::Client>;
using RedisPool = BorrowedObjectPool<RedisClientPtr>;
using RedisPoolPtr = std::shared_ptr<RedisPool>;

struct RedisConnection
{
    RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_);
    ~RedisConnection();

    RedisPoolPtr pool;
    RedisClientPtr client;
};

using RedisConnectionPtr = std::unique_ptr<RedisConnection>;

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration) ;

}
