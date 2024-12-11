#include <Scheduler/Exchange/ExchangeManager.h>
#include <Scheduler/Fragments/Fragment.h>

namespace DB
{

ExchangeDataSourcePtr ExchangeManager::findExchangeDataSource(const ExchangeDataRequest & exchange_data_request)
{
    std::lock_guard lock(mutex);
    const auto it = query_exchange_data_sources.find(exchange_data_request.query_id);

    if (it == query_exchange_data_sources.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find exchange data receiver for query {}", exchange_data_request.query_id);

    const auto & receiver_key = receiverKey(
        exchange_data_request.fragment_id, exchange_data_request.exchange_id, exchange_data_request.from_host);

    const auto & exchanges = it->second;
    const auto exchange_it = exchanges.find(receiver_key);

    if (exchange_it == exchanges.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find  exchange data receiver for exchange {}", exchange_data_request.toString());

    return exchange_it->second;
}

void ExchangeManager::registerExchangeDataSource(const ExchangeDataRequest & exchange_data_request, ExchangeDataSourcePtr receiver)
{
    std::lock_guard lock(mutex);
    auto & exchanges = query_exchange_data_sources[exchange_data_request.query_id];

    const auto & receiver_key = receiverKey(
        exchange_data_request.fragment_id, exchange_data_request.exchange_id, exchange_data_request.from_host);

    exchanges.emplace(receiver_key, receiver);
}

void ExchangeManager::removeExchangeDataSources(const String & query_id)
{
    std::lock_guard lock(mutex);
    query_exchange_data_sources.erase(query_id);
}

}
