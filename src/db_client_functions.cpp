#include "db_client_functions.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include <exception>
#include <memory>

bool insert_logs(clickhouse::Client *client, std::vector<HttpLog> logs) {
    auto timestamp = std::make_shared<clickhouse::ColumnDateTime>();
    auto resource_id = std::make_shared<clickhouse::ColumnUInt64>();
    auto bytes_sent = std::make_shared<clickhouse::ColumnUInt64>();
    auto request_time_milli= std::make_shared<clickhouse::ColumnUInt64>();
    auto response_status = std::make_shared<clickhouse::ColumnUInt16>();
    auto cache_status = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto method = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto remote_addr = std::make_shared<clickhouse::ColumnString>();
    auto url = std::make_shared<clickhouse::ColumnString>();

    for (auto const &log : logs) {
        timestamp->Append(log.timestampEpochMilli);
        resource_id->Append(log.requestTimeMilli);
        bytes_sent->Append(log.bytesSent);
        request_time_milli->Append(log.requestTimeMilli);
        response_status->Append(log.responseStatus);
        cache_status->Append(log.cacheStatus);
        method->Append(log.cacheStatus);
        remote_addr->Append(log.remoteAddr);
        url->Append(log.url);
    }

    clickhouse::Block block{};

    block.AppendColumn("timestamp", timestamp);
    block.AppendColumn("resource_id", resource_id);
    block.AppendColumn("bytes_sent", bytes_sent);
    block.AppendColumn("request_time_milli", request_time_milli);
    block.AppendColumn("response_status", response_status);
    block.AppendColumn("cache_status", cache_status);
    block.AppendColumn("method", method);
    block.AppendColumn("remote_addr", remote_addr);
    block.AppendColumn("url", url);

    try {
        client->Insert("http_log", block);
        return true;
    }
    catch(std::exception &e) {
        std::cerr << "Failed inserting into database: " << e.what() << '\n';
        return false;
    }
}

bool reconnect(clickhouse::Client *client) {
    try {
        client->ResetConnection();
        return true;
    }
    catch (std::exception &e) {
        std::cerr << "Failed recconnection: " << e.what() << '\n';
        return false;
    }
}