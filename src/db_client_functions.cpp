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
        timestamp->Append(log.timestampEpochMilli / 1000);
        resource_id->Append(log.resourceId);
        bytes_sent->Append(log.bytesSent);
        request_time_milli->Append(log.requestTimeMilli);
        response_status->Append(log.responseStatus);
        cache_status->Append(log.cacheStatus);
        method->Append(log.method);
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

void setup_database(clickhouse::Client *client) {
    client->Execute(R"(
        CREATE TABLE IF NOT EXISTS http_log
            (
                timestamp          DateTime,
                resource_id        UInt64,
                bytes_sent         UInt64,
                request_time_milli UInt64,
                response_status    UInt16,
                cache_status       LowCardinality(String),
                method             LowCardinality(String),
                remote_addr        String,
                url                String
            )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (resource_id, timestamp);)");
    
    client->Execute(R"(
        CREATE TABLE IF NOT EXISTS traffic_rollup_1m 
            (
                bucket_start        DateTime,
                resource_id         UInt64,
                response_status     UInt16,
                cache_status        LowCardinality(String),
                remote_addr         String,
                requests            UInt64,
                bytes_sent          UInt64,
                request_time_milli  UInt64
            )
        ENGINE = SummingMergeTree
        PARTITION BY toYYYYMM(bucket_start)
        ORDER BY (resource_id, bucket_start, response_status, cache_status, remote_addr);)");
    client->Execute(R"(
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_traffic_rollup_1m 
        TO traffic_rollup_1m 
        AS
        SELECT 
            toStartOfMinute(timestamp) AS bucket_start, 
            resource_id,
            response_status,
            cache_status,
            remote_addr,
            count()                    AS requests,
            sum(bytes_sent)            AS bytes_sent,
            sum(request_time_milli)   AS request_time_milli
        FROM http_log
        GROUP BY bucket_start, resource_id, response_status, cache_status, remote_addr;)");
}
