# Anonymizer Consumer

Kafka consumer that reads HTTP logs (Cap'n Proto encoded) from the `http_log` Kafka topic, anonymizes the client IP (last octet replaced with `X`) and inserts them into ClickHouse in batches. On startup it creates the `http_log` table, a per-minute traffic rollup (`SummingMergeTree`) and a materialized view over it for fast querying of the traffic totals that are stored in the database.

## Building

The project requires a C++17 compiler, CMake >= 3.14, pkg-config, librdkafka and Cap'n Proto dev packages.

```bash
cmake -S . -B build
cmake --build build
```

Run with (all optional, defaults shown):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
CLICKHOUSE_ADDRESS=localhost CLICKHOUSE_PORT=9000 \
CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD="" \
./build/consumer
```

## Running with Docker Compose

```bash
docker compose up -d
```

This starts the all of the services needed for this service to run: Kafka, the log producer, the consumer - anonymizer (that is built from this project) and ClickHouse. It also starts Kafka UI (http://localhost:8080), Prometheus (http://localhost:9090) and Grafana (http://localhost:3000, user `admin`, password `kafka`). You can also run the consumer executable in another console and it will connect and work with the fallback environment values.

## Flaws

- **What is the latency of the data?** A batch is inserted into the database only when 1000 logs accumulate; there is no time-based flush, so the logs in a batch sit in memory indefinitely. With a slow producer the end-to-end latency is unbounded (time to fill a whole batch). This could easily be solved by introducing a timer that would trigger the insertion of the logs into the database in a preset interval.
- **Is there any scenario when you will start losing data?** If ClickHouse is down longer than Kafka's log retention, then Kafka deletes unconsumed messages and they are lost permanently. I also have a problem with undecodable messages. I currently only print out a messages that a message could not be decoed and move on. In the future i would probaly introduce some other table (in the database) where I would store such messages so that they could potentially be processed in a different way.
- **Is it possible that duplicate records will be stored in ClickHouse?** Yes. To maintain the atleaset once policy, Kafka offsets are committed only after a successful batch insert, so a crash between insert and commit would mean that the logs from kafka are polled and inserted multiple times . The table has a plain `MergeTree` engine with no deduplication, so reinserted rows are kept in database. ClickHouse though has some options for deduplication, such as the `ReplacingMergeTree` engine which can solve this issue, but I think that still at some point in time (between merges in the table) there could be duplicate records stored in the database.
