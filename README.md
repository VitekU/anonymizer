# Anonymizer Consumer

Kafka consumer that reads HTTP logs (Cap'n Proto encoded) from the `http_log` Kafka topic, anonymizes the client IP (last octet replaced with `X`) and inserts them into ClickHouse in batches (currently set to 1000, but could be set higher, depending on the memory constraints). On startup it creates the `http_log` table, a per-minute traffic rollup (`SummingMergeTree`) and a materialized view over it for fast querying of the traffic totals that are stored in the database. The atleast once policy for inserting the logs into the database is maintained by committing the offset to Kafka only on successful insert to database. In case of the database connection outage, the consumer periodically tries to reconnect to the database. If it succeeds it continues polling and inserting messages like it did before the outage.

## Building

The project requires a C++17 compiler, CMake >= 3.14, pkg-config, librdkafka and Cap'n Proto dev packages.

```bash
cmake -S . -B build
cmake --build build
```

Run with - the services that it connects to like Kafka and ClickHouse database also need to be running (all optional, defaults shown):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
CLICKHOUSE_ADDRESS=localhost CLICKHOUSE_PORT=9000 \
CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD="" \
BATCH_SIZE=10000 \
./build/consumer
```

## Running with Docker Compose

```bash
docker compose up -d
```

This starts the all of the services needed for this service to run: Kafka, the log producer, the consumer - anonymizer (that is built from this project) and ClickHouse. It also starts Kafka UI (http://localhost:8080), Prometheus (http://localhost:9090) and Grafana (http://localhost:3000, user `admin`, password `kafka`). You can also run the consumer executable in another console and it will connect and work with the fallback environment values.

## Development process, flaws etc.

The project took me about a week of development mainly due to the research of the technologies used. First, I've spent time on the Kafka setup and setting up the boilerplate code to poll messages from the broker to the consumer. I then dedicated time to design the pipeline of processing the messages. As specified in the assignment, each message has to be stored in the database and it has to be done in batches. The batches part has the obvious solution of storing the polled requests in a vector until there is enough of them to be inserted into the database. If we insert `10 000` records as suggested in the ClickHouse docs and assuming the memory footprint of log record is about 300 bytes, it would mean that the logs would occupy about `3MB` of RAM at one point before inserting, which I feel is perfectly acceptable in this situation (`100 000` logs would mean about `30MB`). 

I had a bit of trouble setting up the `Cap'n proto` decoder (mainly due to dependencies) but I've managed to get it working after a bit of trial and error. I then went straight to setting up the `ClickHouse` database. It was my first time working with a database of this type so I dedicated some time to reading up the documentation and learning how to set it up with the `clickhouse-cpp` library. Around this time I've decided to switch to `cmake` build system because I think that it made installing the library easier. At that time, I've created the table manually through the web interface (later I had it done from code).

For the anonymization of the address I've set on modifying the address string in memory, I've found this implementation pretty elegant and straight forward.

Now for one of the most important parts, which was ensuring that the connection to the database would be restored if the database would come down and up. To make sure that a message cannot be lost during the anonymization process I've decided on the following pipeline: Store messages in the buffer until there is enough of them -> insert them into the database -> commit the offset to the `Kafka` only if the insert was successful. That ensures that even in case that the insert fails and the anonymizer crashes, the messages are still stored in `Kafka` and ready to be polled again by the anonymizer. This however introduces 2 problems. If the anonymizer crashes after the batch was inserted and before the committing of the offset, the logs will be polled again from `Kafka` and stored in the database again, which leads to duplicates. This can be solved by some deduplication engine in the database, however that can slow down performance and there could still be duplicate records in the database at one point in time. 
The second problem is that if the insert was unsuccessful, now the logs sit in the buffer in memory indefinitely (until the connection is reestablished). This could be solved by flushing the buffer after some set time, however it would mean that the flushed logs would have to be processed again in the future. 

The anonymizer tries to reconnect to the database each `X` seconds, which could be set in the program. With slight modification of the `Timer` class, this time interval could grow over with retries and completely stop after that reaching some arbitrary threshold. This would prevent the wasting of resources, however I'm not sure what would the limits and intervals be in production. The timer could also be used to insert the logs into database after certain amount of time instead of relying solely on the amount of them in the buffer.

The part with efficient querying of the total traffic data is done via `ClickHouse`s `materialized view` functionality that upon inserting a new batch updates the rollup table with the summed up values. The service that need said data then query against the rollup table. Like I've mentioned, since this is my first time with a database of this type, it took me a bit longer to understand exactly how does the setup and the syntax work, but I've managed to get it done especially thanks to the ClickHouse docs. This also has that problem with duplicate data, since the duplicate values are added through the materialized view to the data in the rollup table, which means, that the total are then not accurate. We would have to see, how reliable is this consumer to decide whether the error in the totals is too high for our liking or if the margin of error is acceptable.

So this is basically the whole solution, It is not perfect and these are some of the problems that would have to be resolved in production environment. If a message from `Kafka` could not be decoded, It is not being handled in any way and that log is just lost. I think that these messages could be sent to a separate database and they could be recovered or analysed in some other way. It would nonetheless be a metric that I think is good to have. The consumer also does not handle rebalances of partitions in any way, so that could lead to problems when the anonymizer is removed from the group and that could cause more duplicates in the database.

#### Here I list some of the some problems in points like a small TLDR; (altough only for the problems)

- **Latency of the data -** A batch is inserted into the database only when 10000 logs accumulate; there is no time-based flush, so the logs in a batch sit in memory indefinitely. With a slow producer the end-to-end latency is unbounded (time to fill a whole batch). This could easily be solved by introducing a timer that would trigger the insertion of the logs into the database in a preset interval.
- **Losing data -** If ClickHouse is down longer than Kafka's log retention, then Kafka deletes unconsumed messages and they are lost permanently. I also have a problem with undecodable messages. I currently only print out a messages that a message could not be decoed and move on. In the future i would probaly introduce some other table (in the database) where I would store such messages so that they could potentially be processed in a different way.
- **Duplicate records in ClickHouse database -** To maintain the atleaset once policy, Kafka offsets are committed only after a successful batch insert, so a crash between insert and commit would mean that the logs from kafka are polled and inserted multiple times . The table has a plain `MergeTree` engine with no deduplication, so reinserted rows are kept in database. ClickHouse though has some options for deduplication, such as the `ReplacingMergeTree` engine which can solve this issue, but the bigger problem is, that the rollup table would be updated twice by the materialized view and therefore the totals would be higher then in reality.
- **Error Handling -** I currently do not handle some of the errors like not being able to initally connect to the ClickHouse database and/or errors on creating the table, rollup table and materialized view in the database. The consumer just crashes in these instances of errors. In the future I would like to put in place some robust error handling system which would for example log the errors properly.
