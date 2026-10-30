#include <signal.h>
#include <err.h>
#include <librdkafka/rdkafka.h>

#include "clickhouse/client.h"
#include "setup.h"
#include "decoder.h"
#include "db_client_functions.h"
#include "addr_anonymizer.h"

#define TOPIC "http_log"
#define BATCH_SIZE 10

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
    (void)sig;
    run = 0;
}

int main(void) {
    rd_kafka_t *consumer;   
    char errstr[512];
    bool database_responsive = true;

    if (!setup_consumer(&consumer, errstr)) {
        errx(1, "Failed to create new consumer.");
    }

    if (!subscribe_to_topic(&consumer, TOPIC)) {
        errx(1, "Failed to subscribe to topic %s", TOPIC);
    }

    signal(SIGINT, stop);

    std::vector<HttpLog> log_buffer;

    clickhouse::Client client{clickhouse::ClientOptions {}.SetHost(CLICKHOUSE_ADDRES).SetPort(CLICKHOUSE_PORT).SetUser(CLICKHOUSE_USER).SetPassword(CLICKHOUSE_PASSWORD)};

    while (run) {
        rd_kafka_message_t *consumer_message;
       
        if (!database_responsive) {
            rd_kafka_message_t *empty_msg = rd_kafka_consumer_poll(consumer, 500);
            if (empty_msg) {
                rd_kafka_message_destroy(empty_msg);
            }
            
            if (reconnect(&client)) {
                std::cout << "Succesfully reconnected to database\n";
                database_responsive = true;
                resume_partitions(consumer);
            }
            else {
                std::cout << "Failed to reconnect to database\n";
                continue;  
            }
        }

        consumer_message = rd_kafka_consumer_poll(consumer, 500);

        if (!consumer_message) {
            continue;
        }

        if (consumer_message->err && consumer_message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            printf("Consumer error: %s\n", rd_kafka_message_errstr(consumer_message));
            return 1;
        } 

        HttpLog log;
        if (get_log(consumer_message->payload, consumer_message->len, &log)) {
            anonymize(log.remoteAddr);
            log_buffer.push_back(log);
        }
        else {
            // TODO: handle messages that could not be decoded due to wrong format of the message
            std::cout << "Incorrect message format\n";
        }

        if (log_buffer.size() >= BATCH_SIZE) {
            if (insert_logs(&client, log_buffer)) {
                std::cout << "Succesful insert to database\n";
                rd_kafka_commit(consumer, NULL, 0);
                log_buffer.clear();
            }
            else {
                database_responsive = false;
                pause_partitions(consumer);
            }
        }

        rd_kafka_message_destroy(consumer_message);
    }

    printf("Closing consumer\n");
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}