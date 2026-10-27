#include <stdio.h>
#include <signal.h>
#include <err.h>
#include <librdkafka/rdkafka.h>
#include <string.h>

#include "http_log.capnp.h"
#include "setup.h"
#include "decoder.h"

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

    if (!setup_consumer(&consumer, errstr)) {
        errx(1, "Failed to create new consumer.");
    }

    if (!subscribe_to_topic(&consumer, TOPIC)) {
        errx(1, "Failed to subscribe to topic %s", TOPIC);
    }

    signal(SIGINT, stop);

    std::vector<HttpLog> log_buffer;
    while (run) {
        rd_kafka_message_t *consumer_message;
        consumer_message = rd_kafka_consumer_poll(consumer, 500);

        if (!consumer_message) {
            printf("Waiting...\n");
            continue;
        }

        if (consumer_message->err) {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {

            } 
            else {
                printf("Consumer error: %s\n", rd_kafka_message_errstr(consumer_message));
                return 1;
            }
        } 
        else {
            HttpLog log;

            if (get_log(consumer_message->payload, consumer_message->len, &log)) {
                log_buffer.push_back(log);
            }
        }

        if (log_buffer.size() >= BATCH_SIZE) {
            rd_kafka_commit(consumer, NULL, 0);
        }

        rd_kafka_message_destroy(consumer_message);
    }

    printf("Closing consumer\n");
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}