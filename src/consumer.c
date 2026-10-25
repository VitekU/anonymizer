#include <stdio.h>
#include <signal.h>
#include <err.h>
#include <librdkafka/rdkafka.h>
#include <string.h>

#include "helper.h"

#define TOPIC "http_log"

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
    (void)sig;
    run = 0;
}

int setup_consumer(rd_kafka_t **consumer, char *errstr) {
    rd_kafka_conf_t *conf;
    conf = rd_kafka_conf_new();
    
    set_config(conf, "bootstrap.servers", "localhost:9092");

    set_config(conf, "group.id",          "c-consumer");
    set_config(conf, "auto.offset.reset", "earliest");
    set_config(conf, "enable.auto.commit", "false");

    *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

    if (!consumer) {
        return 0;
    }

    rd_kafka_poll_set_consumer(*consumer);
    return 1;   
}

int subscribe_to_topic(rd_kafka_t **consumer, const char *topic) {
    rd_kafka_resp_err_t err;

    rd_kafka_topic_partition_list_t *subscription = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, topic, RD_KAFKA_PARTITION_UA);

    err = rd_kafka_subscribe(*consumer, subscription);

    if (err) {
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(*consumer);
        return 0;
    }

    rd_kafka_topic_partition_list_destroy(subscription);
    return 1;
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
            // here i will process the message somehow
            printf("%s\n", (char*)consumer_message->payload);
        }

        rd_kafka_message_destroy(consumer_message);
    }

    printf("Closing consumer\n");
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}