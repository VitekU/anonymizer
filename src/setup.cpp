#include <stdio.h>
#include <err.h>
#include <librdkafka/rdkafka.h>

void set_config(rd_kafka_conf_t *conf, const char *key, const char *value) {
    char errstr[512];

    rd_kafka_conf_res_t res = rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
        err(1, "Unable to set config '%s': %s", key, errstr);
    }
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