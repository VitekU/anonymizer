#pragma once

#include <librdkafka/rdkafka.h>

void set_config(rd_kafka_conf_t *conf, const char *key, const char *value);

int setup_consumer(rd_kafka_t **consumer, char *errstr);
int subscribe_to_topic(rd_kafka_t **consumer, const char *topic);
void pause_partitions(rd_kafka_t *consumer);
void resume_partitions(rd_kafka_t *consumer);

