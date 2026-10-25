#pragma once

#include <librdkafka/rdkafka.h>

void set_config(rd_kafka_conf_t *conf, const char *key, const char *value);

