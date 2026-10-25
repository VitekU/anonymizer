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
