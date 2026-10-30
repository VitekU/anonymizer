#pragma once
#include <clickhouse/client.h>
#include <vector>

#include "decoder.h"

bool insert_logs(clickhouse::Client *client, std::vector<HttpLog> logs);

bool reconnect(clickhouse::Client *client);

void setup_database(clickhouse::Client *client);
