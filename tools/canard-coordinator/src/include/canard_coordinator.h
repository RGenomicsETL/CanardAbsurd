#ifndef CANARD_COORDINATOR_H
#define CANARD_COORDINATOR_H

#include "duckdb_extension.h"

#include <stdbool.h>

bool canard_coordinator_load(duckdb_connection connection, duckdb_extension_info info,
                             struct duckdb_extension_access *access);

#endif
