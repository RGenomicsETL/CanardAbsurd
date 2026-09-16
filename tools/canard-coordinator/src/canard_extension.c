#include "canard_coordinator.h"

duckdb_ext_api_v1 duckdb_ext_api = {0};

bool canard_coordinator_init_c_api(duckdb_extension_info info, struct duckdb_extension_access *access) {
	DUCKDB_EXTENSION_API_INIT(info, access, DUCKDB_EXTENSION_API_VERSION_STRING);
	duckdb_database *database = access->get_database(info);
	duckdb_connection connection;
	if (duckdb_connect(*database, &connection) == DuckDBError) {
		access->set_error(info, "Unable to open a DuckDB connection while loading the Canard coordinator");
		return false;
	}
	bool loaded = canard_coordinator_load(connection, info, access);
	duckdb_disconnect(&connection);
	return loaded;
}
