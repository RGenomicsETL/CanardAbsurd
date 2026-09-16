#include "canard_coordinator.h"
#include "canard_thread.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CANARD_ERROR_CAPACITY 1024
#define CANARD_MAX_POLL_MILLISECONDS 86400000ULL
#define CANARD_MAX_REAP_LIMIT 1000000ULL

extern duckdb_ext_api_v1 duckdb_ext_api;

/* Same transition as inst/sql/reap.sql, applied across all queues. */
static const char *CANARD_REAP_SQL =
    "UPDATE canard_absurd.tasks\n"
    "SET state = 'failed', failures = failures + 1,\n"
    "    error = 'worker lease expired',\n"
    "    worker = NULL, token = NULL, lease_until = NULL,\n"
    "    updated_at = current_timestamp\n"
    "WHERE id IN (\n"
    "    SELECT id FROM canard_absurd.tasks\n"
    "    WHERE state = 'running'\n"
    "        AND lease_until <= current_timestamp AND failures + 1 >= max_failures\n"
    "        AND (SELECT version FROM canard_absurd.schema_version) = 1\n"
    "    ORDER BY lease_until, id LIMIT ?\n"
    ")\n"
    "RETURNING 1 AS changed, id;";

typedef enum {
	CANARD_COORDINATOR_READY = 0,
	CANARD_COORDINATOR_RUNNING = 1,
	CANARD_COORDINATOR_STOPPING = 2,
	CANARD_COORDINATOR_CLOSED = 3
} canard_coordinator_state;

typedef struct {
	canard_mutex mutex;
	canard_condition condition;
	canard_thread thread;
	duckdb_connection connection;
	canard_coordinator_state state;
	uint64_t poll_milliseconds;
	uint64_t reap_limit;
	uint64_t poll_count;
	uint64_t reaped_count;
	uint64_t references;
	bool stop_requested;
	char last_error[CANARD_ERROR_CAPACITY];
} canard_runtime;

typedef struct {
	canard_coordinator_state state;
	uint64_t poll_milliseconds;
	uint64_t reap_limit;
	uint64_t poll_count;
	uint64_t reaped_count;
	char last_error[CANARD_ERROR_CAPACITY];
} canard_status;

static const char *canard_state_name(canard_coordinator_state state) {
	switch (state) {
	case CANARD_COORDINATOR_RUNNING:
		return "running";
	case CANARD_COORDINATOR_STOPPING:
		return "stopping";
	case CANARD_COORDINATOR_CLOSED:
		return "closed";
	default:
		return "ready";
	}
}

static void canard_copy_error(char *target, const char *message) {
	if (!message) {
		message = "DuckDB operation failed without an error message";
	}
	(void)snprintf(target, CANARD_ERROR_CAPACITY, "%s", message);
}

static void canard_record_poll(canard_runtime *runtime, uint64_t reaped, const char *error) {
	canard_mutex_lock(&runtime->mutex);
	runtime->poll_count++;
	runtime->reaped_count += reaped;
	if (error) {
		canard_copy_error(runtime->last_error, error);
	} else {
		runtime->last_error[0] = '\0';
	}
	canard_mutex_unlock(&runtime->mutex);
}

static bool canard_stop_requested(canard_runtime *runtime) {
	bool requested;
	canard_mutex_lock(&runtime->mutex);
	requested = runtime->stop_requested;
	canard_mutex_unlock(&runtime->mutex);
	return requested;
}

static bool canard_wait(canard_runtime *runtime, uint64_t milliseconds) {
	bool requested;
	canard_mutex_lock(&runtime->mutex);
	if (!runtime->stop_requested) {
		canard_condition_wait_milliseconds(&runtime->condition, &runtime->mutex, milliseconds);
	}
	requested = runtime->stop_requested;
	canard_mutex_unlock(&runtime->mutex);
	return requested;
}

static duckdb_state canard_prepare_reaper(canard_runtime *runtime, duckdb_prepared_statement *statement,
                                          char *error) {
	if (duckdb_prepare(runtime->connection, CANARD_REAP_SQL, statement) == DuckDBError) {
		canard_copy_error(error, duckdb_prepare_error(*statement));
		duckdb_destroy_prepare(statement);
		return DuckDBError;
	}
	if (duckdb_bind_uint64(*statement, 1, runtime->reap_limit) == DuckDBError) {
		canard_copy_error(error, "Unable to bind the coordinator reap limit");
		duckdb_destroy_prepare(statement);
		return DuckDBError;
	}
	return DuckDBSuccess;
}

static duckdb_state canard_execute_reaper(canard_runtime *runtime, duckdb_prepared_statement statement,
                                          uint64_t *reaped, char *error) {
	duckdb_pending_result pending = NULL;
	duckdb_result result;
	memset(&result, 0, sizeof(result));
	if (duckdb_pending_prepared(statement, &pending) == DuckDBError) {
		canard_copy_error(error, duckdb_pending_error(pending));
		duckdb_destroy_pending(&pending);
		return DuckDBError;
	}

	duckdb_pending_state pending_state = DUCKDB_PENDING_RESULT_NOT_READY;
	while (!duckdb_pending_execution_is_finished(pending_state)) {
		pending_state = duckdb_pending_execute_task(pending);
		if (pending_state == DUCKDB_PENDING_NO_TASKS_AVAILABLE && canard_wait(runtime, 1)) {
			duckdb_interrupt(runtime->connection);
		}
		if (canard_stop_requested(runtime)) {
			duckdb_interrupt(runtime->connection);
		}
	}
	if (pending_state == DUCKDB_PENDING_ERROR) {
		canard_copy_error(error, duckdb_pending_error(pending));
		duckdb_destroy_pending(&pending);
		return DuckDBError;
	}
	if (duckdb_execute_pending(pending, &result) == DuckDBError) {
		canard_copy_error(error, duckdb_result_error(&result));
		duckdb_destroy_result(&result);
		duckdb_destroy_pending(&pending);
		return DuckDBError;
	}

	duckdb_data_chunk chunk;
	while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
		*reaped += (uint64_t)duckdb_data_chunk_get_size(chunk);
		duckdb_destroy_data_chunk(&chunk);
	}
	duckdb_destroy_result(&result);
	duckdb_destroy_pending(&pending);
	return DuckDBSuccess;
}

static void canard_coordinator_main(void *argument) {
	canard_runtime *runtime = (canard_runtime *)argument;
	duckdb_prepared_statement statement = NULL;
	while (!canard_stop_requested(runtime)) {
		char error[CANARD_ERROR_CAPACITY] = {0};
		uint64_t reaped = 0;
		duckdb_state poll_result = DuckDBSuccess;
		if (!statement) {
			poll_result = canard_prepare_reaper(runtime, &statement, error);
		}
		if (poll_result == DuckDBSuccess) {
			poll_result = canard_execute_reaper(runtime, statement, &reaped, error);
		}
		if (poll_result == DuckDBError) {
			duckdb_destroy_prepare(&statement);
			if (!canard_stop_requested(runtime)) {
				canard_record_poll(runtime, 0, error);
			}
		} else {
			canard_record_poll(runtime, reaped, NULL);
		}
		if (canard_wait(runtime, runtime->poll_milliseconds)) {
			break;
		}
	}
	duckdb_destroy_prepare(&statement);
}

static bool canard_runtime_start(canard_runtime *runtime, uint64_t poll_milliseconds, uint64_t reap_limit,
                                 char *error) {
	canard_mutex_lock(&runtime->mutex);
	while (runtime->state == CANARD_COORDINATOR_STOPPING) {
		canard_condition_wait(&runtime->condition, &runtime->mutex);
	}
	if (runtime->state == CANARD_COORDINATOR_RUNNING) {
		canard_mutex_unlock(&runtime->mutex);
		return false;
	}
	if (runtime->state == CANARD_COORDINATOR_CLOSED) {
		canard_copy_error(error, "A C API v1 coordinator cannot restart after its owned connection is closed");
		canard_mutex_unlock(&runtime->mutex);
		return false;
	}

	runtime->poll_milliseconds = poll_milliseconds;
	runtime->reap_limit = reap_limit;
	runtime->stop_requested = false;
	runtime->state = CANARD_COORDINATOR_RUNNING;
	if (!canard_thread_start(&runtime->thread, canard_coordinator_main, runtime)) {
		runtime->state = CANARD_COORDINATOR_READY;
		canard_copy_error(error, "Unable to create the coordinator thread");
		canard_mutex_unlock(&runtime->mutex);
		return false;
	}
	runtime->last_error[0] = '\0';
	canard_mutex_unlock(&runtime->mutex);
	return true;
}

static bool canard_runtime_stop(canard_runtime *runtime) {
	canard_thread thread;
	duckdb_connection connection;
	canard_mutex_lock(&runtime->mutex);
	while (runtime->state == CANARD_COORDINATOR_STOPPING) {
		canard_condition_wait(&runtime->condition, &runtime->mutex);
	}
	if (runtime->state == CANARD_COORDINATOR_CLOSED) {
		canard_mutex_unlock(&runtime->mutex);
		return false;
	}
	if (runtime->state == CANARD_COORDINATOR_READY) {
		connection = runtime->connection;
		runtime->connection = NULL;
		runtime->state = CANARD_COORDINATOR_CLOSED;
		canard_mutex_unlock(&runtime->mutex);
		duckdb_disconnect(&connection);
		return true;
	}
	runtime->state = CANARD_COORDINATOR_STOPPING;
	runtime->stop_requested = true;
	thread = runtime->thread;
	connection = runtime->connection;
	canard_condition_broadcast(&runtime->condition);
	canard_mutex_unlock(&runtime->mutex);

	duckdb_interrupt(connection);
	canard_thread_join(thread);
	duckdb_disconnect(&connection);

	canard_mutex_lock(&runtime->mutex);
	runtime->connection = NULL;
	runtime->state = CANARD_COORDINATOR_CLOSED;
	canard_condition_broadcast(&runtime->condition);
	canard_mutex_unlock(&runtime->mutex);
	return true;
}

static void canard_runtime_retain(canard_runtime *runtime) {
	canard_mutex_lock(&runtime->mutex);
	runtime->references++;
	canard_mutex_unlock(&runtime->mutex);
}

static void canard_runtime_release(void *data) {
	canard_runtime *runtime = (canard_runtime *)data;
	bool destroy;
	canard_mutex_lock(&runtime->mutex);
	runtime->references--;
	destroy = runtime->references == 0;
	canard_mutex_unlock(&runtime->mutex);
	if (!destroy) {
		return;
	}
	(void)canard_runtime_stop(runtime);
	canard_condition_destroy(&runtime->condition);
	canard_mutex_destroy(&runtime->mutex);
	free(runtime);
}

static void canard_status_read(canard_runtime *runtime, canard_status *status) {
	canard_mutex_lock(&runtime->mutex);
	status->state = runtime->state;
	status->poll_milliseconds = runtime->poll_milliseconds;
	status->reap_limit = runtime->reap_limit;
	status->poll_count = runtime->poll_count;
	status->reaped_count = runtime->reaped_count;
	canard_copy_error(status->last_error, runtime->last_error);
	canard_mutex_unlock(&runtime->mutex);
}

static bool canard_valid_control_call(duckdb_function_info info, duckdb_data_chunk input, idx_t expected_columns) {
	if (duckdb_data_chunk_get_column_count(input) != expected_columns || duckdb_data_chunk_get_size(input) != 1) {
		duckdb_scalar_function_set_error(info, "Coordinator control functions require one scalar row");
		return false;
	}
	for (idx_t column = 0; column < expected_columns; column++) {
		duckdb_vector vector = duckdb_data_chunk_get_vector(input, column);
		uint64_t *validity = duckdb_vector_get_validity(vector);
		if (validity && !duckdb_validity_row_is_valid(validity, 0)) {
			duckdb_scalar_function_set_error(info, "Coordinator control arguments cannot be NULL");
			return false;
		}
	}
	return true;
}

static void canard_start_function(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	if (!canard_valid_control_call(info, input, 2)) {
		return;
	}
	duckdb_vector poll_vector = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector limit_vector = duckdb_data_chunk_get_vector(input, 1);
	uint64_t poll_milliseconds = ((uint64_t *)duckdb_vector_get_data(poll_vector))[0];
	uint64_t reap_limit = ((uint64_t *)duckdb_vector_get_data(limit_vector))[0];
	if (poll_milliseconds < 1 || poll_milliseconds > CANARD_MAX_POLL_MILLISECONDS) {
		duckdb_scalar_function_set_error(info, "poll_milliseconds must be between 1 and 86400000");
		return;
	}
	if (reap_limit < 1 || reap_limit > CANARD_MAX_REAP_LIMIT) {
		duckdb_scalar_function_set_error(info, "reap_limit must be between 1 and 1000000");
		return;
	}
	char error[CANARD_ERROR_CAPACITY] = {0};
	canard_runtime *runtime = (canard_runtime *)duckdb_scalar_function_get_extra_info(info);
	bool started = canard_runtime_start(runtime, poll_milliseconds, reap_limit, error);
	if (error[0]) {
		duckdb_scalar_function_set_error(info, error);
		return;
	}
	((bool *)duckdb_vector_get_data(output))[0] = started;
}

static void canard_stop_function(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	if (!canard_valid_control_call(info, input, 0)) {
		return;
	}
	canard_runtime *runtime = (canard_runtime *)duckdb_scalar_function_get_extra_info(info);
	((bool *)duckdb_vector_get_data(output))[0] = canard_runtime_stop(runtime);
}

static void canard_status_function(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	canard_runtime *runtime = (canard_runtime *)duckdb_scalar_function_get_extra_info(info);
	canard_status status;
	canard_status_read(runtime, &status);
	idx_t count = duckdb_data_chunk_get_size(input);
	duckdb_vector state_vector = duckdb_struct_vector_get_child(output, 0);
	bool *running = (bool *)duckdb_vector_get_data(duckdb_struct_vector_get_child(output, 1));
	uint64_t *poll_milliseconds =
	    (uint64_t *)duckdb_vector_get_data(duckdb_struct_vector_get_child(output, 2));
	uint64_t *reap_limit = (uint64_t *)duckdb_vector_get_data(duckdb_struct_vector_get_child(output, 3));
	uint64_t *poll_count = (uint64_t *)duckdb_vector_get_data(duckdb_struct_vector_get_child(output, 4));
	uint64_t *reaped_count = (uint64_t *)duckdb_vector_get_data(duckdb_struct_vector_get_child(output, 5));
	duckdb_vector error_vector = duckdb_struct_vector_get_child(output, 6);
	for (idx_t row = 0; row < count; row++) {
		duckdb_vector_assign_string_element(state_vector, row, canard_state_name(status.state));
		running[row] = status.state == CANARD_COORDINATOR_RUNNING;
		poll_milliseconds[row] = status.poll_milliseconds;
		reap_limit[row] = status.reap_limit;
		poll_count[row] = status.poll_count;
		reaped_count[row] = status.reaped_count;
		duckdb_vector_assign_string_element(error_vector, row, status.last_error);
	}
}

static bool canard_register_start(duckdb_connection connection, canard_runtime *runtime) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_logical_type unsigned_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
	duckdb_logical_type boolean_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
	duckdb_scalar_function_set_name(function, "ca_coordinator_start");
	duckdb_scalar_function_add_parameter(function, unsigned_type);
	duckdb_scalar_function_add_parameter(function, unsigned_type);
	duckdb_scalar_function_set_return_type(function, boolean_type);
	duckdb_scalar_function_set_volatile(function);
	/* Let the callback reject NULL instead of silently returning SQL NULL. */
	duckdb_scalar_function_set_special_handling(function);
	canard_runtime_retain(runtime);
	duckdb_scalar_function_set_extra_info(function, runtime, canard_runtime_release);
	duckdb_scalar_function_set_function(function, canard_start_function);
	duckdb_state state = duckdb_register_scalar_function(connection, function);
	duckdb_destroy_logical_type(&unsigned_type);
	duckdb_destroy_logical_type(&boolean_type);
	duckdb_destroy_scalar_function(&function);
	return state == DuckDBSuccess;
}

static bool canard_register_stop(duckdb_connection connection, canard_runtime *runtime) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_logical_type boolean_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
	duckdb_scalar_function_set_name(function, "ca_coordinator_stop");
	duckdb_scalar_function_set_return_type(function, boolean_type);
	duckdb_scalar_function_set_volatile(function);
	canard_runtime_retain(runtime);
	duckdb_scalar_function_set_extra_info(function, runtime, canard_runtime_release);
	duckdb_scalar_function_set_function(function, canard_stop_function);
	duckdb_state state = duckdb_register_scalar_function(connection, function);
	duckdb_destroy_logical_type(&boolean_type);
	duckdb_destroy_scalar_function(&function);
	return state == DuckDBSuccess;
}

static bool canard_register_status(duckdb_connection connection, canard_runtime *runtime) {
	const char *names[] = {"state", "running", "poll_milliseconds", "reap_limit", "poll_count", "reaped_count",
	                       "last_error"};
	duckdb_logical_type members[] = {
	    duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR), duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN),
	    duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT), duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT),
	    duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT), duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT),
	    duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};
	duckdb_logical_type status_type = duckdb_create_struct_type(members, names, 7);
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "ca_coordinator_status");
	duckdb_scalar_function_set_return_type(function, status_type);
	duckdb_scalar_function_set_volatile(function);
	canard_runtime_retain(runtime);
	duckdb_scalar_function_set_extra_info(function, runtime, canard_runtime_release);
	duckdb_scalar_function_set_function(function, canard_status_function);
	duckdb_state state = duckdb_register_scalar_function(connection, function);
	for (idx_t index = 0; index < 7; index++) {
		duckdb_destroy_logical_type(&members[index]);
	}
	duckdb_destroy_logical_type(&status_type);
	duckdb_destroy_scalar_function(&function);
	return state == DuckDBSuccess;
}

bool canard_coordinator_load(duckdb_connection connection, duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
	canard_runtime *runtime = (canard_runtime *)calloc(1, sizeof(canard_runtime));
	if (!runtime) {
		access->set_error(info, "Unable to allocate the Canard coordinator runtime");
		return false;
	}
	if (!canard_mutex_init(&runtime->mutex)) {
		free(runtime);
		access->set_error(info, "Unable to initialize the Canard coordinator mutex");
		return false;
	}
	if (!canard_condition_init(&runtime->condition)) {
		canard_mutex_destroy(&runtime->mutex);
		free(runtime);
		access->set_error(info, "Unable to initialize the Canard coordinator condition variable");
		return false;
	}
	duckdb_database *database = access->get_database(info);
	if (duckdb_connect(*database, &runtime->connection) == DuckDBError) {
		canard_condition_destroy(&runtime->condition);
		canard_mutex_destroy(&runtime->mutex);
		free(runtime);
		access->set_error(info, "Unable to open the Canard coordinator connection");
		return false;
	}
	runtime->state = CANARD_COORDINATOR_READY;
	runtime->references = 1;

	bool registered = canard_register_start(connection, runtime) && canard_register_stop(connection, runtime) &&
	                  canard_register_status(connection, runtime);
	if (!registered) {
		/* Earlier registrations can still retain the runtime after LOAD fails. */
		(void)canard_runtime_stop(runtime);
		access->set_error(info, "Unable to register the Canard coordinator SQL functions");
	}
	canard_runtime_release(runtime);
	return registered;
}
