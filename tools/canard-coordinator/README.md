# Canard coordinator extension

A loadable DuckDB extension that marks abandoned tasks failed across every
queue, from a native thread in the process that owns the database. Workers
already do this for the queues they poll; the coordinator covers queues nobody
polls. It dispatches no jobs, allocates no resources, and runs no R code.

```sql
SELECT ca_coordinator_start(1000, 64); -- poll milliseconds, rows per poll
SELECT unnest(ca_coordinator_status());
SELECT ca_coordinator_stop();
```

Each poll applies the transition in `inst/sql/reap.sql` to every queue: a
running task whose lease expired with no budget left becomes `failed`, with its
failure count raised, the lease error recorded, and its worker, token and
deadline cleared. Retryable expired tasks, live leases and terminal tasks are
untouched. `poll_count` counts polls whether they succeeded or not, and
`last_error` describes the most recent one, so a running coordinator is not by
itself evidence that maintenance is working.

The extension targets the stable C API v1.2.0 and drives its statement with
`duckdb_pending_prepared()` and `duckdb_pending_execute_task()`, so it needs no
C API v2. It opens its own connection while loading, when its borrowed database
handle is still valid. `ca_coordinator_stop()` interrupts that connection, joins
the thread and closes it; the host must call it before closing the database,
even if the coordinator never started. It cannot start again on that handle.

## Build against DuckDB 1.5.5

From a DuckDB 1.5.5 source checkout:

```sh
cmake -S . -B build/canard \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHELL=OFF \
  -DBUILD_UNITTESTS=OFF \
  -DDUCKDB_EXTENSION_CONFIGS=/absolute/path/to/extension_config.cmake
cmake --build build/canard --target canard_coordinator_loadable_extension
```

The result is `build/canard/extension/canard_coordinator/canard_coordinator.duckdb_extension`.

## Test

Install this checkout first, so the tests use the package's own schema and value
encoding rather than a substitute table:

```sh
R CMD INSTALL /absolute/path/to/CanardAbsurd
```

The C API compatibility test covers exhausted tasks in several queues,
untouched retryable, live and terminal tasks, ownership cleanup, argument
validation and the one-shot lifecycle, cleaning up even when an assertion
fails. It enables unsigned extensions only on its own temporary connection.

```sh
Rscript test/test-v1.R /absolute/path/to/canard_coordinator.duckdb_extension
```

The package lifecycle tests additionally check that each way of closing releases
the database file:

```sh
CANARDABSURD_COORDINATOR_EXTENSION=/absolute/path/to/canard_coordinator.duckdb_extension \
  Rscript -e 'tinytest::run_test_file(system.file("tinytest", "test_coordinator.R", package = "CanardAbsurd"))'
```

## Loading a local build

No signed build is distributed, so a local one loads only into a database opened
with the development-only opt-in:

```r
db <- CanardAbsurd::ca_open(path, allow_unsigned_extensions = TRUE)
CanardAbsurd::ca_coordinator_start(db, "/absolute/path/to/canard_coordinator.duckdb_extension")
```

That setting lets any SQL on the database load native code, including from every
Quack token holder once the database is served. Deployments without it cannot
load this extension.
