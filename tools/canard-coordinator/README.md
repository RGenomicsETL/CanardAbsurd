# Canard coordinator extension

This loadable DuckDB extension targets stable C API **v1.2.0** and later. It uses
v1 pending execution to drive a short maintenance mutation from a native thread;
it does not require C API v2.

The extension currently owns one operation: bounded reaping of expired running
tasks that have exhausted `max_failures`. It exposes:

```sql
SELECT ca_coordinator_start(1000, 64); -- poll milliseconds, row limit
SELECT unnest(ca_coordinator_status());
SELECT ca_coordinator_stop();
```

Reaping applies the transition in `inst/sql/reap.sql` across all queues: mark
an exhausted task failed, increment its failure count, record the lease-expiry
error, and clear its worker, token, and lease deadline. It does not change
retryable expired tasks, live leases, or terminal tasks.

The extension opens its dedicated connection while its load entrypoint still
has a valid borrowed database handle. `ca_coordinator_stop()` interrupts active
database work, joins the thread, and closes that connection. The host must call
it before closing the database, even if the extension was loaded but never
started. The coordinator cannot restart on that database handle after the
connection closes.

The maintenance statement uses `duckdb_pending_prepared()` and repeatedly calls
`duckdb_pending_execute_task()`. It counts a mutation only after
`duckdb_execute_pending()` succeeds. Status reports both successful and failed
polls in `poll_count`; `last_error` describes the latest poll.

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

The output is under
`build/canard/extension/canard_coordinator/canard_coordinator.duckdb_extension`.

Install this CanardAbsurd checkout before testing so the test uses the package's
schema and task-input encoding, not a substitute table definition:

```sh
R CMD INSTALL /absolute/path/to/CanardAbsurd
```

From `tools/canard-coordinator`, run the compatibility test with the R `duckdb`
1.5.5 package:

```sh
Rscript test/test-v1.R /absolute/path/to/canard_coordinator.duckdb_extension
```

The test checks exhausted tasks in multiple queues, unchanged retryable/live/
terminal tasks, ownership cleanup, argument validation, and the one-shot
lifecycle. Cleanup also runs when an assertion fails. Unsigned extensions are
enabled only on its temporary development connection.

Through the package, load a development build into a database opened with the
explicit, development-only opt-in:

```r
db <- CanardAbsurd::ca_open(path, allow_unsigned_extensions = TRUE)
CanardAbsurd::ca_coordinator_start(db, "/absolute/path/to/canard_coordinator.duckdb_extension")
```

That setting lets any SQL on the database load native code, including every
Quack token holder when the database is served. No signed build is distributed;
deployments without the opt-in cannot load this extension. The package lifecycle
tests, including release of the database file on each close path, run with:

```sh
CANARDABSURD_COORDINATOR_EXTENSION=/absolute/path/to/canard_coordinator.duckdb_extension \
  Rscript -e 'tinytest::run_test_file(system.file("tinytest", "test_coordinator.R", package = "CanardAbsurd"))'
```

This is a maintenance coordinator. It does not dispatch jobs, allocate physical
resources, or execute R handlers.
