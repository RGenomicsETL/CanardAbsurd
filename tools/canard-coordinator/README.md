# Canard coordinator extension

This loadable DuckDB extension targets stable C API **v1.2.0** and later. It uses
v1 pending execution to drive a short maintenance mutation from a native thread;
it does not require C API v2.

The extension currently owns one operation: bounded reaping of expired running
tasks that have exhausted `max_failures`. It exposes:

```sql
SELECT ca_coordinator_start(1000, 64); -- poll milliseconds, row limit
SELECT (ca_coordinator_status()).*;
SELECT ca_coordinator_stop();
```

The extension opens its dedicated connection while its load entrypoint still
has a valid borrowed database handle. `ca_coordinator_stop()` interrupts active
database work, joins the thread, and closes that connection. The host must call
it before closing the database. The coordinator cannot restart on that database
handle after the connection closes.

C API v1 can drive the maintenance query, but it does not provide a loaded C
extension with a database-shutdown callback or an owned clone of the host
database handle. The one-shot lifecycle is the narrow compatible contract;
a permanently autonomous loadable-extension lifecycle is not supplied by v1.

The maintenance statement uses `duckdb_pending_prepared()` and repeatedly calls
`duckdb_pending_execute_task()`. It counts a mutation only after
`duckdb_execute_pending()` succeeds. This is the v1 equivalent needed for this
bounded operation; v2's chunked result state machine and structured errors are
not required here.

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
Run the compatibility test with the R `duckdb` 1.5.5 package:

```sh
Rscript test/test-v1.R /absolute/path/to/canard_coordinator.duckdb_extension
```

The test enables unsigned extensions only on its temporary development
connection. Production artifacts need the normal DuckDB extension signing and
distribution path.

This is a maintenance coordinator, not an external-job supervisor. Submission
identity, backend closure, resource reservations, and immutable artifact
publication remain separate contracts.
