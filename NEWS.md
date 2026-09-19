# CanardAbsurd 0.0.1

* Host a DuckDB workflow database over Quack or use an embedded connection.
* Submit idempotent task IDs and execute named R handlers through leased claims.
* Replay native DuckDB values, suspend with durable sleeps, and recover expired leases.
* Preserve R value mappings and allow explicit serialization through BLOBs.
* Reject list and data frame names that collide under DuckDB's ASCII case-insensitive comparison during value admission.
* Document the driver and engine compatibility requirements for VARIANT storage.
* Fence stale task writes and support retry budgets and cancellation.
* Optionally run bounded expired-lease maintenance with a DuckDB C API v1.2 coordinator and an explicit start, status, and stop lifecycle. Development builds load only into databases opened with `allow_unsigned_extensions = TRUE`.
* `ca_work()` retries statements aborted by known write conflicts, with bounded jittered backoff (`conflict_retries`), so competing workers no longer exit on routine contention.
* `ca_process()` supervises an external command in an attempt directory, renewing the lease while it runs and killing it on lease loss, cancellation, or timeout.
* `ca_tasks()` lists task metadata without payloads, and `ca_result()` reads one completed result.
* `ca_close()` releases every owned resource even when one cleanup step fails, reporting failures as `canard_close_error`.
* Stored values that cannot be restored raise `canard_restore_error`, a storage error, instead of being recorded as handler failures.
* In-memory databases and Quack clients now receive the package's DuckDB configuration.
