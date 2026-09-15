# CanardAbsurd 0.0.1

* Host a DuckDB workflow database over Quack or use an embedded connection.
* Submit idempotent task IDs and execute named R handlers through leased claims.
* Replay native DuckDB values, suspend with durable sleeps, and recover expired leases.
* Preserve R value mappings and allow explicit serialization through BLOBs.
* Reject list and data frame names that collide under DuckDB's ASCII case-insensitive comparison during value admission.
* Document the driver and engine compatibility requirements for VARIANT storage.
* Fence stale task writes and support retry budgets and cancellation.
