# CanardAbsurd

Durable R workflows with **DuckDB as the state store and Quack as the database service**. One R package hosts the database and supplies thin R clients.

```text
R server process: DuckDB file + workflow SQL + Quack
                                  ⇅
R client processes: submit → claim → execute → checkpoint
```

The checkpoint-and-replay model is inspired by [Absurd](https://github.com/earendil-works/absurd). Workflow handlers run in client R processes, outside database transactions. No separate message broker is required.

## Install

```sh
R CMD INSTALL .
```

Install Quack explicitly for the DuckDB runtime used by R:

```r
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, "INSTALL quack")
DBI::dbDisconnect(con, shutdown = TRUE)
```

Package loading and client connection do not download extensions. A compatible signed extension file can instead be passed as `extension` to `ca_serve()` or `ca_connect()`.

## Host the database

In an R session that remains alive:

```r
library(CanardAbsurd)

server <- ca_serve(
  "workflows.duckdb",
  uri = "quack:127.0.0.1:9494",
  token = Sys.getenv("CANARD_TOKEN")
)
ca_runtime(server)
# ca_close(server) stops serving and closes the database.
```

Set a nonempty token in both server and client environments. Other processes connect through Quack; they do not open the database file. Use authenticated ingress and TLS for non-local deployments. The token grants trusted database access, not per-queue tenant isolation.

## Submit and execute

In another R session:

```r
library(CanardAbsurd)

db <- ca_connect("quack:127.0.0.1:9494", Sys.getenv("CANARD_TOKEN"))
id <- ca_spawn(db, "calculate", list(x = 21), id = "calculation-001")

handlers <- list(calculate = function(input, ctx) {
  value <- ca_step(ctx, "double", function() input$x * 2)
  ca_sleep(ctx, "pause", seconds = 1)
  value
})

ca_work(db, handlers, max_tasks = 2)
ca_inspect(db, id)$result
# 42
ca_close(db)
```

The first attempt saves `double` and suspends at `pause`. The next attempt replays `double`, passes the saved sleep, and completes. Run multiple client processes for concurrency. `ca_work(..., idle_timeout = 0)` drains currently eligible work; the default waits for more work until interrupted.

`ca_open()` provides the same API against an embedded database for development and local execution. Handles own dedicated, process-local connections; do not wrap package operations in externally managed transactions.

## Guarantees and obligations

- **Atomic claims:** each claim is one server-side update. DuckDB transaction conflicts receive bounded retries.
- **Lease fencing:** checkpoints, heartbeats, completion, failure, and sleep require the current unexpired token. Cancellation invalidates worker writes.
- **Recovery:** expired claims are eligible for another attempt until the failure budget is exhausted. The claim path also reaps exhausted leases.
- **Replay:** named steps reuse saved JSON results. Null values are distinct from missing checkpoints. JSON arrays normalize to R lists on both initial execution and replay.
- **Durable sleep:** the database clock sets the deadline; suspension and its checkpoint commit together. Sleeps do not consume the failure budget.
- **External effects are at-least-once:** a crash after an API call but before checkpointing can repeat that call. Use stable business-level idempotency keys.
- **Explicit heartbeats:** step boundaries renew leases. During long operations call `ca_heartbeat(ctx)` before expiry. No R background thread or automatic process termination is used.
- **Stable workflow code:** names and result shapes are persistent interfaces. Give loop steps explicit indexed names and keep resumable handlers compatible with saved checkpoints.
- **Idempotent submission:** a stable task ID admits only an identical submission. Supply that ID when retrying after an ambiguous network outcome. Deduplication lasts as long as the task row is retained.

## Scope

Version 0.0.1 provides tasks, priorities, named steps, fixed retry delays, failure budgets, cancellation, durable sleep, and inspection. Schema version 1 is checked at open/connect time. Events, cron, automatic retention, task history tables, and cross-version migrations are not implemented. Operators must monitor and manage database growth.

Checkpoints live on the task row so lease changes and checkpoint writes conflict on the same durable record. This trades whole-row JSON updates for a simple ownership invariant. Individual JSON values are limited to 1 MiB; a task's checkpoint document is limited to 16 MiB. This is not a high-throughput capacity claim.

Quack is experimental. `ca_runtime()` reports the server's actual DuckDB, Quack, and schema versions. The initial validation target is DuckDB **v1.5.3** with Quack **1693647**, on Linux x86-64; other pairs require testing.

## Development

```sh
make document
make test
make check
```

Tests are installed under `inst/tinytest/`. They exercise independent R processes against a real Quack server, contended claims, worker death, checkpoint recovery, and stale completions. `s7contract` supplies generative replay and fencing laws. `make test` and `make check` require Quack; an ordinary package check skips remote cases only when the extension is unavailable.

Builds and check output go under `artifacts/`. Run the Tree-sitter anti-slop audit on the R sources and tests before handoff.
