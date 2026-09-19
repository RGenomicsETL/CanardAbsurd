# Reproducible CanardAbsurd measurements, one factor at a time from a baseline.
#
#   Rscript tools/benchmark.R local|quack OUTPUT.csv
#
# "local" runs one in-process worker against ca_open(); "quack" runs a server
# process and independent worker processes. Metadata about the revision,
# runtimes and hardware is written next to the CSV as OUTPUT.csv.meta.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L || !args[[1L]] %in% c("local", "quack")) {
  stop("Usage: Rscript tools/benchmark.R local|quack OUTPUT.csv")
}
transport <- args[[1L]]
output <- args[[2L]]
library(CanardAbsurd)
for (package in c("callr", "parallelly", "ps")) {
  if (!requireNamespace(package, quietly = TRUE)) stop("The benchmark requires ", package)
}
dir.create(dirname(output), recursive = TRUE, showWarnings = FALSE)
root <- tempfile("canard-benchmark-")
dir.create(root)
token <- "benchmark-token"

# Each scenario gets a fresh database file, served over Quack when requested.
open_fixture <- function() {
  path <- tempfile(tmpdir = root, fileext = ".duckdb")
  if (transport == "local") return(list(db = ca_open(path), path = path, close = NULL))
  uri <- sprintf("quack:127.0.0.1:%d", parallelly::freePort())
  ready <- paste0(path, ".ready")
  server <- callr::r_bg(function(path, uri, token, ready) {
    db <- CanardAbsurd::ca_serve(path, uri, token = token)
    on.exit(CanardAbsurd::ca_close(db))
    file.create(ready)
    while (file.exists(ready)) Sys.sleep(0.05)
  }, list(path, uri, token, ready), libpath = .libPaths(), supervise = TRUE)
  while (!file.exists(ready)) {
    if (!server$is_alive()) server$get_result()
    Sys.sleep(0.05)
  }
  list(db = ca_connect(uri, token), path = path, uri = uri,
    close = function() {
      unlink(ready)
      server$wait(10000)
    })
}

close_fixture <- function(fixture) {
  ca_close(fixture$db)
  if (!is.null(fixture$close)) fixture$close()
  file.size(fixture$path)
}

# The handler sleeps for the callback duration, then saves `checkpoints` steps.
handler <- function(callback_seconds, checkpoints, checkpoint_doubles) {
  force(callback_seconds)
  function(input, ctx) {
    if (callback_seconds > 0) Sys.sleep(callback_seconds)
    for (i in seq_len(checkpoints)) {
      CanardAbsurd::ca_step(ctx, paste0("step-", i), function() runif(checkpoint_doubles))
    }
    TRUE
  }
}

# Workers start together after connecting; `started` is the shared start time.
run_workers <- function(fixture, workers, work) {
  if (transport == "local") {
    retries <- 0L
    started <- Sys.time()
    count <- withCallingHandlers(ca_work(fixture$db, list(work = work), idle_timeout = 0),
      canard_conflict_retry = function(retry) retries <<- retries + 1L)
    rss <- ps::ps_memory_info(ps::ps_handle())[["rss"]]
    return(list(count = count, retries = retries, exits = 0L, rss = rss, started = started))
  }
  barrier <- tempfile("barrier-", tmpdir = root)
  dir.create(barrier)
  processes <- lapply(seq_len(workers), function(i) callr::r_bg(function(uri, token, work, barrier, i) {
    db <- CanardAbsurd::ca_connect(uri, token)
    on.exit(CanardAbsurd::ca_close(db))
    file.create(file.path(barrier, i))
    while (!file.exists(file.path(barrier, "go"))) Sys.sleep(0.005)
    retries <- 0L
    count <- withCallingHandlers(
      CanardAbsurd::ca_work(db, list(work = work), idle_timeout = 1, poll_seconds = 0.02),
      canard_conflict_retry = function(retry) retries <<- retries + 1L)
    list(count = count, retries = retries, rss = ps::ps_memory_info(ps::ps_handle())[["rss"]])
  }, list(fixture$uri, token, work, barrier, i), libpath = .libPaths(), supervise = TRUE))
  while (!all(file.exists(file.path(barrier, seq_len(workers))))) {
    if (!all(vapply(processes, function(process) process$is_alive(), logical(1L)))) break
    Sys.sleep(0.01)
  }
  started <- Sys.time()
  file.create(file.path(barrier, "go"))
  for (process in processes) process$wait(600000)
  results <- lapply(processes, function(process) tryCatch(process$get_result(), error = identity))
  exited <- vapply(results, inherits, logical(1L), "error")
  finished <- results[!exited]
  list(count = sum(vapply(finished, `[[`, integer(1L), "count")),
    retries = sum(vapply(finished, `[[`, integer(1L), "retries")),
    exits = sum(exited), rss = max(0, vapply(finished, `[[`, numeric(1L), "rss")),
    started = started)
}

measure <- function(scenario, workers = 1L, tasks = 40L, callback_seconds = 0,
                    checkpoints = 1L, checkpoint_doubles = 10L, payload_rows = 0L,
                    history_rows = 0L) {
  if (transport == "local") workers <- 1L
  fixture <- open_fixture()
  if (history_rows > 0L) {
    # Completed rows encoded like a NULL submission and result, inserted in bulk.
    empty <- getFromNamespace(".ca_payload", "CanardAbsurd")(NULL, fixture$db@con)
    sql <- sprintf("INSERT INTO canard_absurd.tasks
      (id, queue, name, input, input_rtype, max_failures, state, attempt, result, result_rtype)
      SELECT 'history-' || i, 'default', 'work', %1$s, %2$s, 1, 'completed', 1, %1$s, %2$s
      FROM range(%3$d) AS series(i)", empty$value, empty$rtype, history_rows)
    if (transport == "local") DBI::dbExecute(fixture$db@con, sql) else fixture$db@query(sql)
  }
  input <- if (payload_rows > 0L) {
    data.frame(id = seq_len(payload_rows), x = runif(payload_rows))
  }
  submitting <- proc.time()[["elapsed"]]
  for (i in seq_len(tasks)) ca_spawn(fixture$db, "work", input, id = paste0("task-", i))
  spawned <- proc.time()[["elapsed"]]
  result <- run_workers(fixture, workers, handler(callback_seconds, checkpoints, checkpoint_doubles))
  # Elapsed runs from the shared start to the last completion on the database
  # clock, excluding worker start-up and idle shutdown. Both clocks are this host's.
  done <- ca_tasks(fixture$db, state = "completed", id = paste0("task-", seq_len(tasks)),
    limit = tasks)
  completed <- nrow(done)
  bytes <- close_fixture(fixture)
  elapsed <- as.numeric(difftime(max(done$updated_at), result$started, units = "secs"))
  data.frame(scenario, transport, workers, tasks, completed, callback_seconds, checkpoints,
    checkpoint_bytes = 8L * checkpoint_doubles, payload_rows, history_rows,
    spawn_ms_per_task = 1000 * (spawned - submitting) / tasks,
    elapsed_seconds = elapsed, tasks_per_second = completed / elapsed,
    overhead_ms_per_task = 1000 * (elapsed * workers - tasks * callback_seconds) / tasks,
    conflict_retries = result$retries, worker_exits = result$exits,
    max_worker_rss_mb = result$rss / 2^20, database_mb = bytes / 2^20)
}

scenarios <- list(
  quote(measure("baseline")),
  quote(measure("workers", workers = 2L)),
  quote(measure("workers", workers = 4L)),
  quote(measure("workers", workers = 8L)),
  quote(measure("callback", callback_seconds = 0.05, workers = 4L)),
  quote(measure("checkpoints", tasks = 5L, checkpoints = 50L)),
  quote(measure("checkpoints", tasks = 5L, checkpoints = 200L)),
  quote(measure("checkpoint-size", tasks = 5L, checkpoints = 20L, checkpoint_doubles = 10000L)),
  quote(measure("payload", payload_rows = 10000L)),
  quote(measure("history", history_rows = 100000L))
)
rows <- lapply(scenarios, function(scenario) {
  message("Running ", deparse(scenario))
  eval(scenario)
})
utils::write.csv(do.call(rbind, rows), output, row.names = FALSE)

runtime_db <- ca_open()
runtime <- ca_runtime(runtime_db)
ca_close(runtime_db)
cpu <- if (file.exists("/proc/cpuinfo")) {
  sub(".*: ", "", grep("^model name", readLines("/proc/cpuinfo"), value = TRUE)[1L])
} else {
  Sys.info()[["machine"]]
}
git <- function(...) {
  tryCatch(system2("git", c(...), stdout = TRUE, stderr = FALSE),
    error = function(e) NA_character_, warning = function(w) NA_character_)
}
revision <- git("rev-parse", "HEAD")
if (length(git("status", "--porcelain", "--untracked-files=no")) > 0L) {
  revision <- paste(revision, "with uncommitted changes")
}
writeLines(c(
  paste("date:", format(Sys.time(), tz = "UTC", usetz = TRUE)),
  paste("revision:", revision),
  paste("CanardAbsurd:", packageVersion("CanardAbsurd")),
  paste("R:", R.version.string),
  paste("duckdb R:", packageVersion("duckdb"), "engine", runtime$duckdb_version),
  paste("transport:", transport),
  paste("os:", utils::osVersion),
  paste("cpu:", cpu, "cores", parallel::detectCores())
), paste0(output, ".meta"))
unlink(root, recursive = TRUE)
message("Wrote ", output)
