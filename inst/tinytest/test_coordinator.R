library(CanardAbsurd)

extension <- Sys.getenv("CANARDABSURD_COORDINATOR_EXTENSION")
if (!nzchar(extension)) {
  exit_file("Native coordinator tests require CANARDABSURD_COORDINATOR_EXTENSION")
}
if (!requireNamespace("callr", quietly = TRUE)) {
  exit_file("Native coordinator lifecycle tests require callr")
}

# Another process can open the file only after its owner released it. DuckDB R
# keeps a database open until failed statements are garbage-collected; see ?ca_close.
reopens <- function(path) {
  gc()
  callr::r(function(path) tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
    DBI::dbDisconnect(con, shutdown = TRUE)
    TRUE
  }, error = function(e) FALSE), list(path), libpath = .libPaths())
}

owner <- function() {
  path <- tempfile(fileext = ".duckdb")
  list(path = path, db = ca_open(path, allow_unsigned_extensions = TRUE))
}

# The public owner path loads a development build and runs one-shot maintenance.
local({
  fixture <- owner()
  db <- fixture$db
  signed_only <- ca_open()
  expect_error(ca_coordinator_start(signed_only, extension), class = "canard_extension_error")
  ca_close(signed_only)
  expect_true(ca_coordinator_start(db, extension, poll_milliseconds = 10, reap_limit = 16))
  expect_false(ca_coordinator_start(db, poll_milliseconds = 10, reap_limit = 16))
  # An expired final attempt in a queue no worker polls. The lease is set in SQL
  # so this test never decodes a stored VARIANT; see test_values.R.
  id <- ca_spawn(db, "work", queue = "unpolled", max_failures = 1)
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks SET state = 'running', attempt = 1,
    worker = 'test-worker', token = uuid(), lease_until = TIMESTAMPTZ '2000-01-01 00:00:00+00'
    WHERE id = ?", params = list(id))
  Sys.sleep(0.3)
  expect_identical(ca_tasks(db, id = id)$state, "failed")
  status <- ca_coordinator_status(db)
  expect_identical(status$state, "running")
  expect_true(status$reaped_count >= 1)
  expect_true(ca_coordinator_stop(db))
  expect_false(ca_coordinator_stop(db))
  expect_identical(ca_coordinator_status(db)$state, "closed")
  expect_error(ca_coordinator_start(db, poll_milliseconds = 10, reap_limit = 16),
    pattern = "cannot restart")
  ca_close(db)
  expect_true(reopens(fixture$path))
})

# Closing releases the file whether the coordinator is running or never started.
local({
  running <- owner()
  ca_coordinator_start(running$db, extension)
  ca_close(running$db)
  expect_true(reopens(running$path))

  failed_start <- owner()
  expect_error(ca_coordinator_start(failed_start$db, extension, reap_limit = 2e6),
    pattern = "reap_limit")
  expect_identical(ca_coordinator_status(failed_start$db)$state, "ready")
  ca_close(failed_start$db)
  expect_true(reopens(failed_start$path))
})

# A failed endpoint shutdown still stops the coordinator and releases the file.
local({
  fixture <- owner()
  db <- fixture$db
  ca_coordinator_start(db, extension)
  db@server <- TRUE
  db@uri <- "quack:127.0.0.1:1"
  expect_error(ca_close(db), class = "canard_close_error")
  expect_false(DBI::dbIsValid(db@con))
  expect_true(reopens(fixture$path))
})

# Bypassing ca_close() pins the database until the owning process exits.
local({
  directory <- tempfile("canard-coordinator-")
  dir.create(directory)
  path <- file.path(directory, "tasks.duckdb")
  bypass <- callr::r_bg(function(path, extension, directory) {
    db <- CanardAbsurd::ca_open(path, allow_unsigned_extensions = TRUE)
    CanardAbsurd::ca_coordinator_start(db, extension)
    DBI::dbDisconnect(db@con, shutdown = TRUE)
    file.create(file.path(directory, "disconnected"))
    while (!file.exists(file.path(directory, "exit"))) Sys.sleep(0.02)
  }, list(path, extension, directory), libpath = .libPaths(), supervise = TRUE)
  on.exit(if (bypass$is_alive()) bypass$kill(), add = TRUE)
  deadline <- proc.time()[["elapsed"]] + 15
  while (!file.exists(file.path(directory, "disconnected")) && bypass$is_alive() &&
      proc.time()[["elapsed"]] < deadline) Sys.sleep(0.02)
  expect_true(file.exists(file.path(directory, "disconnected")))
  expect_false(reopens(path))
  file.create(file.path(directory, "exit"))
  bypass$wait(5000)
  expect_true(reopens(path))
})
