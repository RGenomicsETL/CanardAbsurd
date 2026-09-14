library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Submission IDs are idempotent and preserve completed state.
local({
  db <- local_database()
  id <- "invoice:'\"/unicode-\u00e9"
  input <- list(message = "' ; DROP SCHEMA canard_absurd CASCADE; --", x = NULL)
  expect_identical(ca_spawn(db, "work", input, id = id), id)
  expect_identical(ca_spawn(db, "work", input, id = id), id)
  expect_error(ca_spawn(db, "other", input, id = id), class = "canard_spawn_conflict")
  expect_error(ca_spawn(db, "work", list(x = 1), id = id), class = "canard_spawn_conflict")
  task <- ca_claim(db)
  expect_identical(task@input, input)
  ca_complete(task, list(value = NULL))
  expect_identical(ca_spawn(db, "work", input, id = id), id)
  expect_identical(ca_inspect(db, id)$state, "completed")
  expect_identical(ca_inspect(db, id)$result, list(value = NULL))
  expect_null(ca_inspect(db, "absent"))
})

# Claims respect queue, priority, and registered handler names.
local({
  db <- local_database()
  ca_spawn(db, "unsupported", id = "unregistered", priority = 100)
  ca_spawn(db, "work", id = "other-queue", queue = "elsewhere", priority = 100)
  ca_spawn(db, "work", id = "low")
  ca_spawn(db, "work", id = "high", priority = 10)
  ca_spawn(db, "quoted'name", id = "quoted", priority = 5)
  expect_identical(ca_work(db, list(work = function(input, ctx) ctx@id),
    max_tasks = 1), 1L)
  expect_identical(ca_inspect(db, "high")$state, "completed")
  expect_identical(ca_inspect(db, "unregistered")$attempt, 0L)
  task <- ca_claim(db, task_names = "quoted'name")
  expect_identical(task@id, "quoted")
  ca_complete(task)
  task <- ca_claim(db, task_names = "work")
  expect_identical(task@id, "low")
  expect_null(ca_claim(db, task_names = "work"))
  ca_complete(task)
  expect_false(ca_cancel(db, "low"))
})

# Failures, retry delays, and lease expiry exhaust the failure budget.
local({
  db <- local_database()
  ca_spawn(db, "work", id = "retry", max_failures = 2)
  task <- ca_claim(db)
  ca_fail(task, "first", delay_seconds = 100)
  expect_null(ca_claim(db))
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks SET available_at = current_timestamp")
  task <- ca_claim(db)
  expect_identical(task@attempt, 2L)
  ca_fail(task, "last")
  expect_identical(ca_inspect(db, "retry")$state, "failed")
  expect_identical(ca_inspect(db, "retry")$failures, 2L)
  expect_false(ca_cancel(db, "retry"))

  ca_spawn(db, "work", id = "expired", max_failures = 1)
  task <- ca_claim(db)
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks
    SET lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 - 1) WHERE id = 'expired'")
  expect_error(ca_heartbeat(task), class = "canard_lease_lost")
  expect_null(ca_claim(db))
  expect_identical(ca_inspect(db, "expired")$state, "failed")
  expect_identical(ca_inspect(db, "expired")$failures, 1L)
})

# Current unexpired tokens fence every worker mutation.
local({
  db <- local_database()
  ca_spawn(db, "work", id = "fenced")
  old <- ca_claim(db)
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks
    SET lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 - 1) WHERE id = 'fenced'")
  expect_error(ca_complete(old, "stale"), class = "canard_lease_lost")
  current <- ca_claim(db)
  expect_false(identical(current@token, old@token))
  expect_identical(current@attempt, 2L)
  expect_error(ca_heartbeat(old), class = "canard_lease_lost")
  expect_error(ca_fail(old, "stale"), class = "canard_lease_lost")
  expect_error(ca_sleep(old, "late-sleep", 0), class = "canard_lease_lost")
  expect_error(ca_step(old, "late-step", function() stop("not called")),
    class = "canard_lease_lost")
  expect_true(ca_cancel(db, current@id))
  expect_false(ca_cancel(db, current@id))
  expect_error(ca_complete(current), class = "canard_lease_lost")
  expect_null(ca_claim(db))
})

# A callback cannot checkpoint after cancellation.
local({
  db <- local_database()
  ca_spawn(db, "work", id = "cancel-during-step")
  task <- ca_claim(db)
  expect_error(ca_step(task, "effect", function() {
    ca_cancel(db, task@id)
    1
  }), class = "canard_lease_lost")
  expect_length(ca_inspect(db, task@id)$checkpoints, 0L)
})

# Invalid admission and unsupported schema fail explicitly.
local({
  db <- local_database()
  expect_error(ca_spawn(db, character(), NULL))
  expect_error(ca_spawn(db, "work", max_failures = 0))
  expect_error(ca_claim(db, lease_seconds = 0))
  expect_error(ca_claim(db, lease_seconds = Inf))
  expect_error(ca_work(db, list(function(input, ctx) NULL), idle_timeout = 0))
  expect_error(ca_work(db, list(x = identity), max_tasks = -1))
  expect_identical(ca_work(db, list(x = identity), max_tasks = 0), 0L)
  expect_identical(ca_runtime(db)$schema_version, 1L)
  ca_close(db)
  expect_null(ca_close(db))
  expect_error(ca_runtime(db))

  path <- tempfile(fileext = ".duckdb")
  withr::defer(unlink(c(path, paste0(path, ".wal"))))
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  DBI::dbExecute(con, "CREATE SCHEMA canard_absurd")
  DBI::dbExecute(con, "CREATE TABLE canard_absurd.schema_version(version INTEGER)")
  DBI::dbExecute(con, "INSERT INTO canard_absurd.schema_version VALUES (2)")
  DBI::dbDisconnect(con, shutdown = TRUE)
  expect_error(ca_open(path), "Unsupported CanardAbsurd schema version")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  expect_identical(DBI::dbGetQuery(con,
    "SELECT version FROM canard_absurd.schema_version")$version, 2L)
  DBI::dbDisconnect(con, shutdown = TRUE)
})
