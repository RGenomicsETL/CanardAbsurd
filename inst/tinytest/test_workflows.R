library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Steps preserve JSON nulls, keys, and values across replay.
local({
  db <- local_database()
  ca_spawn(db, "work", id = "replay")
  task <- ca_claim(db)
  payload <- list(null = NULL, nested = list(x = NULL), array = list(1L, NULL, "x"))
  key <- "step.['\"/\u00e9"
  expect_identical(ca_step(task, key, function() payload), payload)
  expect_null(ca_step(task, "null", function() NULL))
  ca_fail(task, "retry after checkpoints")
  task <- ca_claim(db)
  expect_identical(ca_step(task, key, function() stop("must replay")), payload)
  expect_null(ca_step(task, "null", function() stop("must replay null")))
  expect_error(ca_step(task, key, function() NULL), "unique within an attempt")
  ca_complete(task, payload)
  expect_identical(ca_inspect(db, task@id)$result, payload)
})

# Step failures rerun only unfinished work.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  calls <- c(first = 0L, second = 0L)
  handler <- function(input, ctx) {
    x <- ca_step(ctx, "first", function() {
      calls[["first"]] <<- calls[["first"]] + 1L
      40
    })
    y <- ca_step(ctx, "second", function() {
      calls[["second"]] <<- calls[["second"]] + 1L
      if (calls[["second"]] == 1L) stop("transient")
      2
    })
    x + y
  }
  expect_identical(ca_work(db, list(work = handler), idle_timeout = 0), 2L)
  expect_identical(calls, c(first = 1L, second = 2L))
  expect_equal(ca_inspect(db, id)$result, 42)
  expect_identical(ca_inspect(db, id)$failures, 1L)
})

# Durable sleeps release leases and preserve failure budgets.
local({
  db <- local_database()
  id <- ca_spawn(db, "work", max_failures = 1)
  ran_after <- FALSE
  handler <- function(input, ctx) {
    ca_step(ctx, "before", function() 1)
    ca_sleep(ctx, "pause", 3600)
    ran_after <<- TRUE
    42
  }
  task <- ca_claim(db)
  expect_identical(ca_run(task, handler), "suspended")
  expect_false(ran_after)
  expect_identical(ca_inspect(db, id)$state, "ready")
  expect_identical(ca_inspect(db, id)$failures, 0L)
  expect_null(ca_claim(db))
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks SET available_at = current_timestamp")
  expect_identical(ca_run(ca_claim(db), handler), "completed")
  expect_true(ran_after)
  expect_identical(ca_inspect(db, id)$failures, 0L)
  expect_identical(ca_inspect(db, id)$attempt, 2L)
})

# Sleep and value checkpoints have distinct kinds.
local({
  db <- local_database()
  ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "name", function() 1)
  ca_fail(task, "retry")
  expect_error(ca_sleep(ca_claim(db), "name", 0), "Checkpoint kind differs")
})

# Persisted checkpoints survive a database restart.
local({
  path <- tempfile(fileext = ".duckdb")
  withr::defer(unlink(c(path, paste0(path, ".wal"))))
  db <- ca_open(path)
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "saved", function() 42)
  ca_fail(task, "restart")
  ca_close(db)
  db <- local_database(path)
  expect_identical(ca_run(ca_claim(db), function(input, ctx) {
    ca_step(ctx, "saved", function() stop("must replay after restart"))
  }), "completed")
  expect_equal(ca_inspect(db, id)$result, 42)
})

# Lease loss in a handler propagates without failing a newer attempt.
local({
  db <- local_database()
  ca_spawn(db, "work")
  task <- ca_claim(db)
  expect_error(ca_run(task, function(input, ctx) {
    ca_cancel(db, ctx@id)
    ca_step(ctx, "cancelled", function() 1)
  }), class = "canard_lease_lost")
  expect_identical(ca_inspect(db, task@id)$state, "cancelled")
})

# Unsupported result values consume the failure budget immediately.
local({
  db <- local_database()
  id <- ca_spawn(db, "work", max_failures = 1)
  expect_identical(ca_run(ca_claim(db), function(input, ctx) quote(x + y)), "failed")
  expect_identical(ca_inspect(db, id)$state, "failed")
  expect_identical(ca_inspect(db, id)$failures, 1L)
})
