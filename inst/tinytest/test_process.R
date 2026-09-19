library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

if (!requireNamespace("processx", quietly = TRUE)) {
  exit_file("Process supervision tests require processx")
}

rscript <- file.path(R.home("bin"), if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")

# Supervised work outlives several lease periods and publishes attempt-local files.
local({
  db <- local_database()
  root <- tempfile("canard-process-")
  withr::defer(unlink(root, recursive = TRUE))
  id <- ca_spawn(db, "align")
  handler <- function(input, ctx) {
    ca_step(ctx, "align", function() {
      directory <- ca_process(ctx, rscript,
        c("-e", "Sys.sleep(2.5); writeLines('aligned', 'out.txt'); cat('progress')"),
        directory = file.path(root, ctx@id, "align"), heartbeat_seconds = 0.2)
      file.path(directory, "out.txt")
    })
  }
  outcomes <- list()
  ca_work(db, list(align = handler), max_tasks = 1, lease_seconds = 1,
    on_result = function(outcome) outcomes[[1L]] <<- outcome)
  expect_identical(outcomes[[1L]]$status, "completed")
  output <- ca_result(db, id)
  expect_identical(basename(dirname(output)), "attempt-1")
  expect_identical(readLines(output), "aligned")
  expect_identical(readLines(file.path(dirname(output), "stdout.log"), warn = FALSE), "progress")
  record <- ca_tasks(db, id = id)
  expect_identical(c(record$attempt, record$failures), c(1L, 0L))
})

# Cancellation stops the owned process tree and publishes nothing.
local({
  db <- local_database()
  root <- tempfile("canard-process-")
  withr::defer(unlink(root, recursive = TRUE))
  id <- ca_spawn(db, "work")
  task <- ca_claim(db, lease_seconds = 5)
  execute <- db@query
  heartbeats <- 0L
  db@query <- function(sql) {
    if (grepl("SET lease_until", sql, fixed = TRUE) && !grepl("checkpoint", sql, fixed = TRUE)) {
      heartbeats <<- heartbeats + 1L
      if (heartbeats == 3L) ca_cancel(db, id)
    }
    execute(sql)
  }
  task@db <- db
  started <- proc.time()[["elapsed"]]
  error <- tryCatch(ca_step(task, "long", function() {
    ca_process(task, rscript, c("-e", "Sys.sleep(3); writeLines('late', 'late.txt')"),
      directory = root, heartbeat_seconds = 0.2)
  }), error = identity)
  expect_true(inherits(error, "canard_lease_lost"))
  expect_true(proc.time()[["elapsed"]] - started < 3)
  Sys.sleep(3.5)
  expect_false(file.exists(file.path(root, "attempt-1", "late.txt")))
  db@query <- execute
  record <- ca_inspect(db, id)
  expect_identical(record$state, "cancelled")
  expect_length(record$checkpoints, 0L)
})

# Nonzero exits and timeouts are handler failures that keep their logs.
local({
  db <- local_database()
  root <- tempfile("canard-process-")
  withr::defer(unlink(root, recursive = TRUE))
  ca_spawn(db, "work", max_failures = 2)
  outcome <- ca_run(ca_claim(db), function(input, ctx) {
    ca_process(ctx, rscript, c("-e", "message('bad input'); quit(status = 3)"),
      directory = root)
  })
  expect_identical(c(outcome$status, outcome$state), c("failed", "ready"))
  expect_true(inherits(outcome$error, "canard_process_error"))
  expect_identical(outcome$error$status, 3L)
  expect_identical(readLines(file.path(outcome$error$directory, "stderr.log")), "bad input")

  started <- proc.time()[["elapsed"]]
  outcome <- ca_run(ca_claim(db), function(input, ctx) {
    ca_process(ctx, rscript, c("-e", "Sys.sleep(30)"), directory = root, timeout = 0.5)
  })
  expect_true(proc.time()[["elapsed"]] - started < 10)
  expect_identical(outcome$state, "failed")
  expect_identical(outcome$error$status, NA_integer_)
  expect_identical(basename(outcome$error$directory), "attempt-2")
})

# Admission rejects heartbeats that cannot renew the lease in time and reused directories.
local({
  db <- local_database()
  root <- tempfile("canard-process-")
  withr::defer(unlink(root, recursive = TRUE))
  ca_spawn(db, "work")
  task <- ca_claim(db, lease_seconds = 1)
  expect_error(ca_process(task, rscript, directory = root, heartbeat_seconds = 1),
    class = "canard_input_error")
  expect_error(ca_process(task, rscript, NA_character_, directory = root),
    class = "canard_input_error")
  dir.create(file.path(root, "attempt-1"), recursive = TRUE)
  expect_error(ca_process(task, rscript, c("-e", "1"), directory = root), "already exists")
})
