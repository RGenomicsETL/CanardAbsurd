library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Calling handlers own retry limits and delays; unhandled conflicts propagate.
local({
  db <- local_database()
  execute <- db@query
  cause <- simpleError("TransactionContext Error: Conflict on update!")
  calls <- 0L
  db@query <- function(sql) {
    calls <<- calls + 1L
    stop(cause)
  }
  error <- tryCatch(ca_runtime(db), error = identity)
  expect_identical(calls, 1L)
  expect_true(inherits(error, "canard_conflict"))
  expect_identical(error$parent, cause)
  expect_identical(error$attempts, 1L)
  expect_identical(error$operation, "runtime")

  calls <- 0L
  events <- list()
  error <- withCallingHandlers(tryCatch(ca_runtime(db), error = identity),
    canard_retryable = function(event) {
      events[[length(events) + 1L]] <<- event
      if (event$attempts < 3L) invokeRestart("canard_retry")
    })
  expect_identical(error$attempts, 3L)
  expect_identical(calls, 3L)
  expect_length(events, 3L)
  expect_identical(vapply(events, function(event) event$attempts, integer(1L)), 1:3)
  expect_identical(events[[1L]]$parent, cause)
  expect_true(error$elapsed >= 0)

  calls <- 0L
  events <- list()
  db@query <- function(sql) {
    calls <<- calls + 1L
    if (calls <= 2L) stop(cause)
    execute(sql)
  }
  runtime <- withCallingHandlers(ca_runtime(db), canard_retryable = function(event) {
    events[[length(events) + 1L]] <<- event
    invokeRestart("canard_retry")
  })
  expect_identical(runtime$schema_version, 1L)
  expect_identical(calls, 3L)
  expect_length(events, 2L)

  cause <- simpleError("IO Error: response lost")
  calls <- 0L
  db@query <- function(sql) {
    calls <<- calls + 1L
    stop(cause)
  }
  error <- tryCatch(ca_runtime(db), error = identity)
  expect_true(inherits(error, "canard_storage_error"))
  expect_false(inherits(error, "canard_conflict"))
  expect_identical(error$parent, cause)
  expect_identical(calls, 1L)
  offered <- FALSE
  withCallingHandlers(tryCatch(ca_runtime(db), error = identity),
    canard_retryable = function(event) offered <<- TRUE)
  expect_false(offered)
})

# A retry restart repeats a checkpoint write, not the R callback.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  execute <- db@query
  writes <- 0L
  effects <- 0L
  db@query <- function(sql) {
    if (grepl("SET checkpoints =", sql, fixed = TRUE)) {
      writes <<- writes + 1L
      if (writes == 1L) stop("TransactionContext Error: Conflict on update!")
    }
    execute(sql)
  }
  task@db <- db
  outcome <- withCallingHandlers(ca_run(task, function(input, ctx) {
    ca_step(ctx, "saved", function() {
      effects <<- effects + 1L
      42
    })
  }), canard_retryable = function(conflict) {
    expect_identical(conflict$operation, "checkpoint")
    invokeRestart("canard_retry")
  })
  expect_identical(outcome$status, "completed")
  expect_identical(writes, 2L)
  expect_identical(effects, 1L)
  expect_identical(ca_inspect(db, id)$result, 42L)
})

# Caller retry-handler errors propagate without consuming the task failure budget.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  execute <- db@query
  db@query <- function(sql) stop("TransactionContext Error: Conflict on update!")
  task@db <- db
  cause <- simpleError("retry observer failed")
  error <- tryCatch(withCallingHandlers(ca_run(task, function(input, ctx) 42),
    canard_retryable = function(conflict) stop(cause)), error = identity)
  expect_identical(error, cause)
  db@query <- execute
  expect_identical(ca_inspect(db, id)$failures, 0L)
})

# Conflict-looking text within a transport diagnostic does not offer a restart.
local({
  db <- local_database()
  db@query <- function(sql) stop("IO Error: response lost\nConflict on update!")
  offered <- FALSE
  error <- withCallingHandlers(tryCatch(ca_runtime(db), error = identity),
    canard_retryable = function(conflict) offered <<- TRUE)
  expect_false(offered)
  expect_true(inherits(error, "canard_storage_error"))
  expect_false(inherits(error, "canard_conflict"))
})

# Errors before and after each persistence operation propagate without failing the handler.
local({
  for (operation in c("enter", "checkpoint", "sleep", "complete")) {
    for (committed in c(FALSE, TRUE)) {
      db <- local_database()
      id <- ca_spawn(db, "work")
      task <- ca_claim(db)
      execute <- db@query
      cause <- simpleError(paste("IO Error: acknowledgement lost at", operation))
      calls <- 0L
      target <- if (operation %in% c("checkpoint", "sleep")) 2L else 1L
      effects <- 0L
      db@query <- function(sql) {
        calls <<- calls + 1L
        if (calls != target) return(execute(sql))
        if (committed) execute(sql)
        stop(cause)
      }
      handler <- switch(operation,
        enter = function(input, ctx) ca_step(ctx, "value", function() {
          effects <<- effects + 1L
          42
        }),
        checkpoint = function(input, ctx) ca_step(ctx, "value", function() 42),
        sleep = function(input, ctx) ca_sleep(ctx, "pause", 0),
        complete = function(input, ctx) 42)
      task@db <- db
      error <- tryCatch(ca_run(task, handler), error = identity)
      expect_true(inherits(error, "canard_storage_error"))
      expect_identical(error$parent, cause)
      expect_identical(error$operation, operation)
      expect_identical(error$attempts, 1L)
      expect_identical(calls, target)
      db@query <- execute
      record <- ca_inspect(db, id)
      expect_identical(record$failures, 0L)
      state <- if (committed && operation == "sleep") "ready" else
        if (committed && operation == "complete") "completed" else "running"
      expect_identical(record$state, state)
      if (operation == "enter") expect_identical(effects, 0L)
      if (operation == "checkpoint") expect_identical(length(record$checkpoints), as.integer(committed))
      ca_close(db)
    }
  }
})

# Handler errors retain their full condition and diagnostic text.
local({
  db <- local_database()
  id <- ca_spawn(db, "work", max_failures = 2)
  cause <- errorCondition(paste0(strrep("x", 10000L), " ROOT_CAUSE"),
    class = "application_error", detail = list(code = 42L), parent = simpleError("origin"))
  outcome <- ca_run(ca_claim(db), function(input, ctx) stop(cause), failure_delay = 60)
  expect_identical(outcome$error, cause)
  expect_identical(outcome$status, "failed")
  expect_identical(outcome$state, "ready")
  expect_identical(outcome$id, id)
  expect_identical(outcome$attempt, 1L)
  expect_identical(ca_inspect(db, id)$error, conditionMessage(cause))
  expect_null(ca_claim(db))
  DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks SET available_at = current_timestamp")
  outcome <- ca_run(ca_claim(db), function(input, ctx) stop(cause))
  expect_identical(outcome$state, "failed")
  expect_identical(outcome$error, cause)
})

# A failure to persist a handler error retains both causes, even after a commit.
local({
  for (committed in c(FALSE, TRUE)) {
    db <- local_database()
    id <- ca_spawn(db, "work")
    task <- ca_claim(db)
    execute <- db@query
    handler_error <- simpleError("handler failed")
    storage_error <- simpleError("IO Error: response lost")
    db@query <- function(sql) {
      if (committed) execute(sql)
      stop(storage_error)
    }
    task@db <- db
    error <- tryCatch(ca_run(task, function(input, ctx) stop(handler_error)), error = identity)
    expect_true(inherits(error, "canard_failure_recording_error"))
    expect_identical(error$parent, handler_error)
    expect_identical(error$persistence_error$parent, storage_error)
    db@query <- execute
    expect_identical(ca_inspect(db, id)$failures, as.integer(committed))
    ca_close(db)
  }
})

# Ordinary error handlers do not swallow suspension, and interrupts propagate.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  after <- FALSE
  outcome <- ca_run(ca_claim(db), function(input, ctx) {
    tryCatch(ca_sleep(ctx, "pause", 0), error = function(e) NULL)
    after <<- TRUE
  })
  expect_identical(outcome$status, "suspended")
  expect_false(after)
  task <- ca_claim(db)
  interrupt <- structure(list(message = "interrupted", call = NULL),
    class = c("interrupt", "condition"))
  caught <- tryCatch(ca_run(task, function(input, ctx) stop(interrupt)),
    interrupt = identity)
  expect_identical(caught, interrupt)
  expect_identical(ca_inspect(db, id)$failures, 0L)
})

# Worker observers run after persistence and their errors do not fail the task.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  expect_error(ca_work(db, list(work = function(input, ctx) 42), max_tasks = 1,
    on_result = function(outcome) stop("observer failed")), "observer failed")
  expect_identical(ca_inspect(db, id)$state, "completed")
  expect_identical(ca_inspect(db, id)$failures, 0L)

  id <- ca_spawn(db, "work", max_failures = 1)
  cause <- simpleError("handler failed")
  notice <- NULL
  withCallingHandlers(ca_work(db, list(work = function(input, ctx) stop(cause)), max_tasks = 1),
    canard_task_failed = function(w) {
      notice <<- w
      invokeRestart("muffleWarning")
    })
  expect_identical(notice$parent, cause)
  expect_identical(notice$outcome$state, "failed")
  expect_identical(notice$outcome$id, id)
})
