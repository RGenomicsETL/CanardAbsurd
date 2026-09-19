library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Generated histories preserve checkpoints and reject retired or expired leases.
if (requireNamespace("s7contract", quietly = TRUE)) local({
  library(s7contract)
  using(s7contract)
  commands <- list(
    new_command("claim",
      generate = function(state) if (state$phase %in% c("ready", "expired")) gen_constant(NULL),
      require = function(state, input) state$phase %in% c("ready", "expired"),
      execute = function(db, input) ca_claim(db, lease_seconds = 3600),
      update = function(state, input, output) {
        state$owner <- output
        state$phase <- "running"
        state$attempt <- state$attempt + 1L
        state$used <- character()
        state
      },
      ensure = function(state, input, output) !is.null(output) &&
        output@id == "protocol" && output@attempt == state$attempt + 1L),
    new_command("step",
      generate = function(state) {
        keys <- setdiff(c("a", "b", "c"), state$used)
        if (state$phase == "running" && length(keys) > 0L) gen_product(
          task = gen_constant(state$owner), key = gen_element(keys),
          value = gen_element(list(NULL, NA_integer_, 1L, c(TRUE, FALSE))))
      },
      require = function(state, input) state$phase == "running" &&
        identical(input$task, state$owner) && !input$key %in% state$used,
      execute = function(db, input) {
        calls <- 0L
        value <- ca_step(input$task, input$key, function() {
          calls <<- calls + 1L
          input$value
        })
        list(value = value, calls = calls)
      },
      update = function(state, input, output) {
        if (!input$key %in% names(state$saved)) state$saved[input$key] <- list(input$value)
        state$used <- c(state$used, input$key)
        state
      },
      ensure = function(state, input, output) {
        cached <- input$key %in% names(state$saved)
        expected <- if (cached) state$saved[[input$key]] else input$value
        identical(output$value, expected) && output$calls == as.integer(!cached)
      }),
    new_command("release",
      generate = function(state) if (state$phase == "running") gen_product(
        task = gen_constant(state$owner), expire = gen_element(c(FALSE, TRUE))),
      require = function(state, input) state$phase == "running" && identical(input$task, state$owner),
      execute = function(db, input) {
        if (input$expire) {
          DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks SET
            lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 - 1) WHERE id = 'protocol'")
        } else ca_fail(input$task, "release")
        TRUE
      },
      update = function(state, input, output) {
        state$old <- c(state$old, list(state$owner))
        state$owner <- NULL
        state$phase <- if (input$expire) "expired" else "ready"
        state
      },
      ensure = function(state, input, output) isTRUE(output)),
    new_command("stale_write",
      generate = function(state) if (length(state$old) > 0L) gen_element(state$old),
      require = function(state, input) any(vapply(state$old, identical, logical(1L), input)),
      execute = function(db, input) {
        inherits(tryCatch(ca_complete(input, 123L), error = identity), "canard_lease_lost")
      },
      ensure = function(state, input, output) isTRUE(output)),
    new_command("inspect",
      generate = function(state) gen_constant(NULL),
      execute = function(db, input) ca_inspect(db, "protocol"),
      ensure = function(state, input, output) {
        phase <- if (state$phase == "expired") "running" else state$phase
        output$state == phase && identical(names(output$checkpoints), names(state$saved)) &&
          all(vapply(names(state$saved), function(key) {
            identical(output$checkpoints[[key]]$value, state$saved[[key]])
          }, logical(1L)))
      })
  )
  law <- new_state_law("lease histories preserve saved values",
    initial = list(phase = "ready", attempt = 0L, used = character(),
      saved = setNames(list(), character()), old = list()),
    commands = commands, max_commands = 16L,
    setup = function() {
      db <- ca_open()
      tryCatch({
        ca_spawn(db, "work", id = "protocol", max_failures = 100L)
        db
      }, error = function(error) {
        ca_close(db)
        stop(error)
      })
    },
    teardown = ca_close,
    classify = function(sequence) unique(vapply(sequence, `[[`, character(1L), "command")),
    min_coverage = c(step = 0.05, stale_write = 0.05))
  expect_law(law, tests = 80L, shrinks = 100L, seed = 31L)
})

# Cancellation and expiry fence value retrieval after claim.
for (event in c("cancel", "expire")) local({
  db <- local_database()
  id <- ca_spawn(db, "work", list(x = NA_integer_))
  query <- db@query
  armed <- TRUE
  db@query <- function(sql) {
    if (armed && grepl("WITH task_values AS MATERIALIZED", sql, fixed = TRUE)) {
      armed <<- FALSE
      if (event == "cancel") ca_cancel(db, id) else DBI::dbExecute(db@con,
        "UPDATE canard_absurd.tasks SET lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 - 1)")
    }
    query(sql)
  }
  expect_error(ca_claim(db), class = "canard_lease_lost")
  record <- ca_inspect(db, id)
  expect_identical(record$state, if (event == "cancel") "cancelled" else "running")
  expect_identical(record$attempt, 1L)
  expect_identical(record$failures, 0L)
})

# A read transport failure leaves the claimed lease without running a handler.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  query <- db@query
  armed <- TRUE
  effects <- 0L
  db@query <- function(sql) {
    if (armed && grepl("WITH task_values AS MATERIALIZED", sql, fixed = TRUE)) {
      armed <<- FALSE
      stop("connection reset during value retrieval")
    }
    query(sql)
  }
  expect_error(ca_work(db, list(work = function(input, task) effects <<- effects + 1L),
    max_tasks = 1L), class = "canard_storage_error")
  record <- ca_inspect(db, id)
  expect_identical(effects, 0L)
  expect_identical(record$state, "running")
  expect_identical(record$attempt, 1L)
  expect_identical(record$failures, 0L)
})

# A retry of the typed replay SELECT does not repeat checkpoint entry or its callback.
local({
  db <- local_database()
  ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "saved", function() c(NA_integer_, 2L))
  ca_fail(task, "resume")
  task <- ca_claim(db)
  query <- db@query
  entries <- reads <- effects <- 0L
  db@query <- function(sql) {
    if (grepl("AS checkpoint_rtype", sql, fixed = TRUE)) entries <<- entries + 1L
    if (grepl("WITH task_values AS MATERIALIZED", sql, fixed = TRUE)) {
      reads <<- reads + 1L
      if (reads == 1L) stop("TransactionContext Error: Conflict on update!")
    }
    query(sql)
  }
  task@db <- db
  value <- withCallingHandlers(ca_step(task, "saved", function() {
    effects <<- effects + 1L
    99L
  }), canard_retryable = function(event) {
    expect_identical(event$operation, "value")
    invokeRestart("canard_retry")
  })
  expect_identical(value, c(NA_integer_, 2L))
  expect_identical(c(entries, reads, effects), c(1L, 2L, 0L))
})

# Inspection retains its metadata snapshot while immutable payloads are retrieved.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "before", function() NA_integer_)
  query <- db@query
  armed <- TRUE
  db@query <- function(sql) {
    if (armed && grepl("WITH task_values AS MATERIALIZED", sql, fixed = TRUE)) {
      armed <<- FALSE
      ca_step(task, "after", function() 2L)
      ca_complete(task, 3L)
    }
    query(sql)
  }
  first <- ca_inspect(db, id)
  expect_identical(first$state, "running")
  expect_identical(first$result, NULL)
  expect_identical(first$checkpoints, list(before = list(kind = "step", value = NA_integer_)))
  second <- ca_inspect(db, id)
  expect_identical(second$state, "completed")
  expect_identical(second$result, 3L)
  expect_identical(names(second$checkpoints), c("before", "after"))
})
