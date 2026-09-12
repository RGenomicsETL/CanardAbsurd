library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Public admission reports its S7 input type and leaves durable state unchanged.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  failures <- list(
    list(message = NA_character_),
    list(message = character()),
    list(message = c("a", "b")),
    list(message = 42),
    list(message = "error", delay_seconds = -1),
    list(message = "error", delay_seconds = Inf),
    list(message = "error", delay_seconds = NA_real_),
    list(message = "error", delay_seconds = c(0, 1))
  )
  Class <- get("CanardFailure", asNamespace("CanardAbsurd"))
  for (arguments in failures) {
    error <- tryCatch(do.call(ca_fail, c(list(task = task), arguments)), error = identity)
    expect_true(inherits(error, "canard_input_error"))
    expect_identical(error$input_class, Class)
    expect_true(inherits(error$parent, "error"))
    expect_identical(ca_inspect(db, id)$failures, 0L)
    expect_identical(ca_inspect(db, id)$state, "running")
  }
  ca_fail(task, "")
  expect_identical(ca_inspect(db, id)$error, "")
})

# Extension loading preserves the underlying path error.
local({
  error <- tryCatch(ca_connect(token = "test-token", extension = tempfile()), error = identity)
  expect_true(inherits(error, "canard_extension_error"))
  expect_true(inherits(error$parent, "error"))
})

# Worker inputs are admitted before claiming, including an empty run.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  invalid <- list(
    list(handlers = list()),
    list(handlers = list(work = 42)),
    list(handlers = list(function(input, ctx) 1)),
    list(handlers = list(work = identity, work = identity)),
    list(handlers = list(work = identity), max_tasks = 0.5),
    list(handlers = list(work = identity), max_tasks = NA_real_),
    list(handlers = list(work = identity), poll_seconds = 0),
    list(handlers = list(work = identity), idle_timeout = NA_real_),
    list(handlers = list(work = identity), failure_delay = -1),
    list(handlers = list(work = identity), on_result = "print")
  )
  Class <- get("CanardWorker", asNamespace("CanardAbsurd"))
  for (arguments in invalid) {
    error <- tryCatch(do.call(ca_work, c(list(db = db), arguments)), error = identity)
    expect_true(inherits(error, "canard_input_error"))
    expect_identical(error$input_class, Class)
    expect_identical(ca_inspect(db, id)$attempt, 0L)
  }
  expect_identical(ca_work(db, list(work = identity), max_tasks = 0,
    idle_timeout = Inf), 0L)
  expect_error(ca_work(db, list(work = identity), max_tasks = 0,
    lease_seconds = 0), class = "canard_input_error")
  expect_identical(ca_inspect(db, id)$attempt, 0L)
})

# Prepared runners retain the selected handler across multiple names and polls.
local({
  db <- local_database()
  first <- ca_spawn(db, "first")
  second <- ca_spawn(db, "second")
  ca_work(db, list(first = function(input, ctx) "one", second = function(input, ctx) "two"),
    max_tasks = 2)
  expect_identical(ca_inspect(db, first)$result, "one")
  expect_identical(ca_inspect(db, second)$result, "two")
})
