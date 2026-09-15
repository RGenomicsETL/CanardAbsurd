library(CanardAbsurd)

# Structured types take precedence over display text, including through a parent.
local({
  classify <- get(".ca_storage_error", asNamespace("CanardAbsurd"))
  cause <- errorCondition("Display wording is not the classification interface",
    class = "duckdb_error", error_type = "TRANSACTION")
  failure <- classify(simpleError("wrapper", call = NULL), "claim", 1L, 0)
  expect_false(inherits(failure, "canard_conflict"))
  wrapper <- errorCondition("wrapper", parent = cause)
  failure <- classify(wrapper, "claim", 2L, 0.1)
  expect_true(inherits(failure, "canard_conflict"))
  expect_false(failure$message_based)
  expect_identical(failure$parent, wrapper)
  expect_identical(failure$attempts, 2L)
  expect_identical(failure$error_type, "TRANSACTION")

  for (type in c("IO", "BINDER", "CONSTRAINT", "UNRECOGNIZED")) {
    cause <- errorCondition("TransactionContext Error: Conflict on update!",
      class = "duckdb_error", error_type = type)
    expect_false(inherits(classify(cause, "spawn", 1L, 0, id = "x"), "canard_conflict"))
  }
})

# The message compatibility path matches complete known diagnostics and the submitted ID.
local({
  classify <- get(".ca_storage_error", asNamespace("CanardAbsurd"))
  messages <- c(
    "TransactionContext Error: Conflict on update!",
    "TransactionContext Error: Conflict on tuple deletion!",
    "Invalid Error: Invalid Input Error: Conflict on update!",
    "Invalid Error: Invalid Input Error: Conflict on tuple deletion!",
    "Invalid Input Error: Conflict on update!",
    "Invalid Input Error: Conflict on tuple deletion!")
  for (message in messages) {
    failure <- classify(simpleError(paste0(message, "\nAdditional driver context")), "claim", 1L, 0)
    expect_true(inherits(failure, "canard_conflict"))
    expect_true(failure$message_based)
  }
  messages <- c(
    'Constraint Error: Duplicate key "id: x" violates primary key constraint.',
    'Invalid Error: Invalid Input Error: Duplicate key "id: x" violates primary key constraint.',
    'TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "x"',
    'Invalid Error: Invalid Input Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "x"')
  for (message in messages) {
    cause <- simpleError(message)
    expect_true(inherits(classify(cause, "spawn", 1L, 0, id = "x"), "canard_conflict"))
    expect_false(inherits(classify(cause, "spawn", 1L, 0, id = "y"), "canard_conflict"))
    expect_false(inherits(classify(cause, "complete", 1L, 0, id = "x"), "canard_conflict"))
  }
  for (message in c("", "Conflict on update!", "IO Error: Conflict on update!",
                    "Invalid Error: Invalid Input Error: Conflict on update! response lost",
                    'Constraint Error: Duplicate key "name: x" violates primary key constraint.')) {
    failure <- classify(simpleError(message), "spawn", 1L, 0, id = "x")
    expect_false(inherits(failure, "canard_conflict"))
    expect_identical(conditionMessage(failure), message)
  }
})

# Constraint failures acknowledge an existing matching submission without another write.
local({
  db <- ca_open()
  withr::defer(ca_close(db))
  ca_spawn(db, "work", 1L, id = "submitted")
  query <- db@query
  writes <- retries <- 0L
  cause <- errorCondition("constraint diagnostic", class = "duckdb_error", error_type = "CONSTRAINT")
  db@query <- function(sql) {
    if (startsWith(sql, "INSERT INTO canard_absurd.tasks")) {
      writes <<- writes + 1L
      stop(cause)
    }
    query(sql)
  }
  withCallingHandlers({
    expect_identical(ca_spawn(db, "work", 1L, id = "submitted"), "submitted")
    different <- tryCatch(ca_spawn(db, "work", 2L, id = "submitted"), error = identity)
    expect_true(inherits(different, "canard_spawn_conflict"))
    expect_identical(different$parent$parent, cause)
    missing <- tryCatch(ca_spawn(db, "work", 1L, id = "missing"), error = identity)
    expect_true(inherits(missing, "canard_storage_error"))
    expect_identical(missing$parent, cause)
  }, canard_retryable = function(event) retries <<- retries + 1L)
  expect_identical(writes, 3L)
  expect_identical(retries, 0L)

  # Errors from caller retry handlers are not submission acknowledgements.
  conflict <- errorCondition("transaction conflict", error_type = "TRANSACTION")
  policy_error <- errorCondition("caller stopped", class = "canard_storage_error",
    error_type = "CONSTRAINT", operation = "spawn")
  db@query <- function(sql) {
    if (startsWith(sql, "INSERT INTO canard_absurd.tasks")) stop(conflict)
    query(sql)
  }
  caught <- tryCatch(withCallingHandlers(ca_spawn(db, "work", 1L, id = "submitted"),
    canard_retryable = function(event) stop(policy_error)), error = identity)
  expect_identical(caught, policy_error)
  db@query <- function(sql) {
    if (startsWith(sql, "INSERT INTO canard_absurd.tasks")) stop(cause)
    stop(conflict)
  }
  caught <- tryCatch(withCallingHandlers(ca_spawn(db, "work", 1L, id = "submitted"),
    canard_retryable = function(event) stop(policy_error)), error = identity)
  expect_identical(caught, policy_error)

  read_error <- simpleError("connection reset during submission lookup")
  db@query <- function(sql) {
    if (startsWith(sql, "INSERT INTO canard_absurd.tasks")) stop(cause)
    stop(read_error)
  }
  failure <- tryCatch(ca_spawn(db, "work", 1L, id = "submitted"), error = identity)
  expect_identical(failure$operation, "submission")
  expect_identical(failure$parent, read_error)
  expect_identical(failure$submission_error$parent, cause)
})
