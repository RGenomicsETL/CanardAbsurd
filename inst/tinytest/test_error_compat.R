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
