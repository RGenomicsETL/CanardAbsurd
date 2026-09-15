# Compatibility handling for duckdb/duckdb-r#2711 and duckdb/duckdb-quack#212.
# Message forms are fixtures, not a stable upstream contract. Unknown forms fail closed.
.ca_storage_error <- function(error, operation, attempts, elapsed, id = NULL) {
  failure <- errorCondition(conditionMessage(error),
    class = c("canard_storage_error", "canard_error"), operation = operation,
    attempts = attempts, elapsed = elapsed, parent = error)

  native <- error
  while (!is.null(native) && is.null(native$error_type)) native <- native$parent
  if (!is.null(native)) {
    failure$error_type <- native$error_type
    if (!identical(native$error_type, "TRANSACTION")) return(failure)
    failure$message_based <- FALSE
  } else {
    message <- strsplit(conditionMessage(error), "\n", fixed = TRUE)[[1L]][1L]
    prefixes <- c("TransactionContext Error: ", "Invalid Error: Invalid Input Error: ",
      "Invalid Input Error: ")
    expected <- as.vector(outer(prefixes,
      c("Conflict on update!", "Conflict on tuple deletion!"), paste0))
    if (identical(operation, "spawn") && !is.null(id)) {
      key <- paste0('"id: ', id, '"')
      expected <- c(expected, paste0(
        c("Constraint Error: ", "Invalid Error: Invalid Input Error: ", "Invalid Input Error: "),
        "Duplicate key ", key, " violates primary key constraint."),
        paste0(prefixes, 'Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "', id, '"'))
    }
    if (!message %in% expected) return(failure)
    failure$message_based <- TRUE
  }
  class(failure) <- c("canard_conflict", class(failure))
  failure
}
