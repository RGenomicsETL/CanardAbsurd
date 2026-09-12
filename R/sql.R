.ca_sql <- new.env(parent = emptyenv())

.ca_query <- function(db, statement, params = list()) {
  force(params)
  started <- proc.time()[["elapsed"]]
  attempt <- 0L
  repeat {
    attempt <- attempt + 1L
    result <- tryCatch({
      if (!exists(statement, envir = .ca_sql, inherits = FALSE)) {
        path <- system.file("sql", paste0(statement, ".sql"),
          package = "CanardAbsurd", mustWork = TRUE)
        .ca_sql[[statement]] <- paste(readLines(path, warn = FALSE), collapse = "\n")
      }
      sql <- DBI::sqlInterpolate(db@con, .ca_sql[[statement]], .dots = params)
      db@query(sql)
    }, error = identity)
    if (!inherits(result, "error")) return(result)

    failure <- .ca_storage_error(result, statement, attempt,
      proc.time()[["elapsed"]] - started, id = params$id)
    if (!inherits(failure, "canard_conflict")) stop(failure)
    notification <- failure
    class(notification) <- c("canard_retryable", "condition")
    withRestarts({
      signalCondition(notification)
      stop(failure)
    }, canard_retry = function() NULL)
  }
}

.ca_json <- function(value) {
  as.character(jsonlite::toJSON(value, auto_unbox = TRUE,
    null = "null", na = "null", digits = NA))
}

.ca_owned <- function(task, statement, ...) {
  rows <- .ca_query(task@db, statement,
    c(list(id = task@id, token = task@token), list(...)))
  if (nrow(rows) == 0L) {
    stop(errorCondition(paste("Task lease is no longer owned:", task@id),
      class = c("canard_lease_lost", "canard_storage_error", "canard_error"),
      id = task@id, attempt = task@attempt, operation = statement))
  }
  if (nrow(rows) != 1L) {
    stop(errorCondition("A task mutation returned more than one row",
      class = c("canard_protocol_error", "canard_storage_error", "canard_error"),
      id = task@id, operation = statement, rows = nrow(rows)))
  }
  rows
}

#' Workflow conditions
#'
#' CanardAbsurd uses R conditions with machine-readable subclasses and fields.
#' Errors inherit from `canard_error` as well as `error` and `condition`.
#'
#' * `canard_input_error` identifies the S7 input constructor in `input_class`
#'   and preserves its property-validation failure in `parent`. Admission happens
#'   before database effects; internal execution uses the admitted input.
#' * `canard_storage_error` preserves the DBI condition in `parent`, with
#'   `operation`, total SQL `attempts`, and `elapsed` seconds. Its subclass
#'   `canard_conflict` denotes a known transaction conflict. Retry policy belongs
#'   to the caller, not the connection. A conflict's `message_based` flag is
#'   `FALSE` when classified from structured DuckDB metadata and `TRUE` when
#'   classified by the compatibility adapter described below.
#' * `canard_lease_lost` and `canard_protocol_error` are storage errors with `id`
#'   and `operation`. Lease loss also carries the claim `attempt`.
#' * `canard_spawn_conflict` carries the conflicting `id`.
#' * `canard_replay_error` carries `id` and checkpoint `name`.
#' * `canard_schema_error` carries the unsupported `version`.
#' * `canard_extension_error` preserves the load failure in `parent`.
#' * `canard_failure_recording_error` is a storage error with the handler error
#'   in `parent` and the failure to record it in `persistence_error`.
#'
#' A known conflict first signals `canard_retryable`, a notification with the
#' same fields as the storage error. A caller's `withCallingHandlers()` handler
#' can wait and invoke the `canard_retry` restart to repeat only that SQL
#' statement. If the handler returns without invoking the restart, or no handler
#' is supplied, the conflict propagates as an error. No default retry schedule
#' or retry limit is imposed. Errors raised by a calling handler propagate to
#' its caller. Other storage failures offer no retry restart.
#'
#' Current DuckDB R rethrows drop structured fields
#'   (<https://github.com/duckdb/duckdb-r/issues/2711>), and released Quack builds
#'   lose the server's error type
#'   (<https://github.com/duckdb/duckdb-quack/pull/212>). Until both preserve that
#'   metadata, a compatibility adapter recognizes exact tested conflict messages
#'   and duplicate-key messages for the submitted ID. This depends on upstream
#'   display wording, not a stable protocol. Unknown forms propagate without a
#'   retry restart. Structured non-transaction errors take precedence over text.
#'   Tests cover installed DuckDB R 1.5.3 and 1.5.5 with their community Quack builds.
#'
#' `canard_suspended` is a control-flow condition, not an error. It carries `id`
#' and `attempt` and unwinds to [ca_run()] after a durable sleep is saved.
#'
#' Unless given an `on_result` callback, [ca_work()] warns on handler failure with
#' `canard_task_failed`, retaining the original condition in `parent` and the
#' complete attempt record in `outcome`. It does not warn on successful retries
#' or durable sleeps. SQL state names describe persisted state, not error types.
#'
#' @name ca_conditions
NULL
