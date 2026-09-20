.ca_sql <- new.env(parent = emptyenv())

.ca_query <- function(db, statement, params = list(), on_failure = stop) {
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
    if (!inherits(failure, "canard_conflict")) return(on_failure(failure))
    notification <- failure
    class(notification) <- c("canard_retryable", "condition")
    withRestarts({
      signalCondition(notification)
      return(on_failure(failure))
    }, canard_retry = function() NULL)
  }
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
#' CanardAbsurd signals R conditions with machine-readable subclasses and
#' fields. Errors inherit from `canard_error`, `error` and `condition`.
#'
#' * `canard_input_error`: invalid arguments, rejected before any database
#'   effect. `input_class` names the S7 constructor and `parent` holds the
#'   validation failure. Its subclass `canard_value_error` reports an
#'   unsupported value; see [ca_values].
#' * `canard_storage_error`: a failed SQL operation, carrying `operation`,
#'   `attempts`, `elapsed` seconds and the DBI condition in `parent`. Its
#'   subclasses are `canard_conflict` (a known write conflict, whose
#'   `message_based` flag is `TRUE` when the compatibility adapter below
#'   classified it), `canard_lease_lost` (the claim is gone; adds `id` and
#'   `attempt`), `canard_restore_error` (a stored value would not decode as its
#'   recorded R type; adds `id`), `canard_failure_recording_error` (a handler
#'   failed and recording it failed too, kept in `parent` and
#'   `persistence_error`), and `canard_protocol_error`.
#' * `canard_spawn_conflict`: the `id` already holds a different submission.
#' * `canard_result_error`: the task is unknown or unfinished; carries `id` and
#'   `state`. See [ca_result()].
#' * `canard_replay_error`: a step name repeated within an attempt, or its kind
#'   changed; carries `id` and `name`.
#' * `canard_process_error`: a command from [ca_process()] failed or timed out;
#'   carries `id`, `status` (`NA` on timeout) and the `directory` holding logs.
#' * `canard_close_error`: [ca_close()] released everything it owns, but some
#'   step failed; `errors` lists them and `parent` is the first.
#' * `canard_schema_error` carries the unsupported `version`,
#'   `canard_extension_error` the load failure in `parent`, and
#'   `canard_coordinator_error` and `canard_connection_error` report a
#'   coordinator called on the wrong or closed handle.
#'
#' Two conditions are notifications rather than errors. `canard_suspended`
#' unwinds to [ca_run()] once [ca_sleep()] has saved a suspension, so an ordinary
#' error handler does not swallow it. `canard_retryable` announces a known write
#' conflict before it is raised: a `withCallingHandlers()` handler may wait and
#' invoke the `canard_retry` restart to repeat that one SQL statement, never an R
#' callback. [ca_work()] does this by default and signals
#' `canard_conflict_retry` (`conflict`, `attempts`, `delay`) before each retry;
#' `conflict_retries = 0` leaves the policy to you. Handlers that return without
#' restarting, and errors they raise, propagate to the caller.
#'
#' Unless given `on_result`, [ca_work()] warns about a handler failure with
#' `canard_task_failed`, keeping the condition in `parent` and the attempt record
#' in `outcome`.
#'
#' Released DuckDB R and Quack builds discard the structured type of some errors
#' (<https://github.com/duckdb/duckdb-r/issues/2711>,
#' <https://github.com/duckdb/duckdb-quack/pull/212>). Until the installed
#' versions carry both fixes, write conflicts are recognized from exact tested
#' message text, which depends on upstream wording rather than a stable protocol.
#' Structured non-transaction errors take precedence over text, and unrecognized
#' messages never become retryable. See `vignette("durability")`.
#'
#' @name ca_conditions
NULL
