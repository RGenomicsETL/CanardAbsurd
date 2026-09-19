#' Submit a durable task
#'
#' Reusing an ID with the same submission is idempotent, including after
#' completion. A different submission under that ID raises
#' `canard_spawn_conflict`. Supply a stable ID to recover from a lost submission
#' response. Input equality compares the native value and its R type descriptor.
#' A structured constraint failure is followed by an existing-submission lookup,
#' not another write. A match acknowledges the existing task; a different
#' submission raises `canard_spawn_conflict`. Without an existing task the
#' original storage error is retained. If lookup fails, its storage condition
#' also carries the original failure as `submission_error`.
#'
#' @inheritParams ca_close
#' @param name Registered handler name.
#' @param input An R value supported by [ca_values]. No package-imposed byte
#'   limit applies to input, results, checkpoints, or error text.
#' @param queue Queue name.
#' @param id Stable task ID (1 to 256 characters), or `NULL` to generate a UUID.
#' @param priority Integer priority; larger values are claimed first.
#' @param max_failures Number of failures, including expired leases, before a
#'   task becomes terminal. Durable sleeps do not consume this budget. Schema
#'   version 1 accepts values from 1 through 1,000,000.
#' @return The task ID, a character scalar.
#' @export
ca_spawn <- function(db, name, input = NULL, queue = "default", id = NULL,
                     priority = 0L, max_failures = 3L) {
  request <- .ca_input(CanardSubmission, db = db, name = name, input = input,
    queue = queue, id = id, priority = priority, max_failures = max_failures)
  payload <- .ca_payload(request@input, db@con)
  if (is.null(id)) id <- .ca_query(db, "uuid")$id[[1L]]
  params <- list(id = id, queue = request@queue, name = request@name,
    input = payload$value, rtype = payload$rtype, priority = request@priority,
    max_failures = request@max_failures)
  failure <- NULL
  rows <- .ca_query(db, "spawn", params, on_failure = function(error) {
    if (!identical(error$error_type, "CONSTRAINT")) stop(error)
    failure <<- error
    existing <- .ca_query(db, "submission", params, on_failure = function(error) {
      error$submission_error <- failure
      stop(error)
    })
    if (nrow(existing) == 0L) stop(failure)
    existing
  })
  if (nrow(rows) == 0L) rows <- .ca_query(db, "submission", params)
  if (nrow(rows) != 1L || rows$changed[[1L]] != 1L) {
    stop(errorCondition(paste("Task ID already has a different submission:", id),
      class = c("canard_spawn_conflict", "canard_error"), id = id, parent = failure))
  }
  rows$id[[1L]]
}

#' Claim an eligible task
#'
#' Expired leases are recoverable until the failure budget is exhausted.
#' Claims use optimistic DuckDB transactions. Callers can handle
#' `canard_retryable` to retry a conflicting statement; see [ca_conditions()].
#' No database transaction remains open while an R handler executes.
#'
#' @inheritParams ca_spawn
#' @param worker Worker label used for inspection; lease tokens establish ownership.
#' @param lease_seconds Positive lease duration in seconds. Long-running handlers
#'   must explicitly heartbeat before this interval expires.
#' @param task_names Optional character vector of handler names this worker supports.
#' @param reap_limit Maximum exhausted, expired leases to mark failed before each
#'   claim. Batching bounds contention with other workers; it does not limit
#'   recovery of leases that still have failure budget.
#' @return An S7 `CanardTask`, or `NULL` when no eligible task exists. Properties
#'   `id`, `name`, `input`, and `attempt` describe the claim; `db`, `token`, and
#'   `lease_seconds` are used by task operations. Treat claims as process-local.
#' @export
ca_claim <- function(db, queue = "default", worker = paste0("R-", Sys.getpid()),
                     lease_seconds = 30, task_names = NULL, reap_limit = 64L) {
  request <- .ca_input(CanardClaim, db = db, queue = queue, worker = worker,
    lease_seconds = lease_seconds, task_names = task_names, reap_limit = reap_limit)
  claim <- .ca_claimant(request)
  claim()
}

.ca_claimant <- function(request) {
  db <- request@db
  task_names <- DBI::SQL("NULL")
  if (!is.null(request@task_names)) {
    quoted <- DBI::dbQuoteString(db@con, request@task_names)
    task_names <- DBI::SQL(paste0("[", paste(quoted, collapse = ", "), "]"))
  }
  params <- list(queue = request@queue, worker = request@worker,
    lease_seconds = request@lease_seconds, names = task_names)
  reap <- list(queue = request@queue, reap_limit = request@reap_limit)
  function() {
    .ca_query(db, "reap", reap)
    rows <- .ca_query(db, "claim", params)
    if (nrow(rows) == 0L) return(NULL)
    task <- CanardTask(db = db, id = rows$id[[1L]], name = rows$name[[1L]],
      input = NULL, token = rows$token[[1L]], attempt = rows$attempt[[1L]],
      lease_seconds = as.double(request@lease_seconds), seen = new.env(parent = emptyenv()))
    type <- .ca_decoded(.ca_read_type(rows$input_rtype[[1L]]), task@id, "claim")
    task@input <- .ca_read_value(task, "value", type, "input")
    task
  }
}

#' Inspect a task
#'
#' State and checkpoint membership come from one metadata query. A second query
#' retrieves the referenced immutable values; later checkpoints or completion
#' become visible on the next inspection. This relies on mutations using the
#' package protocol rather than modifying stored payloads directly.
#' @inheritParams ca_spawn
#' @return A named list containing task state, counters, timestamps, R input
#'   and result, and checkpoint records; `NULL` for an unknown ID. Step records
#'   contain `kind` and `value`; sleep records contain `kind`.
#' @export
ca_inspect <- function(db, id) {
  request <- .ca_input(CanardLookup, db = db, id = id)
  rows <- .ca_query(request@db, "inspect", list(id = request@id))
  if (nrow(rows) == 0L) return(NULL)

  internal <- c("input_rtype", "result_rtype", "checkpoint_name", "checkpoint_kind",
    "checkpoint_rtype", "checkpoint_ordinal")
  record <- as.list(rows[1L, setdiff(names(rows), internal), drop = FALSE])
  present <- which(!is.na(rows$checkpoint_name))
  rtypes <- c(list(rows$input_rtype[[1L]], rows$result_rtype[[1L]]), rows$checkpoint_rtype[present])
  expressions <- c("input", "result", paste0("map_extract_value(checkpoints, ",
    DBI::dbQuoteString(db@con, rows$checkpoint_name[present]), ").value"))
  types <- .ca_decoded(lapply(rtypes, function(type) {
    if (is.null(type)) list(kind = "NULL", length = 0L) else .ca_read_type(type)
  }), id, "inspect")
  projections <- .ca_decoded(vapply(seq_along(types), function(i) {
    paste0(.ca_projection(expressions[[i]], types[[i]], db@con), " AS v", i)
  }, character(1L)), id, "inspect")
  values <- .ca_query(db, "values", list(id = id,
    projection = DBI::SQL(paste(projections, collapse = ", "))))
  decoded <- .ca_decoded(lapply(seq_along(types), function(i) {
    .ca_restore(values[[i]], types[[i]])
  }), id, "values")
  record[c("input", "result")] <- decoded[1:2]
  checkpoints <- lapply(seq_along(present), function(i) {
    checkpoint <- list(kind = rows$checkpoint_kind[[present[[i]]]])
    if (checkpoint$kind == "step") checkpoint["value"] <- decoded[i + 2L]
    checkpoint
  })
  names(checkpoints) <- rows$checkpoint_name[present]
  record["checkpoints"] <- list(checkpoints)
  record
}

#' List task metadata
#'
#' Reads state, counters, ownership, and timestamps for many tasks in one
#' query, without inputs, results, or checkpoint values. Use it to poll a jobs
#' view; use [ca_result()] or [ca_inspect()] for one task's values.
#'
#' @inheritParams ca_spawn
#' @param queue Optional queue name.
#' @param state Optional character vector of task states: `"ready"`,
#'   `"running"`, `"completed"`, `"failed"`, or `"cancelled"`.
#' @param id Optional character vector of task IDs.
#' @param limit Maximum number of rows, most recently updated first.
#' @return A data frame with one row per task: `id`, `queue`, `name`, `state`,
#'   `priority`, `attempt`, `failures`, `max_failures`, `available_at`, `worker`,
#'   `lease_until`, `error`, `created_at`, and `updated_at`.
#' @export
ca_tasks <- function(db, queue = NULL, state = NULL, id = NULL, limit = 100L) {
  request <- .ca_input(CanardListing, db = db, queue = queue, state = state, id = id,
    limit = limit)
  vector <- function(values) {
    if (is.null(values)) return(DBI::SQL("NULL"))
    DBI::SQL(paste0("[", paste(DBI::dbQuoteString(db@con, values), collapse = ", "), "]"))
  }
  queue <- if (is.null(request@queue)) DBI::SQL("NULL") else request@queue
  .ca_query(request@db, "tasks", list(queue = queue, states = vector(request@state),
    ids = vector(request@id), limit = as.integer(request@limit)))
}

#' Retrieve a completed task's result
#'
#' Reads only the result value, not the input or checkpoints.
#'
#' @inheritParams ca_spawn
#' @return The stored R result. A task that is unknown or not completed raises
#'   `canard_result_error` with its `id` and `state` (`NA` when unknown).
#' @export
ca_result <- function(db, id) {
  request <- .ca_input(CanardLookup, db = db, id = id)
  rows <- .ca_query(request@db, "result", list(id = request@id))
  state <- if (nrow(rows) == 0L) NA_character_ else rows$state[[1L]]
  if (!identical(state, "completed")) {
    stop(errorCondition(paste0("Task ", request@id, " has no result (state: ", state, ")"),
      class = c("canard_result_error", "canard_error"), id = request@id, state = state))
  }
  type <- .ca_decoded(.ca_read_type(rows$result_rtype[[1L]]), request@id, "result")
  projection <- .ca_decoded(.ca_projection("result", type, request@db@con), request@id, "result")
  values <- .ca_query(request@db, "values", list(id = request@id,
    projection = DBI::SQL(paste(projection, "AS value"))))
  .ca_decoded(.ca_restore(values$value, type), request@id, "values")
}

#' Extend a live task lease
#' @param task A claim returned by [ca_claim()] or passed to an R handler.
#' @param seconds Positive duration from the database clock. An existing longer
#'   lease is preserved. An expired lease cannot be revived.
#' @return `NULL`, invisibly. Stale claims raise `canard_lease_lost`.
#' @export
ca_heartbeat <- function(task, seconds = task@lease_seconds) {
  request <- .ca_input(CanardHeartbeat, task = task, seconds = seconds)
  .ca_owned(request@task, "heartbeat", seconds = request@seconds)
  invisible(NULL)
}

#' Finish a claimed task
#' @inheritParams ca_heartbeat
#' @param result An R value supported by [ca_values].
#' @return `NULL`, invisibly. Stale claims raise `canard_lease_lost`.
#' @export
ca_complete <- function(task, result = NULL) {
  request <- .ca_input(CanardCompletion, task = task, result = result)
  .ca_finish(request@task, request@result, return_value = FALSE)
  invisible(NULL)
}

.ca_finish <- function(task, result, return_value = TRUE) {
  payload <- .ca_payload(result, task@db@con)
  if (return_value) {
    return(.ca_read_value(task, "complete", payload$type, "result",
      result = payload$value, rtype = payload$rtype))
  }
  .ca_owned(task, "complete", result = payload$value, rtype = payload$rtype,
    projection = DBI::SQL("NULL::BOOLEAN"))
  invisible(NULL)
}

#' Record a task failure
#' @inheritParams ca_heartbeat
#' @param message Error text, stored without truncation.
#' @param delay_seconds Nonnegative delay before another claim is eligible.
#' @return `NULL`, invisibly. The task becomes ready or terminal according to its
#'   failure budget. Stale claims raise `canard_lease_lost`.
#' @export
ca_fail <- function(task, message, delay_seconds = 0) {
  request <- .ca_input(CanardFailure, task = task, message = message, delay_seconds = delay_seconds)
  .ca_owned(request@task, "fail", message = request@message, delay_seconds = request@delay_seconds)
  invisible(NULL)
}

#' Cancel a nonterminal task
#' @inheritParams ca_spawn
#' @return `TRUE` if cancelled, otherwise `FALSE`. Cancellation fences subsequent
#'   writes but cannot interrupt an external side effect already in progress.
#' @export
ca_cancel <- function(db, id) {
  request <- .ca_input(CanardLookup, db = db, id = id)
  nrow(.ca_query(request@db, "cancel", list(id = request@id))) == 1L
}
