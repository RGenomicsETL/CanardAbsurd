#' Submit a durable task
#'
#' Reusing an ID with the same submission is idempotent, including after
#' completion. A different submission under that ID raises
#' `canard_spawn_conflict`. Supply a stable ID to recover from a lost submission
#' response. JSON encoding defines input equality, including object key order.
#'
#' @inheritParams ca_close
#' @param name Registered handler name.
#' @param input JSON-compatible R value. Schema version 1 limits each input to
#'   1 MiB of UTF-8 JSON and the accumulated checkpoint document to 16 MiB.
#'   These server-side limits also apply to direct SQL clients.
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
  if (is.null(id)) id <- .ca_query(db, "uuid")$id[[1L]]
  params <- list(id = id, queue = request@queue, name = request@name,
    input = .ca_json(request@input), priority = request@priority,
    max_failures = request@max_failures)
  rows <- .ca_query(db, "spawn", params)
  if (nrow(rows) == 0L) rows <- .ca_query(db, "submission", params)
  if (nrow(rows) != 1L) {
    stop(errorCondition(paste("Task ID already has a different submission:", id),
      class = c("canard_spawn_conflict", "canard_error"), id = id))
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
  names_json <- DBI::SQL("NULL")
  if (!is.null(request@task_names)) {
    names_json <- as.character(jsonlite::toJSON(request@task_names, auto_unbox = FALSE))
  }
  params <- list(queue = request@queue, worker = request@worker,
    lease_seconds = request@lease_seconds, names = names_json)
  reap <- list(queue = request@queue, reap_limit = request@reap_limit)
  function() {
    .ca_query(db, "reap", reap)
    rows <- .ca_query(db, "claim", params)
    if (nrow(rows) == 0L) return(NULL)
    CanardTask(db = db, id = rows$id[[1L]], name = rows$name[[1L]],
      input = jsonlite::fromJSON(rows$input[[1L]], simplifyVector = FALSE),
      token = rows$token[[1L]], attempt = rows$attempt[[1L]],
      lease_seconds = as.double(request@lease_seconds), seen = new.env(parent = emptyenv()))
  }
}

#' Inspect a task
#' @inheritParams ca_spawn
#' @return A named list containing task state, counters, timestamps, and decoded
#'   JSON input, result, and checkpoint records; `NULL` for an unknown ID.
#' @export
ca_inspect <- function(db, id) {
  request <- .ca_input(CanardLookup, db = db, id = id)
  rows <- .ca_query(request@db, "inspect", list(id = request@id))
  if (nrow(rows) == 0L) return(NULL)
  record <- as.list(rows[1L, , drop = FALSE])
  for (field in c("input", "result", "checkpoints")) {
    value <- record[[field]]
    record[field] <- list(if (is.na(value)) NULL else
      jsonlite::fromJSON(value, simplifyVector = FALSE))
  }
  record
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
#' @param result JSON-compatible task result.
#' @return `NULL`, invisibly. Stale claims raise `canard_lease_lost`.
#' @export
ca_complete <- function(task, result = NULL) {
  request <- .ca_input(CanardCompletion, task = task, result = result)
  .ca_owned(request@task, "complete", result = .ca_json(request@result))
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
