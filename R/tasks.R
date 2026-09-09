CanardTask <- S7::new_class(
  "CanardTask", package = "CanardAbsurd",
  properties = list(
    db = CanardConnection,
    id = S7::class_character,
    name = S7::class_character,
    input = S7::class_any,
    token = S7::class_character,
    attempt = S7::class_integer,
    lease_seconds = S7::class_double,
    seen = S7::class_environment
  )
)

#' Submit a durable task
#'
#' Reusing an ID with the same submission is idempotent, including after
#' completion. A different submission under that ID raises
#' `canard_spawn_conflict`. Supply a stable ID to recover from a lost submission
#' response. JSON encoding defines input equality, including object key order.
#'
#' @inheritParams ca_close
#' @param name Registered handler name.
#' @param input JSON-compatible R value. Values are limited to 1 MiB of JSON.
#' @param queue Queue name.
#' @param id Stable task ID (1 to 256 characters), or `NULL` to generate a UUID.
#' @param priority Integer priority; larger values are claimed first.
#' @param max_failures Number of failures, including expired leases, before a
#'   task becomes terminal. Durable sleeps do not consume this budget.
#' @return The task ID, a character scalar.
#' @export
ca_spawn <- function(db, name, input = NULL, queue = "default", id = NULL,
                     priority = 0L, max_failures = 3L) {
  stopifnot(is.character(name), length(name) == 1L, !is.na(name), nzchar(name))
  stopifnot(is.character(queue), length(queue) == 1L, !is.na(queue), nzchar(queue))
  stopifnot(is.numeric(priority), length(priority) == 1L, is.finite(priority),
    priority == trunc(priority))
  stopifnot(is.numeric(max_failures), length(max_failures) == 1L,
    is.finite(max_failures), max_failures == trunc(max_failures))
  if (is.null(id)) id <- DBI::dbGetQuery(db@con, "SELECT uuid()::VARCHAR AS id")$id[[1L]]
  stopifnot(is.character(id), length(id) == 1L, !is.na(id), nzchar(id))
  params <- list(id = id, queue = queue, name = name,
    input = .ca_json(input), priority = priority, max_failures = max_failures)
  rows <- .ca_query(db, "spawn", params)
  if (nrow(rows) == 0L) rows <- .ca_query(db, "submission", params)
  if (nrow(rows) != 1L) {
    .ca_abort(paste("Task ID already has a different submission:", id),
      "canard_spawn_conflict")
  }
  rows$id[[1L]]
}

#' Claim an eligible task
#'
#' Expired leases are recoverable until the failure budget is exhausted.
#' Claims use optimistic DuckDB transactions with bounded conflict retries.
#' No database transaction remains open while an R handler executes.
#'
#' @inheritParams ca_spawn
#' @param worker Worker label used for inspection; lease tokens establish ownership.
#' @param lease_seconds Positive lease duration in seconds. Long-running handlers
#'   must explicitly heartbeat before this interval expires.
#' @param task_names Optional character vector of handler names this worker supports.
#' @return An S7 `CanardTask`, or `NULL` when no eligible task exists. Properties
#'   `id`, `name`, `input`, and `attempt` describe the claim; `db`, `token`, and
#'   `lease_seconds` are used by task operations. Treat claims as process-local.
#' @export
ca_claim <- function(db, queue = "default", worker = paste0("R-", Sys.getpid()),
                     lease_seconds = 30, task_names = NULL) {
  stopifnot(is.character(queue), length(queue) == 1L, !is.na(queue), nzchar(queue))
  stopifnot(is.character(worker), length(worker) == 1L, !is.na(worker), nzchar(worker))
  stopifnot(is.numeric(lease_seconds), length(lease_seconds) == 1L,
    is.finite(lease_seconds), lease_seconds > 0)
  names_json <- DBI::SQL("NULL")
  if (!is.null(task_names)) {
    stopifnot(is.character(task_names), !anyNA(task_names), all(nzchar(task_names)))
    names_json <- as.character(jsonlite::toJSON(task_names, auto_unbox = FALSE))
  }
  .ca_query(db, "reap", list(queue = queue))
  rows <- .ca_query(db, "claim", list(queue = queue, worker = worker,
    lease_seconds = lease_seconds, names = names_json))
  if (nrow(rows) == 0L) return(NULL)
  CanardTask(db = db, id = rows$id[[1L]], name = rows$name[[1L]],
    input = jsonlite::fromJSON(rows$input[[1L]], simplifyVector = FALSE),
    token = rows$token[[1L]], attempt = rows$attempt[[1L]],
    lease_seconds = as.double(lease_seconds), seen = new.env(parent = emptyenv()))
}

#' Inspect a task
#' @inheritParams ca_spawn
#' @return A named list containing task state, counters, timestamps, and decoded
#'   JSON input, result, and checkpoint records; `NULL` for an unknown ID.
#' @export
ca_inspect <- function(db, id) {
  stopifnot(is.character(id), length(id) == 1L, !is.na(id))
  rows <- .ca_query(db, "inspect", list(id = id))
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
  stopifnot(is.numeric(seconds), length(seconds) == 1L, is.finite(seconds), seconds > 0)
  .ca_owned(task, "heartbeat", seconds = seconds)
  invisible(NULL)
}

#' Finish a claimed task
#' @inheritParams ca_heartbeat
#' @param result JSON-compatible task result.
#' @return `NULL`, invisibly. Stale claims raise `canard_lease_lost`.
#' @export
ca_complete <- function(task, result = NULL) {
  .ca_owned(task, "complete", result = .ca_json(result))
  invisible(NULL)
}

#' Record a task failure
#' @inheritParams ca_heartbeat
#' @param message Error text, at most 8192 characters.
#' @param delay_seconds Nonnegative delay before another claim is eligible.
#' @return `NULL`, invisibly. The task becomes ready or terminal according to its
#'   failure budget. Stale claims raise `canard_lease_lost`.
#' @export
ca_fail <- function(task, message, delay_seconds = 0) {
  stopifnot(is.character(message), length(message) == 1L, !is.na(message))
  stopifnot(nchar(message) <= 8192L)
  stopifnot(is.numeric(delay_seconds), length(delay_seconds) == 1L,
    is.finite(delay_seconds), delay_seconds >= 0)
  .ca_owned(task, "fail", message = message, delay_seconds = delay_seconds)
  invisible(NULL)
}

#' Cancel a nonterminal task
#' @inheritParams ca_spawn
#' @return `TRUE` if cancelled, otherwise `FALSE`. Cancellation fences subsequent
#'   writes but cannot interrupt an external side effect already in progress.
#' @export
ca_cancel <- function(db, id) {
  stopifnot(is.character(id), length(id) == 1L, !is.na(id))
  nrow(.ca_query(db, "cancel", list(id = id))) == 1L
}
