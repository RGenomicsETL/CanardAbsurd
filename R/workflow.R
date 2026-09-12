.ca_enter_step <- function(task, name, kind) {
  stopifnot(S7::S7_inherits(task, CanardTask))
  stopifnot(is.character(name), length(name) == 1L, !is.na(name), nzchar(name))
  stopifnot(nchar(name) <= 256L)
  if (exists(name, envir = task@seen, inherits = FALSE)) {
    stop("Step names must be unique within an attempt: ", name, call. = FALSE)
  }
  assign(name, TRUE, envir = task@seen)
  rows <- .ca_owned(task, "heartbeat", seconds = task@lease_seconds)
  checkpoints <- jsonlite::fromJSON(rows$checkpoints[[1L]], simplifyVector = FALSE)
  saved <- checkpoints[[name]]
  if (!is.null(saved) && !identical(saved$kind, kind)) {
    stop("Checkpoint kind differs for step: ", name, call. = FALSE)
  }
  saved
}

#' Execute or replay a named step
#'
#' The callback runs outside a transaction. Its result is checkpointed only after
#' it returns, so external effects must be idempotent. Lease tokens fence database
#' writes, not external services. Use stable business keys for external requests.
#'
#' Step names must be unique in an attempt and stable across deployments. Include
#' an explicit index for repeated steps in a loop. Checkpoint results must remain
#' compatible with handlers that can resume existing tasks. Schema version 1
#' limits the accumulated checkpoint document, including escaped value JSON and
#' record keys, to 16 MiB. There is no additional per-step size limit.
#'
#' @inheritParams ca_heartbeat
#' @param name Stable checkpoint name, at most 256 characters.
#' @param fn Zero-argument function returning a JSON-compatible value.
#' @return The JSON-normalized value, both on initial execution and replay.
#'   JSON arrays become R lists, JSON objects become named lists, and JSON null
#'   becomes `NULL`. A cached `NULL` is distinct from an absent checkpoint.
#' @export
ca_step <- function(task, name, fn) {
  stopifnot(is.function(fn))
  saved <- .ca_enter_step(task, name, "step")
  if (!is.null(saved)) {
    return(jsonlite::fromJSON(saved$json, simplifyVector = FALSE))
  }
  encoded <- .ca_json(fn())
  patch <- .ca_json(stats::setNames(list(list(kind = "step", json = encoded)), name))
  .ca_owned(task, "checkpoint", patch = patch, seconds = task@lease_seconds)
  jsonlite::fromJSON(encoded, simplifyVector = FALSE)
}

#' Suspend a workflow until a database-clock deadline
#'
#' Saves a sleep checkpoint and releases the claim atomically. The worker resumes
#' the handler from its beginning after the deadline, replaying completed steps.
#' The sleep returns immediately when replayed. Code after the sleep does not run
#' in the suspended attempt. Do not swallow the `canard_suspended` condition.
#'
#' @inheritParams ca_step
#' @param seconds Nonnegative sleep duration in seconds.
#' @return `NULL`, invisibly, on replay. The first call signals
#'   `canard_suspended` after persisting the suspension.
#' @export
ca_sleep <- function(task, name, seconds) {
  stopifnot(is.numeric(seconds), length(seconds) == 1L, is.finite(seconds), seconds >= 0)
  saved <- .ca_enter_step(task, name, "sleep")
  if (!is.null(saved)) return(invisible(NULL))
  .ca_owned(task, "sleep", name = name, seconds = seconds)
  .ca_abort(paste("Task suspended:", task@id), "canard_suspended")
}

#' Execute one claimed workflow attempt
#'
#' Calls `handler(task@input, task)`, records its result, or records an R error as
#' a task failure. Interrupts and lease loss propagate to the caller. A failure
#' to persist an outcome also propagates; it is not silently acknowledged.
#'
#' @inheritParams ca_heartbeat
#' @param handler Function taking `(input, ctx)`.
#' @return One of `"completed"`, `"suspended"`, or `"failed"`, invisibly.
#' @export
ca_run <- function(task, handler) {
  stopifnot(S7::S7_inherits(task, CanardTask))
  stopifnot(is.function(handler))
  outcome <- tryCatch(list(json = .ca_json(handler(task@input, task))),
    canard_suspended = function(e) list(suspended = TRUE),
    error = function(e) list(error = e))
  if (isTRUE(outcome$suspended)) return(invisible("suspended"))
  if (!is.null(outcome$error)) {
    if (inherits(outcome$error, "canard_lease_lost")) stop(outcome$error)
    ca_fail(task, substr(conditionMessage(outcome$error), 1L, 8192L))
    return(invisible("failed"))
  }
  .ca_owned(task, "complete", result = outcome$json)
  invisible("completed")
}

#' Pull and execute R tasks
#'
#' Each worker executes one attempt at a time. Scale with independent R processes
#' connected through Quack. Only registered handler names are claimed. Heartbeats
#' occur at step boundaries and through [ca_heartbeat()], not on a background R
#' thread. The worker never terminates its host process on lease loss.
#'
#' @inheritParams ca_claim
#' @param handlers Named list of functions taking `(input, ctx)`.
#' @param max_tasks Maximum number of claimed attempts, including failures and
#'   suspensions. `Inf` runs without an attempt limit.
#' @param poll_seconds Positive delay when no work is found.
#' @param idle_timeout Nonnegative seconds without a claim before returning.
#'   `Inf` waits indefinitely. Zero drains currently eligible work.
#' @return The number of executed attempts, invisibly.
#' @export
ca_work <- function(db, handlers, queue = "default", max_tasks = Inf,
                    poll_seconds = 0.1, idle_timeout = Inf,
                    lease_seconds = 30, worker = paste0("R-", Sys.getpid())) {
  stopifnot(is.list(handlers), length(handlers) > 0L,
    all(vapply(handlers, is.function, logical(1L))))
  stopifnot(length(names(handlers)) == length(handlers),
    !anyNA(names(handlers)), all(nzchar(names(handlers))), !anyDuplicated(names(handlers)))
  stopifnot(is.numeric(max_tasks), length(max_tasks) == 1L, !is.na(max_tasks),
    max_tasks >= 0, max_tasks == floor(max_tasks))
  stopifnot(is.numeric(poll_seconds), length(poll_seconds) == 1L,
    is.finite(poll_seconds), poll_seconds > 0)
  stopifnot(is.numeric(idle_timeout), length(idle_timeout) == 1L,
    !is.na(idle_timeout), idle_timeout >= 0)
  count <- 0L
  idle_since <- proc.time()[["elapsed"]]
  while (count < max_tasks) {
    task <- ca_claim(db, queue, worker, lease_seconds, names(handlers))
    if (is.null(task)) {
      if (proc.time()[["elapsed"]] - idle_since >= idle_timeout) break
      Sys.sleep(poll_seconds)
      next
    }
    ca_run(task, handlers[[task@name]])
    count <- count + 1L
    idle_since <- proc.time()[["elapsed"]]
  }
  invisible(count)
}
