.ca_enter_step <- function(task, name, kind) {
  if (exists(name, envir = task@seen, inherits = FALSE)) {
    stop(errorCondition(paste("Step names must be unique within an attempt:", name),
      class = c("canard_replay_error", "canard_error"), id = task@id, name = name))
  }
  rows <- .ca_owned(task, "enter", name = name, seconds = task@lease_seconds)
  saved_kind <- rows$checkpoint_kind[[1L]]
  saved <- if (is.na(saved_kind)) NULL else list(kind = saved_kind)
  if (!is.null(saved) && !is.null(rows$checkpoint_rtype[[1L]])) {
    saved$type <- .ca_read_type(rows$checkpoint_rtype[[1L]])
  }
  if (!is.null(saved) && !identical(saved$kind, kind)) {
    stop(errorCondition(paste("Checkpoint kind differs for step:", name),
      class = c("canard_replay_error", "canard_error"),
      id = task@id, name = name, expected = kind, actual = saved$kind))
  }
  assign(name, TRUE, envir = task@seen)
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
#' compatible with handlers that can resume existing tasks. The package imposes
#' no per-step or cumulative checkpoint byte limit.
#'
#' @inheritParams ca_heartbeat
#' @param name Stable checkpoint name.
#' @param fn Zero-argument function returning a value supported by [ca_values].
#' @return The stored R value, both on initial execution and replay. Timestamps
#'   and durations have DuckDB microsecond precision. A cached `NULL` is distinct
#'   from an absent checkpoint.
#' @export
ca_step <- function(task, name, fn) {
  request <- .ca_input(CanardStep, task = task, name = name, fn = fn)
  saved <- .ca_enter_step(request@task, request@name, "step")
  expr <- paste0("map_extract_value(checkpoints, ",
    DBI::dbQuoteString(task@db@con, request@name), ").value")
  if (!is.null(saved)) {
    rows <- .ca_owned(task, "value",
      projection = DBI::SQL(.ca_projection(expr, saved$type, task@db@con)))
    return(.ca_restore(rows$value, saved$type))
  }
  value <- request@fn()
  payload <- .ca_payload(value, task@db@con)
  rows <- .ca_owned(task, "checkpoint", name = request@name,
    value = payload$value, rtype = payload$rtype,
    projection = DBI::SQL(.ca_projection(expr, payload$type, task@db@con)),
    seconds = task@lease_seconds)
  .ca_restore(rows$value, payload$type)
}

#' Suspend a workflow until a database-clock deadline
#'
#' Saves a sleep checkpoint and releases the claim atomically. The worker resumes
#' the handler from its beginning after the deadline, replaying completed steps.
#' The sleep returns immediately when replayed. Code after the sleep does not run
#' in the suspended attempt. `canard_suspended` is a control-flow condition rather
#' than an error, so an ordinary error handler does not swallow suspension.
#'
#' @inheritParams ca_step
#' @param seconds Nonnegative sleep duration in seconds.
#' @return `NULL`, invisibly, on replay. The first call signals
#'   `canard_suspended` after persisting the suspension.
#' @export
ca_sleep <- function(task, name, seconds) {
  request <- .ca_input(CanardSleep, task = task, name = name, seconds = seconds)
  saved <- .ca_enter_step(request@task, request@name, "sleep")
  if (!is.null(saved)) return(invisible(NULL))
  .ca_owned(request@task, "sleep", name = request@name, seconds = request@seconds)
  stop(structure(list(message = paste("Task suspended:", task@id), call = NULL,
    id = task@id, attempt = task@attempt), class = c("canard_suspended", "condition")))
}

#' Execute one claimed workflow attempt
#'
#' Calls `handler(task@input, task)`, records its result, or records a handler
#' error as a task failure. Interrupts, lease loss, and persistence errors
#' propagate to the caller. A storage failure is not charged to the handler's
#' failure budget. If recording a handler failure also fails, both conditions
#' are retained in `canard_failure_recording_error`; see [ca_conditions()].
#'
#' @inheritParams ca_heartbeat
#' @param handler Function taking `(input, ctx)`.
#' @param failure_delay Nonnegative seconds before a failed task can be claimed
#'   again. This is separate from caller-managed SQL conflict retries.
#' @return A list, invisibly, with `id`, claim `attempt`, attempt `status`
#'   (`"completed"`, `"suspended"`, or `"failed"`), persisted task `state`,
#'   stored R `result`, and original handler `error`. A failed attempt can
#'   leave the task ready for retry or terminally failed. Error text is stored
#'   without truncation; the R condition remains available in this outcome.
#' @export
ca_run <- function(task, handler, failure_delay = 0) {
  request <- .ca_input(CanardRun, task = task, handler = handler, failure_delay = failure_delay)
  run <- .ca_runner(request@handler, request@failure_delay)
  invisible(run(request@task))
}

.ca_runner <- function(handler, failure_delay) {
  force(handler)
  force(failure_delay)
  function(task) {
    tryCatch({
      value <- handler(task@input, task)
      result <- .ca_finish(task, value)
      list(id = task@id, attempt = task@attempt, status = "completed", state = "completed",
        result = result, error = NULL)
    }, canard_suspended = function(e) {
      list(id = task@id, attempt = task@attempt, status = "suspended", state = "ready",
        result = NULL, error = NULL)
    }, error = function(e) {
      if (inherits(e, "canard_storage_error")) stop(e)
      rows <- tryCatch(.ca_owned(task, "fail", message = conditionMessage(e),
        delay_seconds = failure_delay), error = function(persistence_error) {
          stop(errorCondition("Unable to record the handler failure",
            class = c("canard_failure_recording_error", "canard_storage_error", "canard_error"),
            id = task@id, attempt = task@attempt, parent = e,
            persistence_error = persistence_error))
        })
      list(id = task@id, attempt = task@attempt, status = "failed", state = rows$state[[1L]],
        result = NULL, error = e)
    })
  }
}

#' Pull and execute R tasks
#'
#' Each worker executes one attempt at a time. Scale with independent R processes
#' connected through Quack. Only registered handler names are claimed. Calls to
#' named steps and [ca_heartbeat()] renew leases; no background R thread does so.
#' The worker never terminates its host process on lease loss.
#'
#' @inheritParams ca_claim
#' @inheritParams ca_run
#' @param handlers Named list of functions taking `(input, ctx)`.
#' @param max_tasks Maximum number of claimed attempts, including failures and
#'   suspensions. `Inf` runs without an attempt limit.
#' @param poll_seconds Positive delay when no work is found.
#' @param idle_timeout Nonnegative seconds without a claim before returning.
#'   `Inf` waits indefinitely. Zero drains currently eligible work.
#' @param on_result Optional function called with each [ca_run()] outcome after
#'   persistence. The default warns about handler failures with
#'   `canard_task_failed`. A supplied callback owns outcome reporting instead.
#'   Errors from the callback propagate to the caller.
#' @return The number of executed attempts, invisibly. Use `on_result` to collect
#'   outcomes without accumulating an unbounded result list inside the worker.
#' @export
ca_work <- function(db, handlers, queue = "default", max_tasks = Inf,
                    poll_seconds = 0.1, idle_timeout = Inf,
                    lease_seconds = 30, worker = paste0("R-", Sys.getpid()),
                    reap_limit = 64L, failure_delay = 0, on_result = NULL) {
  settings <- .ca_input(CanardWorker, handlers = handlers, max_tasks = max_tasks,
    poll_seconds = poll_seconds, idle_timeout = idle_timeout,
    failure_delay = failure_delay, on_result = on_result)
  request <- .ca_input(CanardClaim, db = db, queue = queue, worker = worker,
    lease_seconds = lease_seconds, task_names = names(settings@handlers), reap_limit = reap_limit)
  claim <- .ca_claimant(request)
  runners <- lapply(settings@handlers, .ca_runner, failure_delay = settings@failure_delay)
  count <- 0L
  idle_since <- proc.time()[["elapsed"]]
  while (count < settings@max_tasks) {
    task <- claim()
    if (is.null(task)) {
      remaining <- settings@idle_timeout - (proc.time()[["elapsed"]] - idle_since)
      if (remaining <= 0) break
      Sys.sleep(min(settings@poll_seconds, remaining))
      next
    }
    outcome <- runners[[task@name]](task)
    if (!is.null(settings@on_result)) {
      settings@on_result(outcome)
    } else if (!is.null(outcome$error)) {
      warning(warningCondition(paste("Task handler failed:", task@id),
        class = "canard_task_failed", parent = outcome$error, outcome = outcome))
    }
    count <- count + 1L
    idle_since <- proc.time()[["elapsed"]]
  }
  invisible(count)
}
