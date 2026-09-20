.ca_enter_step <- function(task, name, kind) {
  if (exists(name, envir = task@seen, inherits = FALSE)) {
    stop(errorCondition(paste("Step names must be unique within an attempt:", name),
      class = c("canard_replay_error", "canard_error"), id = task@id, name = name))
  }
  rows <- .ca_owned(task, "enter", name = name, seconds = task@lease_seconds)
  saved_kind <- rows$checkpoint_kind[[1L]]
  saved <- if (is.na(saved_kind)) NULL else list(kind = saved_kind)
  if (!is.null(saved) && !is.null(rows$checkpoint_rtype[[1L]])) {
    saved$type <- .ca_decoded(.ca_read_type(rows$checkpoint_rtype[[1L]]), task@id, "enter")
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
#' Runs `fn` and saves its result under `name`, or returns the saved value when
#' this task already has one. The callback runs outside any transaction, and its
#' result is saved only once it returns, so external effects must be idempotent:
#' lease tokens fence database writes, not other services. Give external requests
#' stable business keys.
#'
#' Step names must be unique within an attempt and stable across deployments, so
#' index repeated steps in a loop. Keep saved results readable by handlers that
#' may resume existing tasks.
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
  if (!is.null(saved)) return(.ca_read_value(task, "value", saved$type, expr))
  value <- request@fn()
  payload <- .ca_payload(value, task@db@con)
  .ca_read_value(task, "checkpoint", payload$type, expr, name = request@name,
    value = payload$value, rtype = payload$rtype, seconds = task@lease_seconds)
}

#' Suspend a workflow until a database-clock deadline
#'
#' Saves a sleep checkpoint and releases the claim in one statement. Code after
#' the sleep does not run in this attempt: after the deadline a worker reruns the
#' handler from the top, replaying completed steps, and the sleep returns at
#' once. Suspension travels as `canard_suspended`, a control-flow condition, so
#' an ordinary error handler does not swallow it.
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
#' Calls `handler(task@input, task)` and records its result, or records a
#' handler error as a task failure. Interrupts, lease loss and storage errors
#' propagate instead, and do not use the failure budget. If recording a handler
#' failure also fails, `canard_failure_recording_error` keeps both conditions;
#' see [ca_conditions()].
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
#' Claims and runs tasks whose names appear in `handlers`, one attempt at a
#' time, until `max_tasks` or `idle_timeout` is reached. Scale out with more
#' worker processes connected through Quack. Leases are renewed by [ca_step()],
#' [ca_sleep()] and [ca_heartbeat()] only; no background thread does it, so
#' supervise long external commands with [ca_process()]. Losing a lease never
#' ends the worker process.
#'
#' Competing workers pick the same eligible task, and DuckDB aborts the losing
#' statement. The worker retries such a statement up to `conflict_retries` times,
#' backing off from 10 ms to 0.5 s (about 1.6 seconds in total by default),
#' repeating only the SQL and never an R callback, and signalling
#' `canard_conflict_retry` beforehand; see [ca_conditions()]. Jitter comes from
#' the clock and process ID, leaving R's random number stream untouched. An
#' exhausted budget raises `canard_conflict`. Lease loss and transport failures
#' are never retried.
#'
#' @inheritParams ca_claim
#' @inheritParams ca_run
#' @param handlers Named list of functions taking `(input, ctx)`.
#' @param max_tasks Maximum number of claimed attempts, including failures and
#'   suspensions. `Inf` runs without an attempt limit.
#' @param poll_seconds Positive delay when no work is found.
#' @param idle_timeout Nonnegative seconds without a claim before returning.
#'   `Inf` waits indefinitely. Zero drains currently eligible work.
#' @param conflict_retries Nonnegative number of automatic retries for each
#'   statement aborted by a known write conflict. Zero installs no retry policy,
#'   leaving `canard_retryable` to the caller's own calling handler.
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
                    reap_limit = 64L, failure_delay = 0, conflict_retries = 8L,
                    on_result = NULL) {
  settings <- .ca_input(CanardWorker, handlers = handlers, max_tasks = max_tasks,
    poll_seconds = poll_seconds, idle_timeout = idle_timeout,
    failure_delay = failure_delay, conflict_retries = conflict_retries,
    on_result = on_result)
  request <- .ca_input(CanardClaim, db = db, queue = queue, worker = worker,
    lease_seconds = lease_seconds, task_names = names(settings@handlers), reap_limit = reap_limit)
  claim <- .ca_claimant(request)
  runners <- lapply(settings@handlers, .ca_runner, failure_delay = settings@failure_delay)
  count <- 0L
  idle_since <- proc.time()[["elapsed"]]
  work <- function() {
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
      count <<- count + 1L
      idle_since <<- proc.time()[["elapsed"]]
    }
  }
  retries <- settings@conflict_retries
  if (retries == 0) work() else withCallingHandlers(work(), canard_retryable = function(conflict) {
    if (conflict$attempts > retries) return()
    # Clock and PID jitter keeps the caller's RNG stream untouched.
    jitter <- ((as.double(Sys.time()) * 1e6 + Sys.getpid() * 7919) %% 1009) / 1009
    delay <- min(0.5, 0.01 * 2^(conflict$attempts - 1)) * (0.5 + jitter / 2)
    signalCondition(structure(class = c("canard_conflict_retry", "condition"), list(
      message = paste("Retrying conflicting statement:", conflict$operation), call = NULL,
      conflict = conflict, attempts = conflict$attempts, delay = delay)))
    Sys.sleep(delay)
    invokeRestart("canard_retry")
  })
  invisible(count)
}
