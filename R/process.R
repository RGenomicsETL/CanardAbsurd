#' Supervise an external command within a task lease
#'
#' Runs a command in a child process while the worker keeps the task lease
#' alive, so work longer than one lease is not reclaimed by another worker. Use
#' it for external tools, or for blocking R computation run through `Rscript`: a
#' timer inside the blocked process cannot renew anything.
#'
#' The lease is renewed before the command starts, so nothing begins on a lost
#' claim, and then every `heartbeat_seconds`. The command runs in `attempt-<n>`
#' under `directory` and writes `stdout.log` and `stderr.log` there, so relative
#' output paths stay inside that attempt and no later attempt overwrites an
#' earlier one's files. An existing attempt directory is an error. Publish
#' results by returning paths from a [ca_step()] callback: only the current lease
#' holder can save that checkpoint.
#'
#' Lease loss, including from [ca_cancel()], a timeout, or an error or interrupt
#' in the supervising code kills the command's process tree. Lease loss raises
#' `canard_lease_lost`; a nonzero exit or timeout raises `canard_process_error`,
#' which a handler records as an ordinary failure.
#'
#' Overlap remains possible: a paused or partitioned worker can outlive its lease
#' before the next heartbeat. Work submitted to Slurm or a similar backend needs
#' that backend's own job identity, since supervising the submitting command does
#' not supervise the job.
#'
#' @inheritParams ca_heartbeat
#' @param command Executable to run.
#' @param args Character vector of arguments.
#' @param directory Directory that holds this step's attempt directories. Use one
#'   directory per task and step.
#' @param timeout Positive seconds before the command is killed, or `Inf`.
#' @param heartbeat_seconds Positive renewal interval, shorter than the task's
#'   `lease_seconds`.
#' @return The attempt directory path, after the command exits with status 0.
#' @export
ca_process <- function(task, command, args = character(), directory, timeout = Inf,
                       heartbeat_seconds = task@lease_seconds / 3) {
  request <- .ca_input(CanardProcess, task = task, command = command, args = args,
    directory = directory, timeout = timeout, heartbeat_seconds = heartbeat_seconds)
  if (!requireNamespace("processx", quietly = TRUE)) {
    stop("ca_process() requires the processx package")
  }
  task <- request@task
  ca_heartbeat(task)
  attempt <- file.path(request@directory, paste0("attempt-", task@attempt))
  if (dir.exists(attempt) || !dir.create(attempt, recursive = TRUE)) {
    stop("Attempt directory already exists or cannot be created: ", attempt)
  }
  process <- processx::process$new(request@command, request@args, wd = attempt,
    stdout = file.path(attempt, "stdout.log"), stderr = file.path(attempt, "stderr.log"),
    cleanup_tree = TRUE)
  on.exit(if (process$is_alive()) process$kill_tree())
  started <- proc.time()[["elapsed"]]
  repeat {
    remaining <- request@timeout - (proc.time()[["elapsed"]] - started)
    if (remaining <= 0) break
    process$wait(1000 * min(request@heartbeat_seconds, remaining))
    if (!process$is_alive()) break
    ca_heartbeat(task)
  }
  status <- if (process$is_alive()) NA_integer_ else process$get_exit_status()
  if (!identical(status, 0L)) {
    reason <- if (is.na(status)) "timed out" else paste("exited with status", status)
    stop(errorCondition(paste0("Command ", reason, "; logs are in ", attempt),
      class = c("canard_process_error", "canard_error"),
      id = task@id, status = status, directory = attempt))
  }
  attempt
}
