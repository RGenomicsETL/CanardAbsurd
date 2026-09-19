#' Supervise an external command within a task lease
#'
#' Runs a command in its own process while the calling R worker renews the task
#' lease, so work longer than one lease does not let another worker reclaim the
#' task. Use it for aligners, callers, and other programs, or for blocking R
#' computation run through `Rscript`. A timer in the blocked R process cannot
#' renew a lease; this supervisor stays outside the blocking work.
#'
#' The lease is renewed before the command starts, so no work begins on a lost
#' claim, and then every `heartbeat_seconds` while it runs. The command runs in
#' `attempt-<n>` inside `directory`, with standard output and error written to
#' `stdout.log` and `stderr.log` there. Relative output paths in `args` land in
#' that attempt directory. A later attempt cannot overwrite an earlier attempt's
#' files, and an existing attempt directory is an error rather than reused.
#'
#' If the lease is lost, including by [ca_cancel()], the timeout passes, or the
#' supervising R code is interrupted or fails, the command's process tree is
#' killed. Lease loss raises `canard_lease_lost`; a nonzero exit or a timeout
#' raises `canard_process_error`, which a handler records as an ordinary failure.
#' Publish results by returning paths from a [ca_step()] callback: only the
#' current lease holder can save that checkpoint.
#'
#' Supervision cannot guarantee that two attempts never overlap physically. A
#' paused or partitioned worker can outlive its lease before its next heartbeat.
#' Jobs submitted to Slurm or another backend need that backend's own
#' submission, discovery, and cancellation identities; supervising the submit
#' command does not supervise the remote job.
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
