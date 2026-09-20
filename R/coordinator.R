.ca_coordinator_owner <- function(db) {
  if (!DBI::dbIsValid(db@con)) {
    stop(errorCondition("The CanardAbsurd connection is closed",
      class = c("canard_connection_error", "canard_error")))
  }
  if (nzchar(db@uri) && !db@server) {
    stop(errorCondition("The native coordinator must run in the database-owner process",
      class = c("canard_coordinator_error", "canard_error")))
  }
  invisible(NULL)
}

#' Start native task maintenance
#'
#' Loads the optional `canard_coordinator` extension in the database-owner
#' process and starts it marking abandoned tasks failed across every queue, on
#' its own DuckDB connection and native thread. Workers keep doing the same for
#' the queues they poll; this covers queues nobody polls.
#'
#' It can start once per database instance. Call [ca_coordinator_stop()] or
#' [ca_close()] to join the thread and release the connection: the extension
#' holds that connection from the moment it loads, so closing the DBI connection
#' directly leaves the database open, locked and maintained until the process
#' exits.
#'
#' No signed build is distributed, so a local build loads only into a database
#' opened with `allow_unsigned_extensions = TRUE`; see [ca_open()].
#'
#' @param db A local handle returned by [ca_open()] or [ca_serve()].
#' @param extension Optional path to a compatible coordinator extension.
#'   `NULL` loads an explicitly preinstalled `canard_coordinator` extension.
#' @param poll_milliseconds Delay between maintenance polls, in milliseconds.
#' @param reap_limit Maximum expired tasks failed by one poll.
#' @return `TRUE` if this call started the coordinator and `FALSE` if it was
#'   already running.
#' @export
ca_coordinator_start <- function(db, extension = NULL, poll_milliseconds = 1000,
                                 reap_limit = 64) {
  request <- .ca_input(CanardCoordinator, db = db, extension = extension,
    poll_milliseconds = poll_milliseconds, reap_limit = reap_limit)
  .ca_coordinator_owner(request@db)
  if (!db@services$coordinator_loaded) {
    .ca_load_extension(db@con, request@extension,
      "canard_coordinator", "Canard coordinator")
    db@services$coordinator_loaded <- TRUE
    db@services$coordinator <- TRUE
  }
  sql <- DBI::sqlInterpolate(db@con,
    "SELECT ca_coordinator_start(CAST(?poll AS UBIGINT), CAST(?limit AS UBIGINT)) AS started",
    poll = request@poll_milliseconds, limit = request@reap_limit)
  DBI::dbGetQuery(db@con, sql)$started[[1L]]
}

#' Inspect native task maintenance
#'
#' @inheritParams ca_coordinator_start
#' @return A one-row data frame containing lifecycle state, configuration,
#'   successful poll and reaped-task counts, and the latest polling error.
#' @export
ca_coordinator_status <- function(db) {
  .ca_coordinator_owner(db)
  if (!db@services$coordinator_loaded) {
    stop(errorCondition("The Canard coordinator extension is not loaded",
      class = c("canard_coordinator_error", "canard_error")))
  }
  DBI::dbGetQuery(db@con, paste(
    "WITH value AS (SELECT ca_coordinator_status() AS status)",
    "SELECT status.state, status.running, status.poll_milliseconds,",
    "status.reap_limit, status.poll_count, status.reaped_count, status.last_error",
    "FROM value"))
}

#' Stop native task maintenance
#'
#' Interrupts active coordinator SQL, joins the native thread, and closes the
#' coordinator's dedicated DuckDB connection. A stopped C API v1 coordinator
#' cannot restart on the same database handle.
#'
#' @inheritParams ca_coordinator_start
#' @return `TRUE` if this call closed the coordinator connection and `FALSE` if
#'   it was already closed or was never loaded.
#' @export
ca_coordinator_stop <- function(db) {
  .ca_coordinator_owner(db)
  if (!db@services$coordinator_loaded || !db@services$coordinator) return(FALSE)
  stopped <- DBI::dbGetQuery(db@con,
    "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]]
  if (isTRUE(stopped)) db@services$coordinator <- FALSE
  stopped
}
