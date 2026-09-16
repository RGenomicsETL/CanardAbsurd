assert_query_error <- function(con, sql, message) {
  error <- tryCatch(DBI::dbGetQuery(con, sql), error = identity)
  stopifnot(inherits(error, "error"))
  stopifnot(grepl(message, conditionMessage(error), fixed = TRUE))
}

read_status <- function(con) {
  DBI::dbGetQuery(con, "
    WITH value AS MATERIALIZED (SELECT ca_coordinator_status() AS status)
    SELECT status.state, status.poll_count, status.reaped_count, status.last_error
    FROM value
  ")
}

create_fixture <- function(path) {
  # Use the installed package's schema and native input encoding.
  db <- CanardAbsurd::ca_open(path)
  on.exit(CanardAbsurd::ca_close(db))

  tasks <- data.frame(
    id = c("expired-a", "expired-b", "expired-c", "retryable", "live",
           "ready", "completed", "failed", "cancelled"),
    queue = c("alpha", "beta", "alpha", rep("other", 6)),
    max_failures = c(1L, 3L, 2L, 3L, rep(1L, 5))
  )
  for (index in seq_len(nrow(tasks))) {
    CanardAbsurd::ca_spawn(
      db,
      name = "coordinator-test",
      input = list(sample = tasks$id[[index]]),
      id = tasks$id[[index]],
      queue = tasks$queue[[index]],
      max_failures = tasks$max_failures[[index]]
    )
  }

  # Inject expired/live claims without sleeping through actual lease durations.
  DBI::dbExecute(db@con, "
    UPDATE canard_absurd.tasks AS task
    SET state = 'running', attempt = fixture.failures + 1,
        failures = fixture.failures, worker = 'test-worker', token = uuid(),
        lease_until = fixture.deadline
    FROM (VALUES
      ('expired-a', 0, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
      ('expired-b', 2, TIMESTAMPTZ '2000-01-02 00:00:00+00'),
      ('expired-c', 1, TIMESTAMPTZ '2000-01-03 00:00:00+00'),
      ('retryable', 0, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
      ('live', 0, current_timestamp + INTERVAL '1 hour')
    ) AS fixture(id, failures, deadline)
    WHERE task.id = fixture.id
  ")
  DBI::dbExecute(db@con, "
    UPDATE canard_absurd.tasks
    SET state = id, attempt = 1,
        failures = CASE WHEN id = 'failed' THEN 1 ELSE 0 END
    WHERE id IN ('completed', 'failed', 'cancelled')
  ")
}

test_reaper <- function(extension) {
  path <- tempfile(fileext = ".duckdb")
  create_fixture(path)
  driver <- duckdb::duckdb(path, config = list(
    allow_unsigned_extensions = "true",
    autoinstall_known_extensions = "false"
  ))
  con <- DBI::dbConnect(driver)
  loaded <- FALSE
  on.exit({
    tryCatch({
      if (loaded) {
        DBI::dbGetQuery(con, "SELECT ca_coordinator_stop()")
      }
    }, finally = {
      DBI::dbDisconnect(con, shutdown = TRUE)
    })
  })

  DBI::dbExecute(con, "
    CREATE TEMP TABLE before_poll AS SELECT * FROM canard_absurd.tasks
  ")
  DBI::dbExecute(con, paste("LOAD", DBI::dbQuoteString(con, extension)))
  loaded <- TRUE

  assert_query_error(con, "SELECT ca_coordinator_start(NULL, 1)", "cannot be NULL")
  assert_query_error(con, "SELECT ca_coordinator_start(10, NULL)", "cannot be NULL")
  assert_query_error(con, "SELECT ca_coordinator_start(0, 1)", "poll_milliseconds")
  assert_query_error(con, "SELECT ca_coordinator_start(10, 0)", "reap_limit")

  started <- DBI::dbGetQuery(con,
    "SELECT ca_coordinator_start(10, 1) AS started")$started[[1L]]
  stopifnot(isTRUE(started))

  deadline <- proc.time()[["elapsed"]] + 10
  repeat {
    status <- read_status(con)
    if (nzchar(status$last_error[[1L]])) {
      stop("Coordinator poll failed: ", status$last_error[[1L]])
    }
    if (as.numeric(status$reaped_count[[1L]]) == 3) {
      break
    }
    if (proc.time()[["elapsed"]] >= deadline) {
      stop("Coordinator did not reap the three exhausted tasks")
    }
    Sys.sleep(0.01)
  }
  stopifnot(identical(status$state[[1L]], "running"))
  stopifnot(as.numeric(status$poll_count[[1L]]) >= 3)

  started_again <- DBI::dbGetQuery(con,
    "SELECT ca_coordinator_start(10, 1) AS started")$started[[1L]]
  stopifnot(identical(started_again, FALSE))
  stopped <- DBI::dbGetQuery(con,
    "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]]
  stopifnot(isTRUE(stopped))

  reaped <- DBI::dbGetQuery(con, "
    SELECT task.id, task.state, task.failures, task.attempt, task.error,
           task.worker IS NULL AND task.token IS NULL
             AND task.lease_until IS NULL AS ownership_cleared,
           task.input IS NOT DISTINCT FROM before.input AS input_preserved,
           task.input_rtype IS NOT DISTINCT FROM before.input_rtype AS type_preserved,
           task.created_at = before.created_at AS created_at_preserved
    FROM canard_absurd.tasks AS task
    JOIN before_poll AS before USING (id)
    WHERE task.id LIKE 'expired-%'
    ORDER BY task.id
  ")
  stopifnot(identical(reaped$id, c("expired-a", "expired-b", "expired-c")))
  stopifnot(all(reaped$state == "failed"))
  stopifnot(identical(reaped$failures, c(1L, 3L, 2L)))
  stopifnot(identical(reaped$attempt, c(1L, 3L, 2L)))
  stopifnot(all(reaped$error == "worker lease expired"))
  stopifnot(all(reaped$ownership_cleared))
  stopifnot(all(reaped$input_preserved), all(reaped$type_preserved))
  stopifnot(all(reaped$created_at_preserved))

  changed <- DBI::dbGetQuery(con, "
    SELECT * FROM canard_absurd.tasks WHERE id NOT LIKE 'expired-%'
    EXCEPT
    SELECT * FROM before_poll WHERE id NOT LIKE 'expired-%'
  ")
  stopifnot(nrow(changed) == 0L)
  status <- read_status(con)
  stopifnot(identical(status$state[[1L]], "closed"))
  stopifnot(as.numeric(status$reaped_count[[1L]]) == 3)
  stopifnot(identical(status$last_error[[1L]], ""))
  assert_query_error(con, "SELECT ca_coordinator_start(10, 1)", "cannot restart")
  stopped_again <- DBI::dbGetQuery(con,
    "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]]
  stopifnot(identical(stopped_again, FALSE))
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 1L) {
    stop("Usage: Rscript test-v1.R /path/to/canard_coordinator.duckdb_extension")
  }
  extension <- normalizePath(args[[1L]], mustWork = TRUE)
  test_reaper(extension)
}

main()
