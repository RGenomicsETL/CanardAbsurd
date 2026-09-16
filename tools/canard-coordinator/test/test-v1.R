# Run from any working directory; the schema belongs to the package, not this test.
script_argument <- grep("^--file=", commandArgs(), value = TRUE)
script_path <- normalizePath(sub("^--file=", "", script_argument), mustWork = TRUE)
package_root <- normalizePath(file.path(dirname(script_path), "../../.."), mustWork = TRUE)

coordinator_status <- function(con) {
  DBI::dbGetQuery(con, "
    WITH snapshot AS MATERIALIZED (SELECT ca_coordinator_status() AS status)
    SELECT status.state, status.poll_count, status.reaped_count, status.last_error
    FROM snapshot")
}

expect_sql_error <- function(con, sql, message) {
  error <- tryCatch(DBI::dbGetQuery(con, sql), error = identity)
  stopifnot(inherits(error, "error"))
  stopifnot(grepl(message, conditionMessage(error), fixed = TRUE))
}

test_coordinator <- function(extension, package_root) {
  driver <- duckdb::duckdb(tempfile(fileext = ".duckdb"), config = list(
    allow_unsigned_extensions = "true", autoinstall_known_extensions = "false"))
  con <- DBI::dbConnect(driver)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  schema <- readLines(file.path(package_root, "inst/sql/schema.sql"), warn = FALSE)
  statements <- strsplit(paste(schema, collapse = "\n"), ";", fixed = TRUE)[[1L]]
  for (statement in statements) {
    if (nzchar(trimws(statement))) {
      DBI::dbExecute(con, statement)
    }
  }

  DBI::dbExecute(con, paste("LOAD", DBI::dbQuoteString(con, extension)))
  # Stop before disconnecting, including when an assertion below fails.
  on.exit(DBI::dbGetQuery(con, "SELECT ca_coordinator_stop()"), add = TRUE, after = FALSE)

  expect_sql_error(con, "SELECT ca_coordinator_start(NULL, 16)", "cannot be NULL")
  expect_sql_error(con, "SELECT ca_coordinator_start(10, NULL)", "cannot be NULL")
  expect_sql_error(con, "SELECT ca_coordinator_start(0, 16)", "poll_milliseconds must be")
  expect_sql_error(con, "SELECT ca_coordinator_start(10, 0)", "reap_limit must be")
  stopifnot(identical(coordinator_status(con)$state[[1L]], "ready"))

  # Only the two exhausted, expired claims should be reaped. Use actual lease
  # ownership, counters, and native payloads so the package constraints apply.
  DBI::dbExecute(con, "
    INSERT INTO canard_absurd.tasks
      (id, queue, name, input, input_rtype, state, attempt, failures, max_failures,
       worker, token, lease_until, updated_at)
    SELECT id, queue, 'example', 17::VARIANT,
           struct_pack(kind := 'integer', length := 1)::VARIANT,
           'running', failures + 1, failures, max_failures, 'test-worker', uuid(),
           lease_until, TIMESTAMPTZ '1999-01-01 00:00:00+00'
    FROM (VALUES
      ('expired-default', 'default', 0, 1, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
      ('expired-other', 'other', 2, 3, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
      ('retryable', 'default', 0, 3, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
      ('live', 'default', 0, 1, TIMESTAMPTZ '2100-01-01 00:00:00+00')
    ) AS fixture(id, queue, failures, max_failures, lease_until)")
  DBI::dbExecute(con, "
    INSERT INTO canard_absurd.tasks (id, queue, name, input_rtype, state, max_failures)
    SELECT state, 'default', 'example',
           struct_pack(kind := 'NULL', length := 0)::VARIANT, state, 1
    FROM (VALUES ('ready'), ('completed'), ('failed'), ('cancelled')) AS fixture(state)")
  DBI::dbExecute(con, "
    UPDATE canard_absurd.tasks
    SET checkpoints = map(['saved'], [struct_pack(
      kind := 'step', value := 42::VARIANT,
      rtype := struct_pack(kind := 'integer', length := 1)::VARIANT)])
    WHERE id LIKE 'expired-%'")
  DBI::dbExecute(con, "CREATE TABLE before_reaping AS SELECT * FROM canard_absurd.tasks")

  started <- DBI::dbGetQuery(con, "SELECT ca_coordinator_start(10, 1) AS started")$started[[1L]]
  stopifnot(isTRUE(started))
  deadline <- proc.time()[["elapsed"]] + 10
  repeat {
    status <- coordinator_status(con)
    if (nzchar(status$last_error[[1L]])) {
      stop("Coordinator poll failed: ", status$last_error[[1L]])
    }
    if (as.numeric(status$reaped_count[[1L]]) == 2) {
      break
    }
    if (proc.time()[["elapsed"]] >= deadline) {
      stop("Coordinator did not reap the exhausted claims")
    }
    Sys.sleep(0.01)
  }
  stopifnot(identical(status$state[[1L]], "running"))
  stopifnot(as.numeric(status$poll_count[[1L]]) >= 2)
  stopifnot(identical(
    DBI::dbGetQuery(con, "SELECT ca_coordinator_start(10, 1) AS started")$started[[1L]],
    FALSE))
  stopifnot(isTRUE(DBI::dbGetQuery(con, "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]]))

  # Match the complete inst/sql/reap.sql transition, not merely its state label.
  reaped <- DBI::dbGetQuery(con, "
    SELECT current.id,
           current.state = 'failed' AS failed,
           current.failures = previous.failures + 1 AS failure_counted,
           current.error = 'worker lease expired' AS reason_recorded,
           current.worker IS NULL AND current.token IS NULL
             AND current.lease_until IS NULL AS ownership_cleared,
           current.updated_at > previous.updated_at AS timestamp_updated,
           current.attempt = previous.attempt
             AND current.input IS NOT DISTINCT FROM previous.input
             AND current.input_rtype IS NOT DISTINCT FROM previous.input_rtype
             AND current.checkpoints IS NOT DISTINCT FROM previous.checkpoints AS work_preserved
    FROM canard_absurd.tasks AS current
    JOIN before_reaping AS previous USING (id)
    WHERE current.id LIKE 'expired-%'")
  stopifnot(nrow(reaped) == 2L)
  stopifnot(all(as.matrix(reaped[, -1L, drop = FALSE])))

  changed_unrelated <- DBI::dbGetQuery(con, "
    SELECT * FROM canard_absurd.tasks WHERE id NOT LIKE 'expired-%'
    EXCEPT
    SELECT * FROM before_reaping WHERE id NOT LIKE 'expired-%'")
  stopifnot(nrow(changed_unrelated) == 0L)
  stopifnot(identical(coordinator_status(con)$state[[1L]], "closed"))
  stopifnot(as.numeric(coordinator_status(con)$reaped_count[[1L]]) == 2)
  stopifnot(identical(
    DBI::dbGetQuery(con, "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]],
    FALSE))
  expect_sql_error(con, "SELECT ca_coordinator_start(10, 1)", "cannot restart")
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) {
  stop("Usage: Rscript test-v1.R /path/to/canard_coordinator.duckdb_extension")
}
extension <- normalizePath(args[[1L]], mustWork = TRUE)
test_coordinator(extension, package_root)
