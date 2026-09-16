args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) stop("Usage: Rscript test-v1.R /path/to/canard_coordinator.duckdb_extension")
extension <- normalizePath(args[[1L]], mustWork = TRUE)

driver <- duckdb::duckdb(tempfile(fileext = ".duckdb"), shared_home = FALSE,
  config = list(allow_unsigned_extensions = "true", autoinstall_known_extensions = "false"))
con <- DBI::dbConnect(driver)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
DBI::dbExecute(con, paste("LOAD", DBI::dbQuoteString(con, extension)))
DBI::dbExecute(con, "CREATE SCHEMA canard_absurd")
DBI::dbExecute(con, "CREATE TABLE canard_absurd.schema_version (version INTEGER PRIMARY KEY)")
DBI::dbExecute(con, "INSERT INTO canard_absurd.schema_version VALUES (1)")
DBI::dbExecute(con, "
  CREATE TABLE canard_absurd.tasks (
    id VARCHAR PRIMARY KEY,
    state VARCHAR NOT NULL,
    failures INTEGER NOT NULL,
    max_failures INTEGER NOT NULL,
    lease_until TIMESTAMPTZ,
    lease_token UUID,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
  )")
DBI::dbExecute(con, "
  INSERT INTO canard_absurd.tasks
  VALUES ('expired', 'running', 0, 1, TIMESTAMPTZ '2000-01-01 00:00:00+00',
          NULL, NULL, current_timestamp)")

started <- DBI::dbGetQuery(con, "SELECT ca_coordinator_start(10, 16) AS started")$started[[1L]]
stopifnot(isTRUE(started))
deadline <- proc.time()[["elapsed"]] + 10
repeat {
  state <- DBI::dbGetQuery(con,
    "SELECT state FROM canard_absurd.tasks WHERE id = 'expired'")$state[[1L]]
  if (identical(state, "failed")) break
  if (proc.time()[["elapsed"]] >= deadline) stop("Coordinator did not reap the expired task")
  Sys.sleep(0.01)
}
status <- DBI::dbGetQuery(con, "
  SELECT (ca_coordinator_status()).state AS state,
         (ca_coordinator_status()).poll_count AS poll_count,
         (ca_coordinator_status()).reaped_count AS reaped_count,
         (ca_coordinator_status()).last_error AS last_error")
stopifnot(identical(status$state[[1L]], "running"))
stopifnot(as.numeric(status$poll_count[[1L]]) >= 1)
stopifnot(as.numeric(status$reaped_count[[1L]]) == 1)
stopifnot(identical(status$last_error[[1L]], ""))
stopifnot(identical(
  DBI::dbGetQuery(con, "SELECT ca_coordinator_start(10, 16) AS started")$started[[1L]],
  FALSE))
stopifnot(isTRUE(DBI::dbGetQuery(con, "SELECT ca_coordinator_stop() AS stopped")$stopped[[1L]]))
stopifnot(identical(
  DBI::dbGetQuery(con, "SELECT (ca_coordinator_status()).state AS state")$state[[1L]],
  "closed"))
restart <- try(DBI::dbGetQuery(con, "SELECT ca_coordinator_start(10, 16)"), silent = TRUE)
stopifnot(inherits(restart, "try-error"), grepl("cannot restart", as.character(restart), fixed = TRUE))
