extension <- Sys.getenv("CANARDABSURD_COORDINATOR_EXTENSION")
if (!nzchar(extension)) {
  exit_file("Native coordinator tests require CANARDABSURD_COORDINATOR_EXTENSION")
}

driver <- duckdb::duckdb(tempfile(fileext = ".duckdb"), shared_home = FALSE,
  config = list(allow_unsigned_extensions = "true",
    autoinstall_known_extensions = "false",
    storage_compatibility_version = "v1.5.0"))
con <- DBI::dbConnect(driver, bigint = "integer64")
schema <- readLines(system.file("sql", "schema.sql", package = "CanardAbsurd",
  mustWork = TRUE), warn = FALSE)
for (sql in strsplit(paste(schema, collapse = "\n"), ";", fixed = TRUE)[[1L]]) {
  if (nzchar(trimws(sql))) DBI::dbExecute(con, sql)
}
Connection <- getFromNamespace("CanardConnection", "CanardAbsurd")
services <- getFromNamespace(".ca_services", "CanardAbsurd")
db <- Connection(con = con,
  query = function(sql) DBI::dbGetQuery(con, sql), services = services())

expect_true(ca_coordinator_start(db, extension, poll_milliseconds = 10,
  reap_limit = 16))
expect_true(db@services$coordinator_loaded)
expect_true(db@services$coordinator)
status <- ca_coordinator_status(db)
expect_identical(status$state, "running")
expect_false(ca_coordinator_start(db, poll_milliseconds = 10, reap_limit = 16))
expect_true(ca_coordinator_stop(db))
expect_false(db@services$coordinator)
expect_identical(ca_coordinator_status(db)$state, "closed")
expect_error(ca_coordinator_start(db, poll_milliseconds = 10, reap_limit = 16),
  pattern = "cannot restart")
ca_close(db)
expect_false(DBI::dbIsValid(con))
