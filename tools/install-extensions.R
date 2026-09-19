#!/usr/bin/env Rscript

# A shared DuckDB home keeps explicit installations available to later R processes.
dir.create(path.expand("~/.duckdb"), showWarnings = FALSE, recursive = TRUE)
local({
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "INSTALL quack")
  # Quack clients load httpfs; installing it here keeps connections offline.
  DBI::dbExecute(con, "INSTALL httpfs")
  print(DBI::dbGetQuery(con, "SELECT version() AS duckdb_version"))
})
