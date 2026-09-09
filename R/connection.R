CanardConnection <- S7::new_class(
  "CanardConnection", package = "CanardAbsurd",
  properties = list(
    con = S7::class_any,
    uri = S7::new_property(S7::class_character, default = ""),
    server = S7::new_property(S7::class_logical, default = FALSE)
  )
)

#' Open a local workflow database
#'
#' Installs schema version 1 in a transaction, or checks the existing version.
#' The handle owns its DBI connection. Close it explicitly with [ca_close()].
#' No extensions are downloaded. A file-backed database has one owning process;
#' other processes connect to a Quack server rather than opening that file.
#'
#' @param path DuckDB database path, or `":memory:"`.
#' @return An S7 `CanardConnection` handle.
#' @export
#' @examples
#' db <- ca_open()
#' id <- ca_spawn(db, "sum", list(x = 2, y = 3))
#' ca_work(db, list(sum = function(input, ctx) input$x + input$y), max_tasks = 1)
#' ca_inspect(db, id)$result
#' ca_close(db)
ca_open <- function(path = ":memory:") {
  stopifnot(is.character(path), length(path) == 1L, !is.na(path), nzchar(path))
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path,
    config = list(autoinstall_known_extensions = "false"))
  ready <- FALSE
  on.exit(if (!ready) DBI::dbDisconnect(con, shutdown = TRUE))
  schema <- readLines(system.file("sql", "schema.sql", package = "CanardAbsurd",
    mustWork = TRUE), warn = FALSE)
  DBI::dbWithTransaction(con, {
    installed <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM information_schema.tables
      WHERE table_catalog = current_database() AND table_schema = 'canard_absurd'
        AND table_name = 'schema_version'")$n[[1L]]
    if (installed == 1L) {
      version <- DBI::dbGetQuery(con, "SELECT version FROM canard_absurd.schema_version")
      if (!identical(version$version, 1L)) stop("Unsupported CanardAbsurd schema version")
    }
    for (sql in strsplit(paste(schema, collapse = "\n"), ";", fixed = TRUE)[[1L]]) {
      if (nzchar(trimws(sql))) DBI::dbExecute(con, sql)
    }
  })
  ready <- TRUE
  CanardConnection(con = con)
}

.ca_load_quack <- function(con, extension) {
  target <- "quack"
  if (!is.null(extension)) {
    target <- DBI::dbQuoteString(con, normalizePath(extension, mustWork = TRUE))
  }
  tryCatch(DBI::dbExecute(con, paste("LOAD", target)), error = function(e) {
    stop("Quack must be installed explicitly for this DuckDB runtime: ",
      conditionMessage(e), call. = FALSE)
  })
  invisible(NULL)
}

#' Serve a workflow database over Quack
#'
#' The calling process owns the database and must remain alive. Quack handles
#' SQL requests on its native server threads; R task handlers run in clients.
#' Protect non-local endpoints with authenticated ingress and TLS.
#'
#' @inheritParams ca_open
#' @param uri Quack endpoint, including a port when not using the default.
#' @param token Nonempty shared authentication token.
#' @param extension Optional path to a compatible signed Quack extension.
#'   `NULL` loads an explicitly preinstalled `quack` extension.
#' @return A local S7 `CanardConnection` handle owning the server.
#' @export
ca_serve <- function(path, uri = "quack:127.0.0.1:9494", token, extension = NULL) {
  stopifnot(is.character(uri), length(uri) == 1L, !is.na(uri), nzchar(uri))
  stopifnot(is.character(token), length(token) == 1L, !is.na(token), nzchar(token))
  db <- ca_open(path)
  ready <- FALSE
  on.exit(if (!ready) ca_close(db))
  .ca_load_quack(db@con, extension)
  sql <- DBI::sqlInterpolate(db@con,
    "CALL quack_serve(?uri, token = ?token)", uri = uri, token = token)
  started <- DBI::dbGetQuery(db@con, sql)
  db@uri <- started$listen_uri[[1L]]
  db@server <- TRUE
  ready <- TRUE
  db
}

#' Connect to a workflow server
#'
#' Uses DuckDB's Quack client. Each workflow command is executed wholly on the
#' server in one SQL statement. The client owns only an in-memory DuckDB
#' connection, not a second writable handle to the server's database file.
#'
#' @inheritParams ca_serve
#' @return An S7 `CanardConnection` handle.
#' @export
ca_connect <- function(uri = "quack:127.0.0.1:9494", token, extension = NULL) {
  stopifnot(is.character(uri), length(uri) == 1L, !is.na(uri), nzchar(uri))
  stopifnot(is.character(token), length(token) == 1L, !is.na(token), nzchar(token))
  con <- DBI::dbConnect(duckdb::duckdb(),
    config = list(autoinstall_known_extensions = "false"))
  ready <- FALSE
  on.exit(if (!ready) DBI::dbDisconnect(con, shutdown = TRUE))
  .ca_load_quack(con, extension)
  secret <- DBI::sqlInterpolate(con,
    "CREATE SECRET (TYPE quack, TOKEN ?token, SCOPE ?uri)", token = token, uri = uri)
  DBI::dbExecute(con, secret)
  db <- CanardConnection(con = con, uri = uri)
  runtime <- ca_runtime(db)
  if (!identical(runtime$schema_version, 1L)) stop("Unsupported CanardAbsurd schema version")
  ready <- TRUE
  db
}

#' Close a client or server
#' @param db A handle returned by [ca_open()], [ca_serve()], or [ca_connect()].
#' @return `NULL`, invisibly. Closing an already closed handle is harmless.
#' @export
ca_close <- function(db) {
  stopifnot(S7::S7_inherits(db, CanardConnection))
  if (!DBI::dbIsValid(db@con)) return(invisible(NULL))
  on.exit(DBI::dbDisconnect(db@con, shutdown = TRUE))
  if (db@server) {
    sql <- DBI::sqlInterpolate(db@con, "CALL quack_stop(?uri)", uri = db@uri)
    DBI::dbGetQuery(db@con, sql)
  }
  invisible(NULL)
}

#' Inspect the executing database runtime
#' @inheritParams ca_close
#' @return A one-row data frame with server-side DuckDB, loaded Quack, and schema
#'   versions. The Quack version is missing for local databases without Quack.
#' @export
ca_runtime <- function(db) {
  .ca_query(db, "runtime")
}
