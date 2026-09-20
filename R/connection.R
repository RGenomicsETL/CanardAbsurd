.ca_services <- function() {
  services <- new.env(parent = emptyenv())
  services$coordinator_loaded <- FALSE
  services$coordinator <- FALSE
  services
}

#' Open a local workflow database
#'
#' Opens a workflow database in this process, installing schema version 1 or
#' checking the existing one. The handle owns its DBI connection; close it with
#' [ca_close()]. Extensions are never downloaded, and the DuckDB home is
#' session-private. A file-backed database has one owning process: others reach
#' it through a Quack server instead of opening the file.
#'
#' @param path DuckDB database path, or `":memory:"`.
#' @param allow_unsigned_extensions Development-only opt-in for loading an
#'   unsigned build of the native coordinator. It lets any SQL on this database
#'   load arbitrary native code into the owning process. On a served database,
#'   that includes every Quack token holder. Leave it `FALSE` in deployments.
#' @importFrom duckdb duckdb
#' @return An S7 `CanardConnection` handle.
#' @export
#' @examples
#' db <- ca_open()
#' id <- ca_spawn(db, "sum", list(x = 2, y = 3))
#' ca_work(db, list(sum = function(input, ctx) input$x + input$y), max_tasks = 1)
#' ca_inspect(db, id)$result
#' ca_close(db)
ca_open <- function(path = ":memory:", allow_unsigned_extensions = FALSE) {
  .ca_open_database(path, shared_home = FALSE, allow_unsigned_extensions)
}

.ca_open_database <- function(path, shared_home, allow_unsigned_extensions) {
  request <- .ca_input(CanardDatabase, path = path,
    allow_unsigned_extensions = allow_unsigned_extensions)
  config <- list(autoinstall_known_extensions = "false",
    storage_compatibility_version = "v1.5.0")
  if (request@allow_unsigned_extensions) config$allow_unsigned_extensions <- "true"
  # Configure the driver: dbConnect() ignores config for an in-memory database.
  con <- DBI::dbConnect(duckdb(dbdir = request@path, shared_home = shared_home, config = config),
    bigint = "integer64")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  db <- CanardConnection(con = con, query = function(sql) DBI::dbGetQuery(con, sql),
    services = .ca_services())
  schema <- readLines(system.file("sql", "schema.sql", package = "CanardAbsurd",
    mustWork = TRUE), warn = FALSE)
  DBI::dbWithTransaction(con, {
    installed <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM information_schema.tables
      WHERE table_catalog = current_database() AND table_schema = 'canard_absurd'
        AND table_name = 'schema_version'")$n[[1L]]
    if (installed == 1L) {
      version <- DBI::dbGetQuery(con, "SELECT version FROM canard_absurd.schema_version")
      if (!identical(version$version, 1L)) {
        stop(errorCondition("Unsupported CanardAbsurd schema version",
          class = c("canard_schema_error", "canard_error"), version = version$version))
      }
    }
    for (sql in strsplit(paste(schema, collapse = "\n"), ";", fixed = TRUE)[[1L]]) {
      if (nzchar(trimws(sql))) DBI::dbExecute(con, sql)
    }
  })
  on.exit(NULL)
  db
}

.ca_load_extension <- function(con, extension, name, label) {
  tryCatch({
    target <- name
    if (!is.null(extension)) {
      target <- DBI::dbQuoteString(con, normalizePath(extension, mustWork = TRUE))
    }
    DBI::dbExecute(con, paste("LOAD", target))
  }, error = function(e) {
    stop(errorCondition(paste0("Unable to load ", label, ": ", conditionMessage(e)),
      class = c("canard_extension_error", "canard_error"), parent = e))
  })
  invisible(NULL)
}

.ca_load_quack <- function(con, extension) {
  # Quack clients load httpfs. A session-private home cannot supply it, so an
  # explicit Quack path also loads httpfs installed beside it.
  httpfs <- if (!is.null(extension)) file.path(dirname(extension), "httpfs.duckdb_extension")
  if (!is.null(httpfs) && file.exists(httpfs)) .ca_load_extension(con, httpfs, "httpfs", "httpfs")
  .ca_load_extension(con, extension, "quack", "Quack")
}

#' Serve a workflow database over Quack
#'
#' The calling process owns the database and must stay alive. Quack answers SQL
#' on its own server threads; handlers run in clients. Put non-local endpoints
#' behind authenticated, TLS-terminating ingress. With `extension = NULL` the
#' preinstalled `quack` extension is loaded from DuckDB's shared home; a path
#' loads that build into session-private storage.
#'
#' @inheritParams ca_open
#' @param uri Quack endpoint, including a port when not using the default.
#' @param token Shared authentication token. Quack currently requires at least
#'   four characters. A holder can run any SQL, including file access, with the
#'   server process's privileges; give it only to trusted workers and services.
#' @param extension Optional path to a compatible signed Quack extension.
#'   `NULL` loads an explicitly preinstalled `quack` extension.
#' @return A local S7 `CanardConnection` handle owning the server.
#' @export
ca_serve <- function(path, uri = "quack:127.0.0.1:9494", token, extension = NULL,
                     allow_unsigned_extensions = FALSE) {
  endpoint <- .ca_input(CanardEndpoint, uri = uri, token = token, extension = extension)
  db <- .ca_open_database(path, shared_home = is.null(endpoint@extension),
    allow_unsigned_extensions)
  on.exit(ca_close(db))
  .ca_load_quack(db@con, endpoint@extension)
  sql <- DBI::sqlInterpolate(db@con,
    "CALL quack_serve(?uri, token = ?token)", uri = endpoint@uri, token = endpoint@token)
  started <- DBI::dbGetQuery(db@con, sql)
  db@uri <- started$listen_uri[[1L]]
  db@server <- TRUE
  on.exit(NULL)
  db
}

#' Connect to a workflow server
#'
#' Connects to a [ca_serve()] endpoint. The client holds only an in-memory
#' DuckDB connection, never a second writable handle to the server's file. Quack
#' clients also load DuckDB's `httpfs` extension; install both explicitly,
#' because connections never download extensions. With `extension = NULL` both
#' come from DuckDB's shared home; a path loads that build plus any
#' `httpfs.duckdb_extension` beside it into session-private storage.
#'
#' @inheritParams ca_serve
#' @return An S7 `CanardConnection` handle.
#' @export
ca_connect <- function(uri = "quack:127.0.0.1:9494", token, extension = NULL) {
  endpoint <- .ca_input(CanardEndpoint, uri = uri, token = token, extension = extension)
  con <- DBI::dbConnect(duckdb(shared_home = is.null(endpoint@extension),
    config = list(autoinstall_known_extensions = "false")), bigint = "integer64")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  db <- CanardConnection(con = con, uri = uri, services = .ca_services(),
    query = function(sql) DBI::dbGetQuery(con, "SELECT * FROM quack_query(?, ?)",
      params = list(uri, as.character(sql))))
  .ca_load_quack(con, endpoint@extension)
  secret <- DBI::sqlInterpolate(con,
    "CREATE SECRET (TYPE quack, TOKEN ?token, SCOPE ?uri)",
    token = endpoint@token, uri = endpoint@uri)
  DBI::dbExecute(con, secret)
  runtime <- ca_runtime(db)
  if (!identical(runtime$schema_version, 1L)) {
    stop(errorCondition("Unsupported CanardAbsurd schema version",
      class = c("canard_schema_error", "canard_error"), version = runtime$schema_version))
  }
  on.exit(NULL)
  db
}

#' Close a client or server
#'
#' Stops a server handle's Quack endpoint, stops a loaded native coordinator,
#' and closes the DuckDB connection. Every step is attempted even if an earlier
#' one fails, and the failures are raised together as `canard_close_error`.
#'
#' After a statement on the connection has failed, DuckDB R holds the database,
#' and its file lock, until R garbage-collects that statement; only then can
#' another process open a file-backed database.
#'
#' @param db A handle returned by [ca_open()], [ca_serve()], or [ca_connect()].
#' @return `NULL`, invisibly. Closing an already closed handle is harmless.
#' @export
ca_close <- function(db) {
  if (!DBI::dbIsValid(db@con)) return(invisible(NULL))
  errors <- list()
  attempt <- function(step) {
    tryCatch(step, error = function(e) errors[[length(errors) + 1L]] <<- e)
  }
  if (db@server) {
    attempt(DBI::dbGetQuery(db@con,
      DBI::sqlInterpolate(db@con, "CALL quack_stop(?uri)", uri = db@uri)))
  }
  if (db@services$coordinator) attempt(ca_coordinator_stop(db))
  attempt(DBI::dbDisconnect(db@con, shutdown = TRUE))
  if (length(errors) > 0L) {
    stop(errorCondition(paste("Unable to close cleanly:", conditionMessage(errors[[1L]])),
      class = c("canard_close_error", "canard_error"), parent = errors[[1L]], errors = errors))
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
