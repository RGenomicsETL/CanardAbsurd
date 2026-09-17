local_database <- function(path = ":memory:", .local_envir = parent.frame()) {
  db <- ca_open(path)
  withr::defer(ca_close(db), envir = .local_envir)
  db
}

wait_until <- function(predicate, timeout = 15) {
  deadline <- proc.time()[["elapsed"]] + timeout
  while (!predicate()) {
    if (proc.time()[["elapsed"]] >= deadline) stop("Timed out waiting for test condition")
    Sys.sleep(0.02)
  }
  invisible(NULL)
}

quack_available <- function() {
  if (!requireNamespace("callr", quietly = TRUE) ||
      !requireNamespace("withr", quietly = TRUE) ||
      !requireNamespace("parallelly", quietly = TRUE)) return(FALSE)
  con <- DBI::dbConnect(duckdb::duckdb(shared_home = TRUE),
    config = list(autoinstall_known_extensions = "false"))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  info <- DBI::dbGetQuery(con,
    "SELECT installed FROM duckdb_extensions() WHERE extension_name = 'quack'")
  installed <- isTRUE(info$installed[[1L]])
  if (!installed && identical(Sys.getenv("CANARDABSURD_REQUIRE_QUACK"), "true")) {
    stop("Quack is required for this test run")
  }
  installed
}

local_quack <- function(.local_envir = parent.frame()) {
  directory <- tempfile("canard-quack-")
  dir.create(directory)
  uri <- sprintf("quack:127.0.0.1:%d", parallelly::freePort())
  server <- callr::r_bg(function(directory, uri) {
    library(CanardAbsurd)
    db <- ca_serve(file.path(directory, "tasks.duckdb"), uri, token = "test-token")
    on.exit(ca_close(db))
    saveRDS(ca_runtime(db), file.path(directory, "runtime.rds"))
    file.create(file.path(directory, "ready"))
    while (!file.exists(file.path(directory, "stop"))) Sys.sleep(0.02)
  }, args = list(directory, uri), libpath = .libPaths(), supervise = TRUE)
  withr::defer({
    file.create(file.path(directory, "stop"))
    server$wait(3000)
    if (server$is_alive()) server$kill()
    unlink(directory, recursive = TRUE)
  }, envir = .local_envir)
  wait_until(function() file.exists(file.path(directory, "ready")) || !server$is_alive())
  if (!server$is_alive()) server$get_result()
  client <- ca_connect(uri, "test-token")
  withr::defer(ca_close(client), envir = .local_envir)
  list(db = client, server = server, uri = uri, directory = directory,
    runtime = readRDS(file.path(directory, "runtime.rds")))
}
