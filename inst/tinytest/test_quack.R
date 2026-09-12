library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

if (!requireNamespace("callr", quietly = TRUE) ||
    !requireNamespace("withr", quietly = TRUE) ||
    !requireNamespace("parallelly", quietly = TRUE)) {
  exit_file("Quack process tests require callr, withr, and parallelly")
}
quack_installed <- local({
  con <- DBI::dbConnect(duckdb::duckdb(),
    config = list(autoinstall_known_extensions = "false"))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  info <- DBI::dbGetQuery(con,
    "SELECT installed FROM duckdb_extensions() WHERE extension_name = 'quack'")
  isTRUE(info$installed[[1L]])
})
if (!quack_installed) {
  if (identical(Sys.getenv("CANARDABSURD_REQUIRE_QUACK"), "true")) {
    stop("Quack is required for this test run")
  }
  exit_file("Quack is not installed for this DuckDB runtime")
}

# A real remote transaction conflict offers a statement-scoped restart.
local({
  path <- tempfile(fileext = ".duckdb")
  on.exit(unlink(c(path, paste0(path, ".wal"))))
  uri <- sprintf("quack:127.0.0.1:%d", parallelly::freePort())
  server <- ca_serve(path, uri, token = "test-token")
  on.exit(ca_close(server), add = TRUE, after = FALSE)
  db <- ca_connect(uri, token = "test-token")
  on.exit(ca_close(db), add = TRUE, after = FALSE)
  id <- ca_spawn(db, "work", id = "blocked")
  DBI::dbBegin(server@con)
  DBI::dbExecute(server@con, "UPDATE canard_absurd.tasks SET priority = 1 WHERE id = 'blocked'")
  event <- NULL
  task <- withCallingHandlers(ca_claim(db), canard_retryable = function(conflict) {
    event <<- conflict
    DBI::dbRollback(server@con)
    invokeRestart("canard_retry")
  })
  expect_identical(task@id, id)
  expect_identical(task@attempt, 1L)
  expect_identical(event$operation, "claim")
  expect_identical(event$attempts, 1L)
  expect_true(inherits(event$parent, "error"))
  ca_complete(task, 42)
  expect_identical(ca_inspect(db, id)$failures, 0L)
})

# Remote statements preserve JSON and schema semantics; authentication is enforced.
local({
  fixture <- local_quack()
  db <- fixture$db
  expect_identical(ca_runtime(db), fixture$runtime)
  expect_true(nzchar(fixture$runtime$quack_version))
  expect_error(ca_connect(fixture$uri, "wrong-token"))
  id <- ca_spawn(db, "work", list(x = NULL, quote = "'\"\\"), id = "remote'quoted")
  task <- ca_claim(db)
  expect_identical(ca_run(task, function(input, ctx) {
    ca_step(ctx, "a/'\"", function() input)
  })$status, "completed")
  expect_identical(ca_inspect(db, id)$result, list(x = NULL, quote = "'\"\\"))
  expect_identical(ca_inspect(db, id)$attempt, 1L)
  ca_spawn(db, "sleep", id = "sleep")
  handler <- function(input, ctx) {
    ca_sleep(ctx, "pause", 0.05)
    42
  }
  expect_identical(ca_work(db, list(sleep = handler), max_tasks = 2,
    idle_timeout = 2, poll_seconds = 0.02), 2L)
  expect_equal(ca_inspect(db, "sleep")$result, 42)
})

# Independent R workers race on server-owned claims and idempotent submission.
local({
  fixture <- local_quack()
  for (i in seq_len(24L)) {
    ca_spawn(fixture$db, "work", list(n = i), id = paste0("job-", i))
  }
  workers <- list()
  withr::defer(for (p in workers) if (p$is_alive()) p$kill())
  for (i in seq_len(3L)) {
    workers[[i]] <- callr::r_bg(function(uri, directory, i) {
      library(CanardAbsurd)
      db <- ca_connect(uri, "test-token")
      on.exit(ca_close(db))
      file.create(file.path(directory, paste0("worker-", i)))
      while (!file.exists(file.path(directory, "go"))) Sys.sleep(0.01)
      withCallingHandlers({
        ca_spawn(db, "work", list(n = 0), id = "dedupe", queue = "dedupe")
        ca_work(db, list(work = function(input, ctx) {
          ca_step(ctx, "compute", function() {
            cat(ctx@id, "\n", file = file.path(directory, paste0("effects-", i)), append = TRUE)
            input$n * 2
          })
        }), idle_timeout = 0.5, poll_seconds = 0.02, lease_seconds = 10)
      }, canard_retryable = function(conflict) {
        if (conflict$attempts <= 20L) {
          Sys.sleep(runif(1L, 0.001, 0.01))
          invokeRestart("canard_retry")
        }
      })
    }, args = list(fixture$uri, fixture$directory, i), libpath = .libPaths(), supervise = TRUE)
  }
  wait_until(function() all(file.exists(file.path(fixture$directory,
    paste0("worker-", seq_len(3L))))))
  file.create(file.path(fixture$directory, "go"))
  for (p in workers) p$wait(30000)
  expect_false(any(vapply(workers, function(p) p$is_alive(), logical(1L))))
  counts <- vapply(workers, function(p) p$get_result(), integer(1L))
  expect_identical(sum(counts), 24L)
  effects <- unlist(lapply(list.files(fixture$directory, pattern = "^effects-", full.names = TRUE),
    readLines, warn = FALSE), use.names = FALSE)
  expect_identical(sort(trimws(effects)), sort(paste0("job-", seq_len(24L))))
  for (i in seq_len(24L)) {
    record <- ca_inspect(fixture$db, paste0("job-", i))
    expect_identical(record$state, "completed")
    expect_identical(record$attempt, 1L)
    expect_equal(record$result, i * 2)
  }
  expect_identical(ca_inspect(fixture$db, "dedupe")$attempt, 0L)
})

# A killed worker leaves a leased task whose completed step replays on recovery.
local({
  fixture <- local_quack()
  id <- ca_spawn(fixture$db, "work", id = "recover")
  worker <- callr::r_bg(function(uri, directory) {
    library(CanardAbsurd)
    db <- ca_connect(uri, "test-token")
    on.exit(ca_close(db))
    task <- ca_claim(db, lease_seconds = 1)
    ca_step(task, "saved", function() {
      cat("effect\n", file = file.path(directory, "effect"), append = TRUE)
      42
    })
    file.create(file.path(directory, "claimed"))
    repeat Sys.sleep(0.05)
  }, args = list(fixture$uri, fixture$directory), libpath = .libPaths(), supervise = TRUE)
  withr::defer(if (worker$is_alive()) worker$kill())
  wait_until(function() file.exists(file.path(fixture$directory, "claimed")) || !worker$is_alive())
  if (!worker$is_alive()) worker$get_result()
  expect_true(worker$kill())
  worker$wait(5000)
  task <- NULL
  wait_until(function() {
    task <<- ca_claim(fixture$db)
    !is.null(task)
  })
  expect_identical(task@attempt, 2L)
  expect_identical(ca_run(task, function(input, ctx) {
    ca_step(ctx, "saved", function() stop("completed step must replay"))
  })$status, "completed")
  expect_identical(readLines(file.path(fixture$directory, "effect")), "effect")
  expect_equal(ca_inspect(fixture$db, id)$result, 42)
  expect_identical(ca_inspect(fixture$db, id)$failures, 1L)
})

# A live but expired process cannot complete after another worker takes ownership.
local({
  fixture <- local_quack()
  ca_spawn(fixture$db, "work", id = "late")
  late <- callr::r_bg(function(uri, directory) {
    library(CanardAbsurd)
    db <- ca_connect(uri, "test-token")
    on.exit(ca_close(db))
    task <- ca_claim(db, lease_seconds = 0.5)
    file.create(file.path(directory, "old-claim"))
    while (!file.exists(file.path(directory, "release"))) Sys.sleep(0.01)
    tryCatch({ ca_complete(task, "late"); "accepted" },
      canard_lease_lost = function(e) "fenced")
  }, args = list(fixture$uri, fixture$directory), libpath = .libPaths(), supervise = TRUE)
  withr::defer(if (late$is_alive()) late$kill())
  wait_until(function() file.exists(file.path(fixture$directory, "old-claim")) || !late$is_alive())
  if (!late$is_alive()) late$get_result()
  current <- NULL
  wait_until(function() {
    current <<- ca_claim(fixture$db)
    !is.null(current)
  })
  ca_complete(current, "current")
  file.create(file.path(fixture$directory, "release"))
  late$wait(10000)
  expect_identical(late$get_result(), "fenced")
  expect_identical(ca_inspect(fixture$db, "late")$result, "current")
})

# Committed progress survives an abrupt database-host process death.
local({
  fixture <- local_quack()
  ca_spawn(fixture$db, "work", id = "restart")
  task <- ca_claim(fixture$db)
  ca_step(task, "saved", function() 42)
  ca_fail(task, "resume after restart")
  ca_close(fixture$db)
  fixture$server$kill()
  fixture$server$wait(5000)
  server <- ca_serve(file.path(fixture$directory, "tasks.duckdb"),
    fixture$uri, token = "test-token")
  withr::defer(ca_close(server))
  client <- ca_connect(fixture$uri, "test-token")
  withr::defer(ca_close(client))
  expect_identical(ca_run(ca_claim(client), function(input, ctx) {
    ca_step(ctx, "saved", function() stop("persisted step must replay"))
  })$status, "completed")
  expect_equal(ca_inspect(client, "restart")$result, 42)
})
