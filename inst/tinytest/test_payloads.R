library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Quote-heavy JSON has the same value on execution, replay, and completion.
local({
  db <- local_database()
  value <- strrep('"', 300000L)
  id <- ca_spawn(db, "work", value)
  task <- ca_claim(db)
  key <- "a/~0~1/['\"\n\u00e9"
  calls <- 0L
  expect_identical(ca_step(task, key, function() {
    calls <<- calls + 1L
    value
  }), value)
  task_row <- DBI::dbGetQuery(db@con,
    "SELECT input, state FROM canard_absurd.tasks WHERE id = ?", params = list(id))
  expect_identical(task_row$state, "running")
  expect_identical(jsonlite::fromJSON(task_row$input[[1L]]), value)
  checkpoint <- DBI::dbGetQuery(db@con,
    "SELECT name, kind, value_json FROM canard_absurd.task_checkpoints WHERE task_id = ?",
    params = list(id))
  expect_identical(checkpoint$name, key)
  expect_identical(checkpoint$kind, "step")
  expect_identical(jsonlite::fromJSON(checkpoint$value_json[[1L]]), value)
  ca_fail(task, "retry")
  task <- ca_claim(db)
  expect_identical(ca_step(task, key, function() stop("must replay")), value)
  expect_identical(calls, 1L)
  ca_complete(task, value)
  record <- ca_inspect(db, id)
  expect_identical(record$checkpoints[[key]]$kind, "step")
  expect_identical(jsonlite::fromJSON(record$checkpoints[[key]]$json), value)
  expect_identical(record$result, value)
})

# Storage bounds apply to input and each task's checkpoint map.
local({
  db <- local_database()
  expect_error(ca_spawn(db, "work", strrep("x", 1048576L)), class = "canard_storage_error")
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  value <- strrep("x", 9L * 1024L * 1024L)
  expect_identical(ca_step(task, "first", function() value), value)
  expect_error(ca_step(task, "second", function() value), class = "canard_storage_error")
  record <- ca_inspect(db, id)
  expect_identical(names(record$checkpoints), "first")
  expect_identical(record$state, "running")
  expect_identical(record$failures, 0L)
  ca_complete(task, value)
  expect_identical(ca_inspect(db, id)$result, value)
})

# Heartbeats return only acknowledgement data; step entry selects one checkpoint.
local({
  db <- local_database()
  ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "first", function() 1)
  ca_step(task, "second", function() 2)
  ca_fail(task, "retry")
  task <- ca_claim(db)
  execute <- db@query
  reply <- NULL
  db@query <- function(sql) {
    reply <<- execute(sql)
    reply
  }
  task@db <- db
  ca_heartbeat(task)
  expect_identical(names(reply), c("changed", "id"))
  expect_equal(reply$changed, 1)
  expect_identical(ca_step(task, "first", function() stop("must replay")), 1L)
  expect_identical(names(reply),
    c("changed", "id", "checkpoint_kind", "checkpoint_json"))
  expect_identical(reply$checkpoint_kind, "step")
  expect_identical(reply$checkpoint_json, "1")
})
