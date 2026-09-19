library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

# Quote-heavy values remain native and identical across execution and replay.
local({
  db <- local_database()
  unit <- paste0("'\"\\", "\n", "\t", "\r", "\b", "\f", intToUtf8(233L))
  value <- strrep(unit, 200L)
  key <- "quote'\"\\/step"
  id <- ca_spawn(db, "work", value)
  task <- ca_claim(db)
  calls <- 0L
  fn <- function() {
    calls <<- calls + 1L
    value
  }
  expect_identical(task@input, value)
  expect_identical(ca_step(task, key, fn), value)
  stored <- DBI::dbGetQuery(db@con,
    "SELECT input::VARCHAR AS input FROM canard_absurd.tasks WHERE id = ?", params = list(id))
  expect_identical(stored$input, value)
  checkpoint <- DBI::dbGetQuery(db@con,
    "SELECT name, kind, value::VARCHAR AS value FROM canard_absurd.task_checkpoints WHERE task_id = ?",
    params = list(id))
  expect_identical(checkpoint$name, key)
  expect_identical(checkpoint$kind, "step")
  expect_identical(checkpoint$value, value)
  ca_fail(task, "replay")
  task <- ca_claim(db)
  expect_identical(ca_step(task, key, fn), value)
  expect_identical(calls, 1L)
  ca_complete(task, value)
  record <- ca_inspect(db, id)
  expect_identical(record$result, value)
  expect_identical(record$checkpoints[[key]], list(kind = "step", value = value))
})

# Inputs and cumulative checkpoints have no package byte quota.
local({
  db <- local_database()
  input <- strrep("x", 2L * 1024L^2L)
  value <- strrep("a", 9L * 1024L^2L)
  id <- ca_spawn(db, "large", input)
  task <- ca_claim(db, lease_seconds = 120)
  expect_identical(task@input, input)
  expect_identical(ca_step(task, "first", function() value), value)
  expect_identical(ca_step(task, "second", function() value), value)
  ca_fail(task, "resume")
  task <- ca_claim(db, lease_seconds = 120)
  expect_identical(ca_step(task, "second", function() stop("must replay")), value)
  ca_complete(task, value)
  record <- ca_inspect(db, id)
  expect_identical(record$input, input)
  expect_identical(record$result, value)
  expect_identical(names(record$checkpoints), c("first", "second"))
})

# Completion acknowledges a BLOB without returning its contents.
local({
  db <- local_database()
  value <- raw(1024L * 1024L)
  id <- ca_spawn(db, "blob")
  task <- ca_claim(db)
  query <- db@query
  replies <- list()
  db@query <- function(sql) {
    reply <- query(sql)
    replies[[length(replies) + 1L]] <<- reply
    reply
  }
  task@db <- db
  expect_identical(withVisible(ca_complete(task, value)), list(value = NULL, visible = FALSE))
  expect_length(replies, 1L)
  expect_identical(replies[[1L]], data.frame(changed = 1L, id = id, value = NA))
  expect_identical(ca_inspect(db, id)$result, value)
})

# Checkpoint entry returns only the requested descriptor before a typed read.
local({
  db <- local_database()
  id <- ca_spawn(db, "work")
  task <- ca_claim(db)
  ca_step(task, "first", function() 1L)
  ca_step(task, "second", function() strrep("x", 100000L))
  ca_fail(task, "replay")
  task <- ca_claim(db)
  query <- db@query
  replies <- list()
  db@query <- function(sql) {
    reply <- query(sql)
    replies[[length(replies) + 1L]] <<- reply
    reply
  }
  task@db <- db
  expect_identical(ca_step(task, "first", function() stop("must replay")), 1L)
  expect_identical(names(replies[[1L]]),
    c("changed", "id", "checkpoint_kind", "checkpoint_rtype"))
  expect_identical(replies[[1L]]$checkpoint_kind, "step")
  expect_identical(names(replies[[2L]]), "value")
  expect_identical(replies[[2L]]$value, 1L)
  ca_complete(task)
  expect_identical(ca_inspect(db, id)$state, "completed")
})
