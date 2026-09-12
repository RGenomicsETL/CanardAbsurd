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
  ca_fail(task, "retry")
  task <- ca_claim(db)
  expect_identical(ca_step(task, key, function() stop("must replay")), value)
  expect_identical(calls, 1L)
  ca_complete(task, value)
  expect_identical(ca_inspect(db, id)$result, value)
})
