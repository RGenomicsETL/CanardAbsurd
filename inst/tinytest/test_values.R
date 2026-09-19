library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

case_collisions <- list(
  list(A = 1L, a = 2L),
  data.frame(A = 1L, a = 2L),
  list(nested = list(Gene.ID = 1L, gene.id = 2L))
)

values <- list(
  null = NULL, logical = c(TRUE, NA, FALSE), integer = c(1L, NA_integer_),
  double = c(1.2345678901234567, Inf, -Inf, NaN, NA_real_),
  missing = list(NA, NA_integer_, NA_real_, NA_character_, as.Date(NA)),
  all_missing = c(NA_integer_, NA_integer_),
  text = c("a'b", "a\\b", "a\nb", intToUtf8(233L), NA_character_),
  bytes = as.raw(c(0, 255, 1)), empty_bytes = raw(),
  empty = list(logical(), integer(), numeric(), character(), list(),
    setNames(list(), character()), data.frame(), data.frame(x = integer())),
  nested = list(NULL, list(a = NA_integer_, b = list(NULL, 1L, "x"))),
  named = setNames(c(1L, NA_integer_), c("one", "two")),
  atomic_case_names = c(A = 1L, a = 2L),
  preserved_case = list(A = 1L, b = 2L,
    unicode = setNames(list(3L, 4L), intToUtf8(c(196L, 228L), multiple = TRUE))),
  date = as.Date(c("2026-01-02", NA)),
  timestamp = as.POSIXct(c("2026-01-02 10:20:30", NA), tz = "Europe/Paris"),
  duration = as.difftime(c(1, NA, 2), units = "hours"),
  factor = factor(c("a", NA), levels = c("a", "b")), ordered = ordered(c("b", "a")),
  integer64 = bit64::as.integer64(c("9007199254740993", NA)),
  frame = data.frame(i = c(1L, NA_integer_), day = as.Date(c("2026-01-02", NA)),
    nested = I(list(NULL, list(a = 4))), row.names = c("first", "second")),
  no_columns = data.frame(row.names = c("first", "second")),
  integer_rows = data.frame(x = 1:2, row.names = 7:8),
  sequential_rows = data.frame(x = 1:2, row.names = 1:2),
  integer_rows_no_columns = data.frame(row.names = 7:8),
  single_integer_row = data.frame(x = 1L, row.names = 7L),
  integer_temporal = list(structure(1L, class = "Date"),
    structure(c(1L, NA_integer_), class = c("POSIXct", "POSIXt")),
    as.difftime(c(1L, 2147483647L), units = "hours")),
  attributes = list(factor(character()), factor("a"),
    structure(1.2, class = c("POSIXct", "POSIXt")),
    as.POSIXct(character(), tz = "UTC"),
    setNames(integer(), character()),
    data.frame(x = 1L), data.frame(x = 1L, row.names = "custom"))
)

# Indexed native values survive checkpointing and a file reopen. Skipped on
# Windows, where CI reliably hits duckdb-r's unprotected VARIANT conversion
# (duckdb/duckdb-r#2750); Linux hits it only occasionally.
if (.Platform$OS.type != "windows") local({
  path <- tempfile(fileext = ".duckdb")
  withr::defer(unlink(c(path, paste0(path, ".wal"))))
  db <- local_database(path)
  id <- ca_spawn(db, "persist", values)
  task <- ca_claim(db)
  ca_step(task, "saved", function() values)
  ca_fail(task, "restart")
  DBI::dbExecute(db@con, "CHECKPOINT")
  ca_close(db)
  db <- local_database(path)
  task <- ca_claim(db)
  expect_identical(task@input, values)
  expect_identical(ca_step(task, "saved", function() stop("must replay")), values)
  ca_complete(task, values)
  expect_identical(ca_inspect(db, id)$result, values)
  ca_close(db)
})

# Appending after VARIANT shredding and reopening retains indexed payloads.
local({
  path <- tempfile(fileext = ".duckdb")
  withr::defer(unlink(c(path, paste0(path, ".wal"))))
  db <- local_database(path)
  type <- CanardAbsurd:::.ca_payload(list(x = 1L, day = as.Date("2026-01-02")), db@con)$rtype
  for (batch in 0:5) {
    sql <- DBI::sqlInterpolate(db@con, "INSERT INTO canard_absurd.tasks
      (id, queue, name, max_failures, attempt, failures, input, input_rtype, checkpoints)
      SELECT 'bulk-' || i::VARCHAR, 'default', 'bulk', 3, 1, 1,
        {'x': i::INTEGER, 'day': DATE '2026-01-02'}::VARIANT AS input, ?rtype,
        map(['saved'], [struct_pack(kind := 'step', value := input, rtype := ?rtype)])
      FROM range(?start, ?end) ids(i)",
      rtype = type, start = batch * 10000L, end = (batch + 1L) * 10000L)
    DBI::dbExecute(db@con, sql)
    DBI::dbExecute(db@con, "CHECKPOINT")
  }
  id <- ca_spawn(db, "after-shred", list(x = 99L, day = as.Date(NA)))
  ca_close(db)
  db <- local_database(path)
  task <- ca_claim(db, task_names = "after-shred")
  expect_identical(task@input, list(x = 99L, day = as.Date(NA)))
  ca_complete(task, task@input)
  expect_identical(ca_inspect(db, id)$result, task@input)
  record <- ca_inspect(db, "bulk-49999")
  expect_identical(record$input, list(x = 49999L, day = as.Date("2026-01-02")))
  expect_identical(record$checkpoints$saved$value, record$input)
  task <- ca_claim(db, task_names = "bulk")
  expect_identical(ca_step(task, "saved", function() stop("must replay")), task@input)
  ca_complete(task, task@input)
  expect_identical(ca_inspect(db, task@id)$result, task@input)
  ca_close(db)
})

# Unsupported classes and attributes fail before a submission writes to the database.
local({
  db <- local_database()
  query <- db@query
  queries <- 0L
  db@query <- function(sql) {
    queries <<- queries + 1L
    query(sql)
  }
  for (value in list(identity, globalenv(), quote(x + y), matrix(1:4, 2L),
    structure(1, class = "custom"), setNames(list(1, 2), c("x", "x")),
    as.Date(NaN, origin = "1970-01-01"),
    as.POSIXct(NaN, origin = "1970-01-01", tz = "UTC"),
    as.difftime(NaN, units = "secs"),
    structure(1, class = c("POSIXct", "POSIXt"), tzone = NA_character_),
    structure(1, class = c("POSIXct", "POSIXt"), tzone = character()))) {
    expect_error(ca_spawn(db, "bad", value), class = "canard_value_error")
  }
  expect_identical(queries, 0L)
})

# Native mappings are identical across admission, checkpointing, replay and inspection.
for (remote in c(FALSE, TRUE)) local({
  if (remote && !quack_available()) {
    exit_file("Quack process tests require Quack, callr, withr, and parallelly")
    return(invisible(NULL))
  }
  db <- if (remote) local_quack()$db else local_database()

  # Case-colliding fields fail admission without issuing submission SQL.
  query <- db@query
  queries <- 0L
  db@query <- function(sql) {
    queries <<- queries + 1L
    query(sql)
  }
  for (value in case_collisions) {
    expect_error(ca_spawn(db, "invalid", value), class = "canard_value_error")
  }
  expect_identical(queries, 0L)
  db@query <- query

  # Invalid callback values consume a failure, without a checkpoint or result.
  for (value in case_collisions) for (operation in c("step", "result")) {
    id <- ca_spawn(db, "invalid", max_failures = 1L)
    calls <- 0L
    outcome <- ca_run(ca_claim(db), function(input, task) {
      produce <- function() {
        calls <<- calls + 1L
        value
      }
      if (operation == "step") ca_step(task, "invalid", produce) else produce()
    })
    expect_identical(calls, 1L)
    expect_identical(outcome$status, "failed")
    expect_true(inherits(outcome$error, "canard_value_error"))
    expect_true(inherits(outcome$error$parent, "error"))
    expect_false(inherits(outcome$error, "canard_storage_error"))
    record <- ca_inspect(db, id)
    expect_identical(record$state, "failed")
    expect_identical(record$failures, 1L)
    expect_length(record$checkpoints, 0L)
    expect_null(record$result)
  }

  for (i in seq_along(values)) {
    value <- values[[i]]
    id <- ca_spawn(db, "value", value)
    task <- ca_claim(db)
    expect_identical(task@input, value, info = names(values)[[i]])
    expect_identical(ca_step(task, "saved", function() value), value)
    ca_fail(task, "resume")
    task <- ca_claim(db)
    expect_identical(ca_step(task, "saved", function() stop("must replay")), value)
    ca_complete(task, value)
    record <- ca_inspect(db, id)
    expect_identical(record$input, value)
    expect_identical(record$result, value)
    expect_identical(record$checkpoints$saved, list(kind = "step", value = value))
  }
  id <- ca_spawn(db, "bundle", values)
  result <- ca_run(ca_claim(db), function(input, task) ca_step(task, "bundle", function() input))
  expect_identical(result$result, values)
  expect_identical(ca_inspect(db, id)$result, values)
})
