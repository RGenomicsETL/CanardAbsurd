library(CanardAbsurd)
source(system.file("tinytest", "helpers.R", package = "CanardAbsurd"), local = TRUE)

if (!quack_available()) {
  exit_file("Quack process tests require Quack, callr, withr, and parallelly")
}

# Explicit serialization carries R objects through BLOBs between fresh workers.
local({
  remote <- local_quack()
  shift <- function(x) x + offset
  environment(shift) <- list2env(list(offset = 7L), parent = baseenv())
  value <- list(matrix = matrix(1:6, 2L), expression = quote(x + 1L), shift = shift)
  hooks <- function() sakura::serial_config("buffer_reader",
    function(reader) serialize(list(bytes = reader$bytes, position = seek(reader$con)), NULL),
    function(bytes) {
      saved <- unserialize(bytes)
      reader <- new.env(parent = emptyenv())
      reader$bytes <- saved$bytes
      reader$con <- rawConnection(saved$bytes, "rb")
      seek(reader$con, saved$position)
      class(reader) <- "buffer_reader"
      reader
    })
  environment(hooks) <- baseenv()

  for (codec in c("rds", "qs2", "sakura")) {
    if (codec != "rds" && !requireNamespace(codec, quietly = TRUE)) {
      next
    }
    bytes <- switch(codec,
      rds = serialize(value, NULL),
      qs2 = qs2::qs_serialize(value, nthreads = 1L),
      sakura = local({
        reader <- new.env(parent = emptyenv())
        reader$bytes <- charToRaw("alpha\nbeta\n")
        reader$con <- rawConnection(reader$bytes, "rb")
        withr::defer(close(reader$con))
        class(reader) <- "buffer_reader"
        expect_identical(readLines(reader$con, n = 1L), "alpha")
        value$reader <- reader
        sakura::serialize(value, hooks())
      }))
    id <- ca_spawn(remote$db, paste0(codec, "-v1"), bytes)
    for (resume in c(FALSE, TRUE)) {
      result <- callr::r(function(uri, codec, resume, hooks) {
        library(CanardAbsurd)
        db <- ca_connect(uri, token = "test-token")
        on.exit(ca_close(db))
        task <- ca_claim(db)
        bytes <- ca_step(task, "object", function() {
          if (resume) stop("The checkpoint callback must not execute on replay")
          task@input
        })
        object <- switch(codec,
          rds = unserialize(bytes),
          qs2 = qs2::qs_deserialize(bytes, nthreads = 1L),
          sakura = sakura::unserialize(bytes, hooks()))
        observed <- list(matrix = object$matrix, expression = object$expression,
          shifted = object$shift(5L))
        if (codec == "sakura") {
          on.exit(close(object$reader$con), add = TRUE)
          observed$line <- readLines(object$reader$con, n = 1L)
        }
        if (resume) ca_complete(task, bytes) else ca_fail(task, "resume in another worker")
        observed
      }, args = list(remote$uri, codec, resume, hooks), libpath = .libPaths(),
        system_profile = TRUE)
      expect_identical(result$matrix, value$matrix)
      expect_identical(result$expression, value$expression)
      expect_identical(result$shifted, 12L)
      if (codec == "sakura") expect_identical(result$line, "beta")
    }
    record <- ca_inspect(remote$db, id)
    expect_identical(record$input, bytes)
    expect_identical(record$result, bytes)
    expect_identical(record$checkpoints$object$value, bytes)
  }
})
