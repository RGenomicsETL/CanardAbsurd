.ca_query <- function(db, statement, params = list()) {
  stopifnot(S7::S7_inherits(db, CanardConnection))
  path <- system.file("sql", paste0(statement, ".sql"),
    package = "CanardAbsurd", mustWork = TRUE)
  sql <- DBI::sqlInterpolate(db@con,
    paste(readLines(path, warn = FALSE), collapse = "\n"), .dots = params)
  for (attempt in seq_len(8L)) {
    result <- tryCatch({
      if (db@server || !nzchar(db@uri)) {
        DBI::dbGetQuery(db@con, sql)
      } else {
        DBI::dbGetQuery(db@con, "SELECT * FROM quack_query(?, ?)",
          params = list(db@uri, as.character(sql)))
      }
    }, error = identity)
    if (!inherits(result, "error")) return(result)
    message <- conditionMessage(result)
    insert_race <- identical(statement, "spawn") && grepl(paste0(
      'Duplicate key "id:.*violates primary key constraint|',
      'Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key'
    ), message)
    conflict <- grepl("Conflict on (update|tuple deletion)!", message) || insert_race
    if (!conflict || attempt == 8L) stop(result)
    Sys.sleep(min(0.001 * 2^(attempt - 1L), 0.05))
  }
}

.ca_abort <- function(message, class) {
  stop(structure(list(message = message, call = NULL),
    class = c(class, "error", "condition")))
}

.ca_json <- function(value) {
  encoded <- as.character(jsonlite::toJSON(value, auto_unbox = TRUE,
    null = "null", na = "null", digits = NA))
  if (nchar(encoded, type = "bytes") > 1048576L) {
    stop("A JSON value may not exceed 1 MiB", call. = FALSE)
  }
  encoded
}

.ca_owned <- function(task, statement, ...) {
  stopifnot(S7::S7_inherits(task, CanardTask))
  rows <- .ca_query(task@db, statement,
    c(list(id = task@id, token = task@token), list(...)))
  if (nrow(rows) != 1L) {
    .ca_abort(paste("Task lease is no longer owned:", task@id), "canard_lease_lost")
  }
  rows
}
