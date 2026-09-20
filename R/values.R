#' Native workflow values
#'
#' Inputs, results and checkpoint values are stored as DuckDB `VARIANT` values,
#' queryable with field access, `variant_extract()` and casts. An R type
#' descriptor stored beside each value records what SQL cannot: `NULL` versus a
#' typed `NA`, vector versus list, data frame versus named list, and names,
#' factor levels, time zones, row names and `I()` wrappers.
#'
#' Supported values are `NULL`; logical, integer, double, character and raw
#' vectors; `Date`, `POSIXct`, `difftime`, factors, ordered factors and
#' `bit64::integer64`; and lists and ordinary data frames composed of those.
#' They map as:
#'
#' * length-one logical, integer, double and character values to the matching
#'   DuckDB scalar type, and longer vectors to typed lists;
#' * raw vectors to BLOB, `Date` to DATE, `POSIXct` to TIMESTAMPTZ, `difftime`
#'   to INTERVAL, factors to VARCHAR, and `integer64` to BIGINT;
#' * named lists and data frames to objects, unnamed lists to arrays.
#'
#' Timestamps and durations are rounded to DuckDB microsecond precision and must
#' be finite or missing; dates must be whole finite days or missing. Double
#' infinities and `NaN` survive, and factor levels must not be missing. Names of
#' list elements and data frame columns become STRUCT fields, so they must be
#' nonempty and unique ignoring ASCII case: `A` and `a` cannot share a container.
#' Atomic vector names are unrestricted.
#'
#' Anything else, including custom classes, matrices, environments and language
#' objects, needs explicit conversion. The package never serializes objects for
#' you, nor downloads extensions to convert them: store your own bytes as a raw
#' vector and decode them in the handler, deserializing only trusted payloads.
#' See `vignette("serialized-values")`. The mappings follow the declared-type
#' approach of [Rducks](https://github.com/RGenomicsETL/Rducks), which is not a
#' dependency.
#'
#' Values are read back through descriptor-typed projections rather than the
#' driver's generic VARIANT decoder; `vignette("native-values")` explains why and
#' lists the driver defects involved. Files need DuckDB storage compatibility
#' v1.5.0. There is no package byte limit, though engine memory, disk and
#' transport still apply.
#'
#' An unsupported value raises `canard_value_error`, a `canard_input_error`
#' carrying the conversion failure in `parent`. A step callback returning one has
#' failed, and no checkpoint is saved for it.
#'
#' @name ca_values
NULL

.ca_types <- c(logical = "BOOLEAN", integer = "INTEGER", double = "DOUBLE",
  character = "VARCHAR", raw = "BLOB", Date = "DATE", POSIXct = "TIMESTAMPTZ",
  difftime = "INTERVAL", factor = "VARCHAR", ordered = "VARCHAR", integer64 = "BIGINT")

#' @rawNamespace S3method(.ca_type, default)
#' @rawNamespace S3method(.ca_type, NULL)
#' @rawNamespace S3method(.ca_type, logical)
#' @rawNamespace S3method(.ca_type, integer)
#' @rawNamespace S3method(.ca_type, numeric)
#' @rawNamespace S3method(.ca_type, character)
#' @rawNamespace S3method(.ca_type, raw)
#' @rawNamespace S3method(.ca_type, Date)
#' @rawNamespace S3method(.ca_type, POSIXct)
#' @rawNamespace S3method(.ca_type, difftime)
#' @rawNamespace S3method(.ca_type, factor)
#' @rawNamespace S3method(.ca_type, integer64)
#' @rawNamespace S3method(.ca_type, AsIs)
#' @rawNamespace S3method(.ca_type, list)
#' @rawNamespace S3method(.ca_type, data.frame)
#' @noRd
.ca_type <- function(value) UseMethod(".ca_type")

.ca_type_info <- function(value, kind, allowed = "names") {
  extra <- setdiff(names(attributes(value)), allowed)
  if (length(extra) > 0L) stop("Unsupported value attributes: ", paste(extra, collapse = ", "))
  out <- list(kind = kind, length = length(value), storage = typeof(value))
  labels <- names(value)
  if (!is.null(labels)) {
    if (anyNA(labels)) stop("Value names must not be missing")
    out$named <- TRUE
    if (length(labels) > 0L) out$names <- labels
  }
  out
}

.ca_type.default <- function(value) {
  stop("Unsupported value class: ", paste(class(value), collapse = ", "))
}
.ca_type.NULL <- function(value) list(kind = "NULL", length = 0L)
.ca_type.logical <- .ca_type.integer <- .ca_type.numeric <- .ca_type.character <-
  .ca_type.raw <- function(value) .ca_type_info(value, typeof(value))

.ca_type.Date <- function(value) {
  if (!identical(class(value), "Date")) stop("Coerce Date subclasses to Date explicitly")
  if (any(is.nan(value) | (!is.na(value) & (!is.finite(value) | unclass(value) != trunc(unclass(value)))))) {
    stop("Dates must be whole finite days or NA")
  }
  .ca_type_info(value, "Date", c("class", "names"))
}
.ca_type.POSIXct <- function(value) {
  if (!identical(class(value), c("POSIXct", "POSIXt"))) stop("Unsupported timestamp class")
  if (any(is.nan(value) | (!is.na(value) & !is.finite(value)))) stop("Timestamps must be finite or NA")
  out <- .ca_type_info(value, "POSIXct", c("class", "tzone", "names"))
  zone <- attr(value, "tzone")
  if (!is.null(zone)) {
    if (!is.character(zone) || length(zone) == 0L || anyNA(zone) || !is.null(attributes(zone))) {
      stop("Timestamp timezones must be plain nonmissing character vectors")
    }
    out$tzone <- zone
  }
  out
}
.ca_type.difftime <- function(value) {
  if (!identical(class(value), "difftime")) stop("Unsupported duration class")
  out <- .ca_type_info(value, "difftime", c("class", "units", "names"))
  if (any(is.nan(value) | (!is.na(value) & !is.finite(value)))) stop("Durations must be finite or NA")
  out$units <- attr(value, "units")
  out
}
.ca_type.factor <- function(value) {
  expected <- if (is.ordered(value)) c("ordered", "factor") else "factor"
  if (!identical(class(value), expected)) stop("Unsupported factor class")
  if (anyNA(levels(value))) stop("Factor levels must not be missing")
  kind <- if (is.ordered(value)) "ordered" else "factor"
  out <- .ca_type_info(value, kind, c("class", "levels", "names"))
  if (length(levels(value)) > 0L) out$levels <- levels(value)
  out
}
#' @importFrom bit64 as.character.integer64
#' @noRd
.ca_type.integer64 <- function(value) {
  if (!identical(class(value), "integer64")) stop("Unsupported integer64 class")
  .ca_type_info(value, "integer64", c("class", "names"))
}
.ca_type.AsIs <- function(value) {
  remaining <- setdiff(class(value), "AsIs")
  class(value) <- if (length(remaining) > 0L) remaining else NULL
  out <- .ca_type(value)
  out$asis <- TRUE
  out
}
.ca_type.list <- function(value) {
  out <- .ca_type_info(value, "list")
  if (!is.null(names(value)) &&
      (anyDuplicated(chartr("A-Z", "a-z", names(value))) || any(!nzchar(names(value))))) {
    stop("List names must be nonempty and unique ignoring ASCII case")
  }
  if (length(value) > 0L) out$children <- unname(lapply(value, .ca_type))
  out
}
.ca_type.data.frame <- function(value) {
  if (!identical(class(value), "data.frame")) stop("Coerce data frame subclasses explicitly")
  out <- .ca_type_info(value, "data.frame", c("class", "row.names", "names"))
  if (anyDuplicated(chartr("A-Z", "a-z", names(value))) || any(!nzchar(names(value)))) {
    stop("Data frame column names must be nonempty and unique ignoring ASCII case")
  }
  if (length(value) > 0L) out$children <- unname(lapply(value, .ca_type))
  out$nrow <- nrow(value)
  if (.row_names_info(value, 1L) > 0L) out$rows <- attr(value, "row.names")
  out
}

.ca_literal <- function(value, type, con) {
  kind <- type$kind
  if (kind == "NULL") return("NULL")
  if (kind %in% c("list", "data.frame")) {
    if (length(value) == 0L) return(if (isTRUE(type$named)) "map()" else "[]::BOOLEAN[]")
    fields <- vapply(seq_along(value), function(i) {
      paste0("(", .ca_literal(value[[i]], type$children[[i]], con), ")::VARIANT")
    }, character(1L))
    if (isTRUE(type$named)) {
      fields <- paste0(DBI::dbQuoteString(con, names(value)), ": ", fields)
      return(paste0("{", paste(fields, collapse = ", "), "}"))
    }
    return(paste0("[", paste(fields, collapse = ", "), "]"))
  }
  if (kind == "raw") {
    return(paste0("from_hex('", paste(sprintf("%02x", as.integer(value)), collapse = ""), "')"))
  }
  sql_type <- .ca_types[[kind]]
  if (type$length == 0L) return(paste0("[]::", sql_type, "[]"))
  literal <- switch(kind,
    logical = ifelse(value, "TRUE", "FALSE"),
    integer = as.character(value),
    double = paste0(DBI::dbQuoteString(con, sprintf("%.17g", value)), "::DOUBLE"),
    character = as.character(DBI::dbQuoteString(con, value)),
    Date = paste0(DBI::dbQuoteString(con, as.character(value)), "::DATE"),
    POSIXct = paste0("to_timestamp(", DBI::dbQuoteString(con, sprintf("%.17g", as.double(value))), "::DOUBLE)"),
    difftime = paste0("to_microseconds(", sprintf("%.0f", round(as.double(value, units = "secs") * 1e6)), ")"),
    factor = as.character(DBI::dbQuoteString(con, as.character(value))),
    ordered = as.character(DBI::dbQuoteString(con, as.character(value))),
    integer64 = paste0(DBI::dbQuoteString(con, as.character(value)), "::BIGINT"))
  missing <- is.na(value)
  if (kind == "double") missing <- missing & !is.nan(value)
  literal[missing] <- paste0("NULL::", sql_type)
  if (type$length == 1L) return(paste0("(", literal, ")::", sql_type))
  paste0("[", paste(literal, collapse = ", "), "]::", sql_type, "[]")
}

.ca_payload <- function(value, con) {
  force(value)
  tryCatch({
    type <- .ca_type(value)
    list(value = DBI::SQL(paste0("(", .ca_literal(value, type, con), ")::VARIANT")),
      rtype = DBI::SQL(paste0("(", .ca_literal(type, .ca_type(type), con), ")::VARIANT")),
      type = type)
  }, error = function(e) {
    stop(errorCondition(conditionMessage(e), class = c("canard_value_error", "canard_input_error", "canard_error"),
      parent = e))
  })
}

# A stored value that cannot be decoded is a storage failure, like a failed read.
.ca_decoded <- function(value, id, operation) {
  tryCatch(value, error = function(e) {
    stop(errorCondition(paste("Unable to restore a stored value:", conditionMessage(e)),
      class = c("canard_restore_error", "canard_storage_error", "canard_error"),
      id = id, operation = operation, parent = e))
  })
}

# Run a lease-fenced statement whose projection returns one typed value.
.ca_read_value <- function(task, statement, type, expr, ...) {
  projection <- .ca_decoded(.ca_projection(expr, type, task@db@con), task@id, statement)
  rows <- .ca_owned(task, statement, ..., projection = DBI::SQL(projection))
  .ca_decoded(.ca_restore(rows$value, type), task@id, statement)
}

.ca_read_type <- function(value) {
  out <- lapply(value, function(column) {
    field <- if (is.list(column)) column[[1L]] else column
    if (is.atomic(field)) c(field) else field
  })
  children <- out$children
  if (is.data.frame(children)) {
    out$children <- lapply(seq_len(nrow(children)), function(i) .ca_read_type(children[i, , drop = FALSE]))
  } else if (length(children) > 0L) {
    out$children <- lapply(children, .ca_read_type)
  }
  out
}

.ca_projection <- function(expr, type, con) {
  kind <- type$kind
  if (kind == "NULL" || (kind %in% c("list", "data.frame") && type$length == 0L)) {
    return("NULL::BOOLEAN")
  }
  if (kind %in% c("list", "data.frame")) {
    fields <- vapply(seq_len(type$length), function(i) {
      key <- if (isTRUE(type$named)) DBI::dbQuoteString(con, type$names[[i]]) else paste0(i, "::UINTEGER")
      child <- paste0("variant_extract(", expr, ", ", key, ")")
      paste0("f", i, " := ", .ca_projection(child, type$children[[i]], con))
    }, character(1L))
    return(paste0("struct_pack(", paste(fields, collapse = ", "), ")"))
  }
  sql_type <- .ca_types[kind]
  if (is.na(sql_type)) stop("Unknown stored R value type: ", kind)
  vector <- type$length != 1L && kind != "raw"
  out <- paste0("CAST(", expr, " AS ", sql_type, if (vector) "[]", ")")
  if (kind == "difftime") {
    out <- if (vector) paste0("list_transform(", out, ", lambda x: epoch(x))") else paste0("epoch(", out, ")")
  }
  out
}

.ca_restore <- function(column, type) {
  kind <- type$kind
  if (kind == "NULL") return(NULL)
  if (kind %in% c("list", "data.frame")) {
    value <- lapply(seq_len(type$length), function(i) .ca_restore(column[[i]], type$children[[i]]))
    if (kind == "data.frame") {
      rows <- if (length(type$rows) > 0L) type$rows else .set_row_names(type$nrow)
      value <- structure(value, class = "data.frame", row.names = rows)
    }
  } else {
    value <- if (type$length != 1L || kind == "raw") column[[1L]] else column
    if (kind %in% c("factor", "ordered")) {
      value <- factor(value, levels = as.character(type$levels), ordered = kind == "ordered")
    } else if (kind == "POSIXct") {
      attr(value, "tzone") <- type$tzone
    } else if (kind == "difftime") {
      value <- as.difftime(value, units = "secs")
      units(value) <- type$units
      if (type$storage == "integer") value <- round(value)
    }
    storage.mode(value) <- type$storage
  }
  if (isTRUE(type$named)) names(value) <- as.character(type$names)
  if (isTRUE(type$asis)) value <- I(value)
  value
}
