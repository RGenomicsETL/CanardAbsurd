.ca_text <- S7::new_property(S7::class_character, validator = function(value) {
  if (length(value) != 1L || is.na(value)) "must be one non-missing string"
})

.ca_name_problem <- function(value) {
  if (length(value) != 1L || is.na(value) || !nzchar(value)) "must be one nonempty string"
}

.ca_name <- S7::new_property(S7::class_character, validator = .ca_name_problem)

.ca_optional_name <- S7::new_property(S7::new_union(NULL, S7::class_character),
  validator = function(value) if (!is.null(value)) .ca_name_problem(value))

.ca_seconds <- S7::new_property(S7::class_numeric, validator = function(value) {
  if (length(value) != 1L || !is.finite(value) || value < 0) {
    "must be one finite nonnegative duration"
  }
})

.ca_positive_seconds <- S7::new_property(S7::class_numeric, validator = function(value) {
  if (length(value) != 1L || !is.finite(value) || value <= 0) {
    "must be one finite positive duration"
  }
})

.ca_integer <- S7::new_property(S7::class_numeric, validator = function(value) {
  if (length(value) != 1L || !is.finite(value) || value != trunc(value)) {
    "must be one finite integer-valued number"
  }
})

.ca_whole <- function(minimum) {
  S7::new_property(S7::class_numeric, validator = function(value) {
    if (length(value) != 1L || !is.finite(value) || value < minimum || value != trunc(value)) {
      paste("must be one integer-valued number of at least", minimum)
    }
  })
}
.ca_positive_integer <- .ca_whole(1)
.ca_count <- .ca_whole(0)

CanardDatabase <- S7::new_class("CanardDatabase", properties = list(
  path = .ca_name,
  allow_unsigned_extensions = S7::new_property(S7::class_logical, validator = function(value) {
    if (length(value) != 1L || is.na(value)) "must be TRUE or FALSE"
  })
))

CanardEndpoint <- S7::new_class("CanardEndpoint", properties = list(
  uri = .ca_name, token = .ca_name, extension = .ca_optional_name
))

CanardCoordinator <- S7::new_class("CanardCoordinator", properties = list(
  db = CanardConnection,
  extension = .ca_optional_name,
  poll_milliseconds = .ca_positive_integer,
  reap_limit = .ca_positive_integer
))

CanardSubmission <- S7::new_class("CanardSubmission", properties = list(
  db = CanardConnection, name = .ca_name, input = S7::class_any, queue = .ca_name,
  priority = .ca_integer, max_failures = .ca_integer,
  id = .ca_optional_name
))

CanardClaim <- S7::new_class("CanardClaim", properties = list(
  db = CanardConnection, queue = .ca_name, worker = .ca_name,
  lease_seconds = .ca_positive_seconds,
  reap_limit = .ca_positive_integer,
  task_names = S7::new_property(S7::new_union(NULL, S7::class_character),
    validator = function(value) {
      if (anyNA(value) || any(!nzchar(value))) "must contain nonempty handler names"
    })
))

CanardLookup <- S7::new_class("CanardLookup", properties = list(
  db = CanardConnection, id = .ca_text
))

CanardHeartbeat <- S7::new_class("CanardHeartbeat", properties = list(
  task = CanardTask, seconds = .ca_positive_seconds
))

CanardCompletion <- S7::new_class("CanardCompletion", properties = list(
  task = CanardTask, result = S7::class_any
))

CanardFailure <- S7::new_class("CanardFailure", properties = list(
  task = CanardTask, message = .ca_text, delay_seconds = .ca_seconds
))

CanardStep <- S7::new_class("CanardStep", properties = list(
  task = CanardTask, name = .ca_name, fn = S7::class_function
))

CanardSleep <- S7::new_class("CanardSleep", properties = list(
  task = CanardTask, name = .ca_name, seconds = .ca_seconds
))

CanardProcess <- S7::new_class("CanardProcess",
  properties = list(
    task = CanardTask, command = .ca_name, directory = .ca_name,
    heartbeat_seconds = .ca_positive_seconds,
    args = S7::new_property(S7::class_character, validator = function(value) {
      if (anyNA(value)) "must not contain missing values"
    }),
    timeout = S7::new_property(S7::class_numeric, validator = function(value) {
      if (length(value) != 1L || is.na(value) || value <= 0) "must be one positive duration or Inf"
    })
  ),
  validator = function(self) {
    if (self@heartbeat_seconds >= self@task@lease_seconds) {
      "heartbeat_seconds must be shorter than the task lease"
    }
  }
)

.ca_states <- c("ready", "running", "completed", "failed", "cancelled")

CanardListing <- S7::new_class("CanardListing", properties = list(
  db = CanardConnection, limit = .ca_positive_integer,
  queue = .ca_optional_name,
  state = S7::new_property(S7::new_union(NULL, S7::class_character), validator = function(value) {
    if (!all(value %in% .ca_states)) paste("must contain only", paste(.ca_states, collapse = ", "))
  }),
  id = S7::new_property(S7::new_union(NULL, S7::class_character), validator = function(value) {
    if (anyNA(value)) "must not contain missing task IDs"
  })
))

CanardRun <- S7::new_class("CanardRun", properties = list(
  task = CanardTask, handler = S7::class_function, failure_delay = .ca_seconds
))

CanardWorker <- S7::new_class("CanardWorker", properties = list(
  handlers = S7::new_property(S7::class_list, validator = function(value) {
    names <- names(value)
    if (!length(value) || length(names) != length(value) || anyNA(names) ||
        any(!nzchar(names)) || anyDuplicated(names) ||
        !all(vapply(value, is.function, logical(1L)))) {
      "must be a nonempty list of functions with unique, nonempty names"
    }
  }),
  max_tasks = S7::new_property(S7::class_numeric, validator = function(value) {
    if (length(value) != 1L || is.na(value) || value < 0 || value != floor(value)) {
      "must be a nonnegative integer-valued number or Inf"
    }
  }),
  poll_seconds = .ca_positive_seconds,
  idle_timeout = S7::new_property(S7::class_numeric, validator = function(value) {
    if (length(value) != 1L || is.na(value) || value < 0) {
      "must be one nonnegative duration or Inf"
    }
  }),
  failure_delay = .ca_seconds,
  conflict_retries = .ca_count,
  on_result = S7::new_union(NULL, S7::class_function)
))

.ca_input <- function(Class, ...) {
  tryCatch(Class(...), error = function(e) {
    stop(errorCondition(conditionMessage(e),
      class = c("canard_input_error", "canard_error"),
      input_class = Class, parent = e))
  })
}
