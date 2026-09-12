CanardConnection <- S7::new_class(
  "CanardConnection", package = "CanardAbsurd",
  properties = list(
    con = S7::class_any,
    query = S7::class_function,
    uri = S7::new_property(S7::class_character, default = ""),
    server = S7::new_property(S7::class_logical, default = FALSE)
  )
)

CanardTask <- S7::new_class(
  "CanardTask", package = "CanardAbsurd",
  properties = list(
    db = CanardConnection,
    id = S7::class_character,
    name = S7::class_character,
    input = S7::class_any,
    token = S7::class_character,
    attempt = S7::class_integer,
    lease_seconds = S7::class_double,
    seen = S7::class_environment
  )
)
