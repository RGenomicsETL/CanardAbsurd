#' Durable R workflows with DuckDB and Quack
#'
#' Submit tasks that outlive the process requesting them, run them in R workers
#' under a lease, and replay named checkpoints after a crash. Host the database
#' with [ca_open()] or [ca_serve()], reach it with [ca_connect()], run tasks with
#' [ca_work()], and save stages with [ca_step()]. The checkpoint model follows
#' Absurd (<https://github.com/earendil-works/absurd>).
#'
#' @keywords internal
"_PACKAGE"
