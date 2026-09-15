library(CanardAbsurd)
if (!requireNamespace("s7contract", quietly = TRUE)) {
  exit_file("Generative laws require s7contract")
}
using(s7contract)

local({
  replay <- s7contract::new_law(
    "a saved step replays the same native value after a failure",
    generators = list(
      value = s7contract::gen_choice(
        s7contract::gen_integer(-1000L, 1000L),
        s7contract::gen_element(c("", "'\"\\/", "\u00e9", "null")),
        s7contract::gen_constant(list(x = NULL, nested = list(y = NULL)))
      ),
      name = s7contract::gen_element(c("step", "a.b", "a/'\"", "\u00e9"))
    ),
    holds = function(value, name) {
      db <- ca_open()
      on.exit(ca_close(db))
      ca_spawn(db, "work")
      first <- ca_claim(db)
      before <- ca_step(first, name, function() value)
      ca_fail(first, "replay")
      second <- ca_claim(db)
      after <- ca_step(second, name, function() stop("checkpoint was not replayed"))
      ca_complete(second, after)
      identical(before, after)
    }
  )
  expect_law(replay, tests = 30L, seed = 20260909L)
})

local({
  fencing <- s7contract::new_law(
    "expired attempts cannot alter a replacement lease",
    generators = list(reclaims = s7contract::gen_integer(1L, 5L)),
    holds = function(reclaims) {
      db <- ca_open()
      on.exit(ca_close(db))
      ca_spawn(db, "work", id = "law", max_failures = 10L)
      stale <- list()
      current <- ca_claim(db)
      for (i in seq_len(reclaims)) {
        stale[[i]] <- current
        DBI::dbExecute(db@con, "UPDATE canard_absurd.tasks
          SET lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 - 1) WHERE id = 'law'")
        current <- ca_claim(db)
      }
      rejected <- vapply(stale, function(task) {
        tryCatch({ ca_complete(task, "stale"); FALSE },
          canard_lease_lost = function(e) TRUE)
      }, logical(1L))
      ca_complete(current, "current")
      record <- ca_inspect(db, "law")
      all(rejected) && identical(record$result, "current") &&
        record$attempt == reclaims + 1L
    }
  )
  expect_law(fencing, tests = 15L, seed = 20260910L)
})
