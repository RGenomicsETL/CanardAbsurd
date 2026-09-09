# CanardAbsurd

CanardAbsurd ships a DuckDB workflow state machine and a thin R client. Quack owns remote SQL transport. R handlers execute in worker processes.

## Authorities

- `inst/sql/`: state transitions and schema. Every task transition is a single server-side SQL statement.
- `R/`: lifecycle, input admission, SQL interpolation, JSON conversion, and handler replay.
- `inst/tinytest/`: local and real Quack behavior, including competing R processes. Use `s7contract` for behavioral laws where appropriate.
- Roxygen comments generate `man/` and `NAMESPACE`.
- `README.Rmd` is evaluated into `README.md`; litedown renders that Markdown as the site landing page. Do not edit the generated README.
- `vignettes/*.Rmd` are evaluated offline package guides. `vignettes/articles/*.Rmd` are evaluated Quack-dependent site articles. All R chunks execute; no disabled demonstrations or invented results.
- `_pkgdown.yml` owns the API/reference site; `tools/build-site.R` composes pkgdown and the litedown landing page.

## Invariants

- Claim, checkpoint, heartbeat, completion, and cancellation contend on the same task row.
- Mutation results put numeric `changed` first: DuckDB's R driver sums the first result column to compute affected rows.
- Worker writes require the current lease token and an unexpired lease.
- SQL transaction conflicts can be retried; ambiguous transport failures are surfaced.
- No R handler runs inside a database transaction or a Quack server callback.
- Loading the package never installs extensions or starts services.
- Quack installation is explicit. Tests skip remote cases only when Quack is unavailable; `CANARDABSURD_REQUIRE_QUACK=true` makes absence a failure.

## Validation

Run `make document`, `make test`, and `make check`, followed by the Tree-sitter anti-slop audit. Documentation changes require `make docs`; install Quack explicitly with `make quack` when needed. The integration tests must exercise installed package artifacts and independent R processes. Keep build output under `artifacts/`. Preserve the separately developed examples in `~/ducknng`.
