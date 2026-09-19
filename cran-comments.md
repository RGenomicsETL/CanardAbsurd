## Test environment

- Ubuntu 24.04.3 LTS, R 4.6.0 (2026-04-24), DuckDB R 1.5.5

## R CMD check results

0 errors | 0 warnings | 1 note

- This is a new submission.

## External software

Remote database hosting and clients require the DuckDB Quack extension. The
package does not install extensions when loaded or connected. Examples,
vignettes, and CRAN tests exercise the single-process API without Quack;
integration tests run conditionally when a matching Quack extension is present.
