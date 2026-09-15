-- Fetch whole VARIANT values before field extraction: DuckDB issue #24064.
WITH task_values AS MATERIALIZED (
    SELECT input, result, checkpoints FROM canard_absurd.tasks WHERE id = ?id
)
SELECT ?projection FROM task_values;
