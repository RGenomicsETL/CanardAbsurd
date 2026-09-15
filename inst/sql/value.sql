-- Fetch whole VARIANT values before field extraction: DuckDB issue #24064.
WITH task_values AS MATERIALIZED (
    SELECT input, checkpoints FROM canard_absurd.tasks
    WHERE id = ?id AND token = ?token::UUID AND state = 'running'
        AND lease_until > current_timestamp
)
SELECT ?projection AS value FROM task_values;
