UPDATE canard_absurd.tasks
SET state = 'failed', failures = failures + 1,
    error = 'worker lease expired',
    worker = NULL, token = NULL, lease_until = NULL,
    updated_at = current_timestamp
WHERE id IN (
    SELECT id FROM canard_absurd.tasks
    WHERE queue = ?queue AND state = 'running'
        AND lease_until <= current_timestamp AND failures + 1 >= max_failures
    ORDER BY lease_until, id LIMIT 64
)
RETURNING 1 AS changed, id;
