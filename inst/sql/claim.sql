UPDATE canard_absurd.tasks
SET state = 'running', attempt = attempt + 1,
    failures = failures + CASE WHEN state = 'running' THEN 1 ELSE 0 END,
    error = CASE WHEN state = 'running' THEN 'worker lease expired' ELSE error END,
    worker = ?worker, token = uuid(),
    lease_until = to_timestamp(epoch_ms(current_timestamp) / 1000.0 + ?lease_seconds),
    updated_at = current_timestamp
WHERE id = (
    SELECT id FROM canard_absurd.tasks
    WHERE queue = ?queue
        AND (?names IS NULL OR name IN (
            SELECT unnest(?names::VARCHAR[])
        ))
        AND (
            (state = 'ready' AND available_at <= current_timestamp)
            OR (state = 'running' AND lease_until <= current_timestamp
                AND failures + 1 < max_failures)
        )
    ORDER BY priority DESC, available_at, created_at, id
    LIMIT 1
)
RETURNING 1 AS changed, id, name, input, token::VARCHAR AS token, attempt;
