UPDATE canard_absurd.tasks
SET state = CASE WHEN failures + 1 >= max_failures THEN 'failed' ELSE 'ready' END,
    failures = failures + 1, error = ?message,
    available_at = to_timestamp(epoch_ms(current_timestamp) / 1000.0 + ?delay_seconds),
    worker = NULL, token = NULL, lease_until = NULL,
    updated_at = current_timestamp
WHERE id = ?id AND token = ?token::UUID AND state = 'running'
    AND lease_until > current_timestamp
RETURNING 1 AS changed, id;
