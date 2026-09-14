UPDATE canard_absurd.tasks
SET state = 'completed', result = ?result, error = NULL,
    worker = NULL, token = NULL, lease_until = NULL,
    updated_at = current_timestamp
WHERE id = ?id AND token = ?token::UUID AND state = 'running'
    AND lease_until > current_timestamp
RETURNING 1 AS changed, id;
