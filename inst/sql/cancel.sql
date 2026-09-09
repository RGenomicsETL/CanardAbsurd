UPDATE canard_absurd.tasks
SET state = 'cancelled', worker = NULL, token = NULL, lease_until = NULL,
    updated_at = current_timestamp
WHERE id = ?id AND state IN ('ready', 'running')
RETURNING 1 AS changed, id;
