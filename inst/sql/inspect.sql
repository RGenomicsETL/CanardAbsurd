SELECT id, queue, name, input::VARCHAR AS input, priority, state, attempt,
    failures, max_failures, available_at, worker, lease_until,
    checkpoints::VARCHAR AS checkpoints, result::VARCHAR AS result, error,
    created_at, updated_at
FROM canard_absurd.tasks WHERE id = ?id;
