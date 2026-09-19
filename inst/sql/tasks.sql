SELECT id, queue, name, state, priority, attempt, failures, max_failures,
    available_at, worker, lease_until, error, created_at, updated_at
FROM canard_absurd.tasks
WHERE (?queue IS NULL OR queue = ?queue)
    AND (?states IS NULL OR list_contains(?states::VARCHAR[], state))
    AND (?ids IS NULL OR list_contains(?ids::VARCHAR[], id))
ORDER BY updated_at DESC, id
LIMIT ?limit;
