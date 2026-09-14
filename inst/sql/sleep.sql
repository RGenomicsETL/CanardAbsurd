UPDATE canard_absurd.tasks
SET state = 'ready',
    checkpoints = map_concat(checkpoints,
        map([?name], [struct_pack(kind := 'sleep', json := NULL::VARCHAR)])),
    available_at = to_timestamp(epoch_ms(current_timestamp) / 1000.0 + ?seconds),
    worker = NULL, token = NULL, lease_until = NULL,
    updated_at = current_timestamp
WHERE id = ?id AND token = ?token::UUID AND state = 'running'
    AND lease_until > current_timestamp AND NOT map_contains(checkpoints, ?name)
RETURNING 1 AS changed, id;
