UPDATE canard_absurd.tasks
SET checkpoints = map_concat(checkpoints,
        map([?name], [struct_pack(kind := 'step', value := ?value, rtype := ?rtype)])),
    lease_until = greatest(lease_until,
        to_timestamp(epoch_ms(current_timestamp) / 1000.0 + ?seconds)),
    updated_at = current_timestamp
WHERE id = ?id AND token = ?token::UUID AND state = 'running'
    AND lease_until > current_timestamp AND NOT map_contains(checkpoints, ?name)
RETURNING 1 AS changed, id, ?projection AS value;
