UPDATE canard_absurd.tasks
SET lease_until = greatest(lease_until,
        to_timestamp(epoch_ms(current_timestamp) / 1000.0 + ?seconds)),
    updated_at = current_timestamp
WHERE id = ?id AND token = ?token::UUID AND state = 'running'
    AND lease_until > current_timestamp
RETURNING 1 AS changed, id,
    (map_extract_value(checkpoints, ?name)).kind AS checkpoint_kind,
    (map_extract_value(checkpoints, ?name)).json AS checkpoint_json;
