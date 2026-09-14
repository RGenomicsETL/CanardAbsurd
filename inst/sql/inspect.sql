SELECT task.id, task.queue, task.name, task.input, task.priority, task.state,
    task.attempt, task.failures, task.max_failures, task.available_at,
    task.worker, task.lease_until, task.result, task.error, task.created_at,
    task.updated_at, checkpoint.entry.key AS checkpoint_name,
    checkpoint.entry.value.kind AS checkpoint_kind,
    checkpoint.entry.value.json AS checkpoint_json,
    checkpoint.ordinal AS checkpoint_ordinal
FROM canard_absurd.tasks AS task
LEFT JOIN LATERAL UNNEST(map_entries(task.checkpoints)) WITH ORDINALITY
    AS checkpoint(entry, ordinal) ON TRUE
WHERE task.id = ?id
ORDER BY checkpoint.ordinal;
