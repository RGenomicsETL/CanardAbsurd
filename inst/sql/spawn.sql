INSERT INTO canard_absurd.tasks (id, queue, name, input, priority, max_failures)
VALUES (?id, ?queue, ?name, ?input, ?priority, ?max_failures)
ON CONFLICT (id) DO NOTHING
RETURNING 1 AS changed, id;
