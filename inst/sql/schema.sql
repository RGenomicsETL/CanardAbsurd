CREATE SCHEMA IF NOT EXISTS canard_absurd;

CREATE TABLE IF NOT EXISTS canard_absurd.schema_version (
    version INTEGER PRIMARY KEY CHECK (version = 1)
);
INSERT INTO canard_absurd.schema_version VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS canard_absurd.tasks (
    id VARCHAR PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 256),
    queue VARCHAR NOT NULL CHECK (length(queue) BETWEEN 1 AND 128),
    name VARCHAR NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    input VARIANT,
    input_rtype VARIANT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    state VARCHAR NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'running', 'completed', 'failed', 'cancelled')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    failures INTEGER NOT NULL DEFAULT 0 CHECK (failures >= 0),
    max_failures INTEGER NOT NULL CHECK (max_failures BETWEEN 1 AND 1000000),
    available_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    worker VARCHAR,
    token UUID,
    lease_until TIMESTAMPTZ,
    checkpoints MAP(VARCHAR, STRUCT(kind VARCHAR, value VARIANT, rtype VARIANT)) NOT NULL DEFAULT map(),
    result VARIANT,
    result_rtype VARIANT,
    error VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (
        (state = 'running' AND worker IS NOT NULL AND token IS NOT NULL
            AND lease_until IS NOT NULL)
        OR
        (state <> 'running' AND worker IS NULL AND token IS NULL
            AND lease_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS canard_absurd_claim
ON canard_absurd.tasks (queue, state, available_at);

CREATE VIEW IF NOT EXISTS canard_absurd.task_checkpoints AS
SELECT task.id AS task_id, checkpoint.entry.key AS name,
    checkpoint.entry.value.kind AS kind,
    checkpoint.entry.value.value AS value,
    checkpoint.entry.value.rtype AS rtype,
    checkpoint.ordinal AS ordinal
FROM canard_absurd.tasks AS task,
    UNNEST(map_entries(task.checkpoints)) WITH ORDINALITY
        AS checkpoint(entry, ordinal);
