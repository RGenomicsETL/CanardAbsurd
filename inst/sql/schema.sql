CREATE SCHEMA IF NOT EXISTS canard_absurd;

CREATE TABLE IF NOT EXISTS canard_absurd.schema_version (
    version INTEGER PRIMARY KEY CHECK (version = 1)
);
INSERT INTO canard_absurd.schema_version VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS canard_absurd.tasks (
    id VARCHAR PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 256),
    queue VARCHAR NOT NULL CHECK (length(queue) BETWEEN 1 AND 128),
    name VARCHAR NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    input JSON NOT NULL,
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
    checkpoints JSON NOT NULL DEFAULT '{}',
    result JSON,
    error VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (octet_length(encode(input::VARCHAR)) <= 1048576),
    CHECK (octet_length(encode(checkpoints::VARCHAR)) <= 16777216),
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
