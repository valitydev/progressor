CREATE TYPE process_status AS ENUM ('running', 'error');
CREATE TYPE task_status AS ENUM ('waiting', 'running', 'blocked', 'error', 'finished', 'cancelled');
CREATE TYPE task_type AS ENUM ('init', 'timeout', 'call', 'notify', 'repair');

CREATE TABLE IF NOT EXISTS default_processes(
    "process_id" VARCHAR(80) PRIMARY KEY,
    "status" process_status NOT NULL,
    "detail" TEXT,
    "aux_state" BYTEA,
    "metadata" JSONB
);

CREATE TABLE IF NOT EXISTS default_tasks(
    "task_id" BIGSERIAL PRIMARY KEY,
    "process_id" VARCHAR(80) NOT NULL,
    "task_type" task_type NOT NULL,
    "status" task_status NOT NULL,
    "scheduled_time" TIMESTAMP WITH TIME ZONE NOT NULL,
    "running_time" TIMESTAMP WITH TIME ZONE,
    "finished_time" TIMESTAMP WITH TIME ZONE,
    "args" BYTEA,
    "metadata" JSONB,
    "idempotency_key" VARCHAR(80) UNIQUE,
    "response" BYTEA,
    "blocked_task" BIGINT REFERENCES default_tasks ("task_id"),
    "last_retry_interval" INTEGER NOT NULL,
    "attempts_count" SMALLINT NOT NULL,
    "context" BYTEA,
    FOREIGN KEY ("process_id") REFERENCES default_processes ("process_id")
);

CREATE TABLE IF NOT EXISTS default_events(
    "process_id" VARCHAR(80) NOT NULL,
    "task_id" BIGINT NOT NULL,
    "event_id" SMALLINT NOT NULL,
    "timestamp" TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    "metadata" JSONB,
    "payload" BYTEA NOT NULL,
    PRIMARY KEY ("process_id", "event_id"),
    FOREIGN KEY ("process_id") REFERENCES default_processes ("process_id"),
    FOREIGN KEY ("task_id") REFERENCES default_tasks ("task_id")
);

CREATE INDEX IF NOT EXISTS "process_idx" on default_events USING HASH ("process_id");
