-module('0000000000000-create').

-export([perform/2]).

-spec perform(_, _) -> _.
perform(Connection, MigrationOpts) ->
    NsId = proplists:get_value(namespace, MigrationOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    {ok, _, [{IsProcessStatusExists}]} = epg_pool:query(
        Connection,
        "select exists (select 1 from pg_type where typname = 'process_status')"
    ),
    _ =
        case IsProcessStatusExists of
            true ->
                ok;
            false ->
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "CREATE TYPE process_status AS ENUM ('running', 'error')"
                )
        end,
    %% create type task_status if not exists
    {ok, _, [{IsTaskStatusExists}]} = epg_pool:query(
        Connection,
        "select exists (select 1 from pg_type where typname = 'task_status')"
    ),
    _ =
        case IsTaskStatusExists of
            true ->
                ok;
            false ->
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "CREATE TYPE task_status AS ENUM "
                    "('waiting', 'running', 'blocked', 'error', 'finished', 'cancelled')"
                )
        end,
    %% create type task_type if not exists
    {ok, _, [{IsTaskTypeExists}]} = epg_pool:query(
        Connection,
        "select exists (select 1 from pg_type where typname = 'task_type')"
    ),
    _ =
        case IsTaskTypeExists of
            true ->
                ok;
            false ->
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "CREATE TYPE task_type AS ENUM ('init', 'timeout', 'call', 'notify', 'repair', 'remove')"
                )
        end,
    %% create processes table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ ProcessesTable ++
            " ("
            "process_id VARCHAR(80) PRIMARY KEY, "
            "status process_status NOT NULL, "
            "detail TEXT, "
            "aux_state BYTEA, "
            "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
            "metadata JSONB)"
    ),
    %% create tasks table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ TaskTable ++
            " ("
            "task_id BIGSERIAL PRIMARY KEY, "
            "process_id VARCHAR(80) NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "running_time TIMESTAMP WITH TIME ZONE, "
            "finished_time TIMESTAMP WITH TIME ZONE, "
            "args BYTEA, "
            "metadata JSONB, "
            "idempotency_key VARCHAR(80) UNIQUE, "
            "response BYTEA, "
            "blocked_task BIGINT REFERENCES " ++ TaskTable ++
            " (task_id), "
            "last_retry_interval INTEGER NOT NULL, "
            "attempts_count SMALLINT NOT NULL, "
            "context BYTEA, "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++ " (process_id))"
    ),
    %% create constraint for process error cause
    {ok, _, _} = epg_pool:query(
        Connection,
        "ALTER TABLE " ++ ProcessesTable ++
            " ADD COLUMN IF NOT EXISTS corrupted_by BIGINT REFERENCES " ++ TaskTable ++ "(task_id)"
    ),

    %% create schedule table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ ScheduleTable ++
            " ("
            "task_id BIGINT PRIMARY KEY, "
            "process_id VARCHAR(80) NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "args BYTEA, "
            "metadata JSONB, "
            "last_retry_interval INTEGER NOT NULL, "
            "attempts_count SMALLINT NOT NULL, "
            "context BYTEA, "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++
            " (process_id), "
            "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
    ),

    %% create running table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ RunningTable ++
            " ("
            "process_id VARCHAR(80) PRIMARY KEY, "
            "task_id BIGINT NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "running_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "args BYTEA, "
            "metadata JSONB, "
            "last_retry_interval INTEGER NOT NULL, "
            "attempts_count SMALLINT NOT NULL, "
            "context BYTEA, "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++
            " (process_id), "
            "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
    ),

    %% create events table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ EventsTable ++
            " ("
            "process_id VARCHAR(80) NOT NULL, "
            "task_id BIGINT NOT NULL, "
            "event_id SMALLINT NOT NULL, "
            "timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(), "
            "metadata JSONB, "
            "payload BYTEA NOT NULL, "
            "PRIMARY KEY (process_id, event_id), "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++
            " (process_id), "
            "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
    ),
    ok.
