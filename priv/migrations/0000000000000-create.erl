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
        generations := GensTable
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
                    "CREATE TYPE process_status AS ENUM ('init', 'running', 'error')"
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
            "process_id VARCHAR(256) PRIMARY KEY, "
            "status process_status NOT NULL, "
            "detail TEXT, "
            "aux_state BYTEA, "
            "current_generation INTEGER, "
            "corrupted_type task_type, "
            "previous_status process_status NOT NULL, "
            "status_changed_at TIMESTAMP WITH TIME ZONE NOT NULL, "
            "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
            "metadata JSONB)"
    ),
    %% create tasks table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ TaskTable ++
            " ("
            "task_id BIGSERIAL PRIMARY KEY, "
            "process_id VARCHAR(256) NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "running_time TIMESTAMP WITH TIME ZONE, "
            "finished_time TIMESTAMP WITH TIME ZONE, "
            "args BYTEA, "
            "generation INTEGER, "
            "metadata JSONB, "
            "idempotency_key VARCHAR(256) UNIQUE, "
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
            " ADD COLUMN IF NOT EXISTS corrupted_by BIGINT REFERENCES " ++ TaskTable ++ "(task_id) DEFERRABLE INITIALLY DEFERRED"
    ),

    %% create schedule table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ ScheduleTable ++
            " ("
            "task_id BIGINT PRIMARY KEY, "
            "process_id VARCHAR(256) NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "args BYTEA, "
            "generation INTEGER, "
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
            "process_id VARCHAR(256) PRIMARY KEY, "
            "task_id BIGINT NOT NULL, "
            "task_type task_type NOT NULL, "
            "status task_status NOT NULL, "
            "scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "running_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "args BYTEA, "
            "generation INTEGER, "
            "metadata JSONB, "
            "last_retry_interval INTEGER NOT NULL, "
            "attempts_count SMALLINT NOT NULL, "
            "context BYTEA, "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++
            " (process_id), "
            "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
    ),

    %% create state generations table
    {ok, _, _} = epg_pool:query(
        Connection,
        "CREATE TABLE IF NOT EXISTS " ++ GensTable ++
            " ("
            "process_id VARCHAR(256) NOT NULL, "
            "task_id BIGINT NOT NULL, "
            "generation INTEGER NOT NULL, "
            "timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(), "
            "metadata JSONB, "
            "payload BYTEA NOT NULL, "
            "PRIMARY KEY (process_id, generation), "
            "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++
            " (process_id), "
            "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
    ),
    lists:foreach(
        fun({Table, Suffix, Field}) ->
            Index = "\"" ++ lists:flatten(string:replace(Table, "\"", "", all)) ++ "_" ++ Suffix ++ "\"",
            {ok, _, _} = epg_pool:query(Connection,
                "CREATE INDEX " ++ Index ++ " ON " ++ Table ++ " USING HASH (" ++ Field ++ ")")
        end,
        [{GensTable, "process_idx", "process_id"}, {TaskTable, "process_idx", "process_id"},
         {ScheduleTable, "process_idx", "process_id"}, {RunningTable, "task_idx", "task_id"}]
    ),
    ok.
