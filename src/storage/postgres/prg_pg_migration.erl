-module(prg_pg_migration).

-include_lib("progressor/include/progressor.hrl").

-export([db_init/2]).
-export([cleanup/2]).

-spec db_init(prg_pg_backend:pg_opts(), namespace_id()) -> ok.
db_init(#{pool := Pool}, NsId) ->
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    {ok, _, _} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            %% create type process_status if not exists
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
            %% create indexes
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS process_idx on " ++ EventsTable ++ " USING HASH (process_id)"
            ),
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS process_idx on " ++ TaskTable ++ " USING HASH (process_id)"
            ),
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS process_idx on " ++ ScheduleTable ++ " USING HASH (process_id)"
            ),
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS task_idx on " ++ RunningTable ++ " USING HASH (task_id)"
            ),

            %% MIGRATIONS
            %% migrate process_id to varchar 256
            ok = lists:foreach(
                fun(T) ->
                    TableStr = string:replace(T, "\"", "'", all),
                    {ok, _, [{VarSize}]} = epg_pool:query(
                        Connection,
                        "SELECT character_maximum_length FROM information_schema.columns "
                        "WHERE table_name = " ++ TableStr ++ " AND column_name = 'process_id'"
                    ),
                    case VarSize < 256 of
                        true ->
                            {ok, _, _} = epg_pool:query(
                                Connection,
                                "ALTER TABLE " ++ T ++ "ALTER COLUMN process_id TYPE VARCHAR(256)"
                            );
                        false ->
                            skip
                    end
                end,
                [ProcessesTable, TaskTable, ScheduleTable, RunningTable, EventsTable]
            ),
            {ok, [], []}
        end
    ),
    ok.

-spec cleanup(_, _) -> _.
cleanup(#{pool := Pool}, NsId) ->
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _} = epg_pool:query(Connection, "ALTER TABLE " ++ ProcessesTable ++ " DROP COLUMN corrupted_by"),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ EventsTable),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ RunningTable),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ ScheduleTable),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ TaskTable),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ ProcessesTable),
            _ = epg_pool:query(Connection, "DROP TYPE task_type, task_status, process_status")
        end
    ),
    ok.
