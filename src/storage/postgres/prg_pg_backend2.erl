-module(prg_pg_backend2).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("progressor/include/progressor.hrl").

%% API

%% api handler functions
-export([get_task_result/3]).
-export([get_process_status/3]).
-export([prepare_init/4]).
-export([prepare_call/4]).
-export([prepare_repair/4]).

%% scan functions
-export([collect_zombies/3]).
-export([search_timers/4]).
-export([search_calls/3]).

%% worker functions
-export([complete_and_continue/6]).
-export([complete_and_suspend/5]).
-export([complete_and_unlock/5]).
-export([complete_and_error/4]).
-export([remove_process/3]).

%% shared functions
-export([get_task/4]).
-export([get_process/5]).

%% Init operations
-export([db_init/2]).

-export([cleanup/2]).

-type pg_opts() :: #{pool := atom()}.

-define(PROTECT_TIMEOUT, 5). %% second

-spec get_task_result(pg_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, term()} | {error, _Reason}.
get_task_result(PgOpts, NsId, KeyOrId) ->
    Pool = get_pool(external, PgOpts),
    TaskTable = construct_table_name(NsId, "_tasks"),
    case do_get_task_result(Pool, TaskTable, KeyOrId) of
        {ok, _, []} ->
            {error, not_found};
        {ok, _, [{null}]} ->
            {error, in_progress};
        {ok, _, [{Value}]} ->
            {ok, binary_to_term(Value)}
    end.

-spec get_task(recipient(), pg_opts(), namespace_id(), task_id()) -> {ok, task()} | {error, _Reason}.
get_task(Recipient, PgOpts, NsId, TaskId) ->
    Pool = get_pool(Recipient, PgOpts),
    TaskTable = construct_table_name(NsId, "_tasks"),
    case do_get_task(Pool, TaskTable, TaskId) of
        {ok, _, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            [Task] = to_maps(Columns, Rows, fun marshal_task/1),
            {ok, Task}
    end.

-spec get_process_status(pg_opts(), namespace_id(), id()) -> term().
get_process_status(PgOpts, NsId, Id) ->
    Pool = get_pool(external, PgOpts),
    Table = construct_table_name(NsId, "_processes"),
    {ok, _Columns, Rows} = epg_pool:query(
        Pool,
        "SELECT status from " ++ Table ++ " WHERE process_id = $1",
        [Id]
    ),
    case Rows of
        [] -> {error, <<"process not found">>};
        [{Status}] -> {ok, Status}
    end.

-spec get_process(recipient(), pg_opts(), namespace_id(), id(), history_range()) -> {ok, process()}.
get_process(Recipient, PgOpts, NsId, ProcessId, HistoryRange) ->
    Pool = get_pool(Recipient, PgOpts),
    EventsTable = construct_table_name(NsId, "_events"),
    ProcessesTable = construct_table_name(NsId, "_processes"),
    RangeCondition = create_range_condition(HistoryRange),
    %% TODO optimize request
    {ok, Columns, Rows} = epg_pool:query(
        Pool,
        "SELECT pr.process_id, pr.status, pr.detail, pr.aux_state, pr.metadata as p_meta, pr.corrupted_by,"
        "    ev.event_id, ev.timestamp, ev.metadata, ev.payload "
        "FROM "
        "  (SELECT * FROM " ++ ProcessesTable ++ " WHERE process_id = $1) AS pr "
        "   LEFT JOIN (SELECT * FROM " ++ EventsTable ++ RangeCondition ++ " ORDER BY event_id ASC) AS ev "
        "   ON ev.process_id = pr.process_id ",
        [ProcessId]
    ),
    case Rows of
        [] ->
            {error, <<"process not found">>};
        [Head | _] ->
            [Proc] = to_maps(Columns, [Head], fun marshal_process/1),
            Events = lists:filter(
                fun(Rec) -> is_map_key(event_id, Rec) end,
                to_maps(Columns, Rows, fun marshal_event/1)
            ),
            {ok, Proc#{history => Events}}
    end.

-spec remove_process(pg_opts(), namespace_id(), id()) -> ok | no_return().
remove_process(PgOpts, NsId, ProcessId) ->
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _R} = epg_pool:query(Connection, "DELETE FROM " ++ RunningTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _S} = epg_pool:query(Connection, "DELETE FROM " ++ ScheduleTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _E} = epg_pool:query(Connection, "DELETE FROM " ++ EventsTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _T} = epg_pool:query(Connection, "DELETE FROM " ++ TaskTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _P} = epg_pool:query(Connection, "DELETE FROM " ++ ProcessesTable ++ " WHERE process_id = $1", [ProcessId])
        end
    ),
    ok.

-spec collect_zombies(pg_opts(), namespace_id(), timeout_sec()) -> ok.
collect_zombies(PgOpts, NsId, Timeout) ->
    Pool = get_pool(scan, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        running := RunningTable
    } = tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    TsBackward = unixtime_to_datetime(NowSec - (Timeout + ?PROTECT_TIMEOUT)),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _} = epg_pool:query(
                Connection,
                "WITH zombie_tasks as ("
                "  DELETE FROM " ++ RunningTable ++ " WHERE running_time < $1 "
                "    RETURNING process_id, task_id"
                "  ), "
                "  t1 AS (UPDATE " ++ TaskTable ++ " SET status = 'cancelled' WHERE process_id IN (SELECT process_id FROM zombie_tasks))"
                "  t2 AS (UPDATE " ++ TaskTable ++ " SET status = 'error', finished_time = $2 WHERE task_id IN (SELECT task_id FROM zombie_tasks))"
                "MERGE INTO " ++ ProcessesTable ++ " AS pt USING zombie_tasks AS zt ON pt.process_id = zt.process_id "
                "  WHEN MATCHED THEN UPDATE SET status = 'error', detail = 'zombie detected', corrupted_by = zt.task_id",
                [TsBackward, Now]
            )
        end
    ),
    ok.

-spec search_timers(pg_opts(), namespace_id(), timeout_sec(), pos_integer()) -> [task()].
search_timers(PgOpts, NsId, _Timeout, Limit) ->
    Pool = get_pool(scan, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    NowText = unixtime_to_text(NowSec),
    {ok, _, Columns, Rows} = _Res = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _, _} = epg_pool:query(
                Connection,
                "WITH tasks_for_run as("
                "  DELETE FROM " ++ ScheduleTable ++ " WHERE task_id IN "
                "    (SELECT task_id FROM " ++ ScheduleTable ++ " WHERE status = 'waiting' AND scheduled_time <= $1 "
                "      AND task_type IN ('timeout', 'remove') AND process_id NOT IN (SELECT process_id FROM " ++ RunningTable ++ " )"
                "      ORDER BY scheduled_time ASC LIMIT $3)"
                "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                "      last_retry_interval, attempts_count, context"
                "  ) "
                "INSERT INTO " ++ RunningTable ++
                "  (task_id, process_id, task_type, status, scheduled_time, running_time, args, metadata, last_retry_interval, attempts_count, context) "
                "  SELECT * FROM tasks_for_run RETURNING *",
                [Now, NowText, Limit]
            )
        end
    ),
    to_maps(Columns, Rows, fun marshal_task/1).

-spec search_calls(pg_opts(), namespace_id(), pos_integer()) -> [task()].
search_calls(PgOpts, NsId, Limit) ->
    Pool = get_pool(scan, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_text(NowSec),
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _, _} = epg_pool:query(
                Connection,
                "WITH tasks_for_run as("
                "  DELETE FROM " ++ ScheduleTable ++ " WHERE task_id IN "
                "    (SELECT min(task_id) FROM " ++ ScheduleTable ++ " WHERE status = 'waiting' AND task_type IN ('init', 'call', 'repair')"
                "      AND process_id NOT IN (SELECT process_id FROM " ++ RunningTable ++ " )"
                "      GROUP BY process_id ORDER BY min ASC LIMIT $2"
                "    ) "
                "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                "      TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                "      last_retry_interval, attempts_count, context"
                "  ) "
                "INSERT INTO " ++ RunningTable ++
                "  (task_id, process_id, task_type, status, scheduled_time, running_time, args, metadata, last_retry_interval, attempts_count, context) "
                "  SELECT * FROM tasks_for_run RETURNING *",
                [Now, Limit]
            )
        end
    ),
    to_maps(Columns, Rows, fun marshal_task/1).

-spec prepare_init(pg_opts(), namespace_id(), process(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_init(PgOpts, NsId, Process, Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            case do_save_process(Connection, ProcessesTable, Process) of
                {ok, _} ->
                    {ok, _, _, [{TaskId}]} = do_save_task(Connection, TaskTable, Task),
                    case Task of
                        #{status := <<"running">>} ->
                            {ok, _, _, _} = do_save_running(Connection, RunningTable, Task#{task_id => TaskId}),
                            {ok, {continue, TaskId}};
                        #{status := <<"waiting">>} ->
                            {ok, _, _, _} = do_save_schedule(Connection, ScheduleTable, Task#{task_id => TaskId}),
                            {ok, {postpone, TaskId}}
                    end;
                {error, #error{codename = unique_violation}} ->
                    {error, <<"process already exists">>}
            end
        end
    ).

-spec prepare_call(pg_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Error}.
prepare_call(PgOpts, NsId, ProcessId, #{status := <<"waiting">>} = Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        schedule := ScheduleTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(C) ->
            {ok, _, _, [{TaskId}]} = do_save_task(C, TaskTable, Task),
            {ok, _BlockedTaskId} = do_block_timer(C, ScheduleTable, ProcessId),
            {ok, _, _, _} = do_save_schedule(C, ScheduleTable, Task#{task_id => TaskId}),
            {ok, {postpone, TaskId}}
        end
    );
prepare_call(PgOpts, NsId, ProcessId, #{status := <<"running">>} = Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(C) ->
            {ok, _, _, [{TaskId}]} = do_save_task(C, TaskTable, Task),
            {ok, _BlockedTaskId} = do_block_timer(C, ScheduleTable, ProcessId),
            case do_save_running(C, RunningTable, Task#{task_id => TaskId}) of
                {ok, _, _, []} ->
                    {ok, _, _, _} = do_save_schedule(C, ScheduleTable, Task#{task_id => TaskId, status => <<"waiting">>}),
                    {ok, {postpone, TaskId}};
                {ok, _, _, [{TaskId}]} ->
                    {ok, {continue, TaskId}}
            end
        end
    ).

-spec prepare_repair(pg_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_repair(PgOpts, NsId, _ProcessId, #{status := <<"waiting">>} = Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        schedule := ScheduleTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(C) ->
            {ok, _, _, [{TaskId}]} = do_save_task(C, TaskTable, Task),
            {ok, _, _, _} = do_save_schedule(C, ScheduleTable, Task#{task_id => TaskId}),
            {ok, {postpone, TaskId}}
        end
    );
prepare_repair(PgOpts, NsId, _ProcessId, #{status := <<"running">>} = Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        running := RunningTable
    } = tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(C) ->
            {ok, _, _, [{TaskId}]} = do_save_task(C, TaskTable, Task),
            case do_save_running(C, RunningTable, Task#{task_id => TaskId}) of
                {ok, _, _, []} ->
                    {error, <<"process is running">>};
                {ok, _, _, [{TaskId}]} ->
                    {ok, {continue, TaskId}}
            end
        end
    ).

-spec complete_and_continue(pg_opts(), namespace_id(), task_result(), process(), [event()], task()) ->
    {ok, [task()]}.
complete_and_continue(PgOpts, NsId, TaskResult, Process, Events, NextTask) ->
    % update completed task and process,
    % cancel blocked and waiting timers,
    % save new timer,
    % return continuation call if exists
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            %% TODO implement via batch execute
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _, [{NextTaskId}]} = do_save_task(Connection, TaskTable, NextTask),
            case do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult#{process_id => ProcessId}) of
                {ok, _, _, []} ->
                    %% continuation call not exist
                    case NextTask of
                        #{status := <<"running">>} ->
                            do_save_running(
                                Connection,
                                RunningTable,
                                NextTask#{task_id => NextTaskId, running_time => erlang:system_time(second)},
                                " * "
                            );
                        #{status := <<"waiting">>} ->
                            {ok, _, _, [{NextTaskId}]} = do_save_schedule(
                                Connection,
                                ScheduleTable,
                                NextTask#{task_id => NextTaskId}
                            ),
                            {ok, 0, [], []}
                    end;
                {ok, _, Col, Row} ->
                    %% continuation call exists, return it
                    {ok, _, _, [{NextTaskId}]} = do_save_schedule(
                        Connection,
                        ScheduleTable,
                        NextTask#{task_id => NextTaskId, status => <<"blocked">>}
                    ),
                    {ok, 1, Col, Row}
            end
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec complete_and_suspend(pg_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_suspend(PgOpts, NsId, TaskResult, Process, Events) ->
    % update completed task and process, cancel blocked and waiting timers
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult#{process_id => ProcessId})
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec complete_and_error(pg_opts(), namespace_id(), task_result(), process()) -> ok.
complete_and_error(PgOpts, NsId, TaskResult, Process) ->
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = tables(NsId),
    #{process_id := ProcessId} = Process,
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, 1} = do_update_process(Connection, ProcessesTable, Process),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _} =  do_cancel_calls(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _, _} = do_complete_task(
                Connection,
                TaskTable,
                ScheduleTable,
                RunningTable,
                TaskResult#{process_id => ProcessId}
            )
        end
    ),
    ok.

-spec complete_and_unlock(pg_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_unlock(PgOpts, NsId, TaskResult, Process, Events) ->
    % update completed task and process, unlock blocked task
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            Completion = do_complete_task(
                Connection,
                TaskTable,
                ScheduleTable,
                RunningTable,
                TaskResult#{process_id => ProcessId}
            ),
            case Completion of
                {ok, _, _, []} ->
                    {ok, _} = do_unlock_timer(Connection, ScheduleTable, ProcessId);
                {ok, _, _Col, _Row} ->
                    %% if postponed call exists then timer remain blocked
                    do_nothing
            end,
            Completion
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec db_init(pg_opts(), namespace_id()) -> ok.
db_init(#{pool := Pool}, NsId) ->
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
    {ok, _, _} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            %% create type process_status if not exists
            {ok, _, [{IsProcessStatusExists}]} = epg_pool:query(
                Connection,
                "select exists (select 1 from pg_type where typname = 'process_status')"
            ),
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
                "CREATE TABLE IF NOT EXISTS " ++ ProcessesTable ++ " ("
                "process_id VARCHAR(80) PRIMARY KEY, "
                "status process_status NOT NULL, "
                "detail TEXT, "
                "aux_state BYTEA, "
                "metadata JSONB)"
            ),
            %% create tasks table
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE TABLE IF NOT EXISTS " ++ TaskTable ++ " ("
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
                "blocked_task BIGINT REFERENCES " ++ TaskTable ++ " (task_id), "
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
                "CREATE TABLE IF NOT EXISTS " ++ ScheduleTable ++ " ("
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
                "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++ " (process_id), "
                "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
            ),

            %% create running table
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE TABLE IF NOT EXISTS " ++ RunningTable ++ " ("
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
                "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++ " (process_id), "
                "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
            ),

            %% create events table
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE TABLE IF NOT EXISTS " ++ EventsTable ++ " ("
                "process_id VARCHAR(80) NOT NULL, "
                "task_id BIGINT NOT NULL, "
                "event_id SMALLINT NOT NULL, "
                "timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(), "
                "metadata JSONB, "
                "payload BYTEA NOT NULL, "
                "PRIMARY KEY (process_id, event_id), "
                "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++ " (process_id), "
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
            )

        end),
    ok.

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(#{pool := Pool}, NsId) ->
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = tables(NsId),
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

%-endif.

%% Internal functions

construct_table_name(NsId, Postfix) ->
    "\"" ++ erlang:atom_to_list(NsId) ++ Postfix ++ "\"".

tables(NsId) ->
    #{
        processes => construct_table_name(NsId, "_processes"),
        tasks => construct_table_name(NsId, "_tasks"),
        schedule => construct_table_name(NsId, "_schedule"),
        running => construct_table_name(NsId, "_running"),
        events => construct_table_name(NsId, "_events")
    }.

create_range_condition(#{offset := Offset, limit := Limit}) ->
    " WHERE event_id > " ++ integer_to_list(Offset) ++ " AND event_id <= " ++ integer_to_list(Offset + Limit) ++ " ";
create_range_condition(#{offset := Offset}) ->
    " WHERE event_id > " ++ integer_to_list(Offset) ++ " ";
create_range_condition(#{limit := Limit}) ->
    " WHERE event_id <= " ++ integer_to_list(Limit) ++ " ";
create_range_condition(_) ->
    " ".

do_save_process(Connection, Table, Process) ->
    #{
        process_id := ProcessId,
        status := Status
    } = Process,
    Detail = maps:get(detail, Process, null),
    AuxState = maps:get(aux_state, Process, null),
    Meta = maps:get(metadata, Process, null),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++ " (process_id, status, detail, aux_state, metadata) VALUES ($1, $2, $3, $4, $5)",
        [ProcessId, Status, Detail, AuxState, json_encode(Meta)]
    ).

do_save_task(Connection, Table, Task) ->
    do_save_task(Connection, Table, Task, " task_id ").

do_save_task(Connection, Table, Task, Returning) ->
    #{
        process_id := ProcessId,
        task_type := TaskType,
        status := Status,
        scheduled_time := ScheduledTs,
        last_retry_interval := LastRetryInterval,
        attempts_count := AttemptsCount
    } = Task,
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    IdempotencyKey = maps:get(idempotency_key, Task, null),
    BlockedTask = maps:get(blocked_task, Task, null),
    RunningTs = maps:get(running_time, Task, null),
    Response = maps:get(response, Task, null),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++ " "
        "  (process_id, task_type, status, scheduled_time, running_time, args, "
        "   metadata, idempotency_key, blocked_task, response, last_retry_interval, attempts_count, context)"
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING " ++ Returning,
        [
            ProcessId, TaskType, Status, unixtime_to_datetime(ScheduledTs), unixtime_to_datetime(RunningTs), Args,
            json_encode(MetaData), IdempotencyKey, BlockedTask, Response, LastRetryInterval, AttemptsCount, Context
        ]
    ).
%%

do_save_running(Connection, Table, Task) ->
    do_save_running(Connection, Table, Task, " task_id ").

do_save_running(Connection, Table, Task, Returning) ->
    #{
        task_id := TaskId,
        process_id := ProcessId,
        task_type := TaskType,
        status := Status,
        scheduled_time := ScheduledTs,
        last_retry_interval := LastRetryInterval,
        attempts_count := AttemptsCount
    } = Task,
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    RunningTs = erlang:system_time(second),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++ " "
        "  (task_id, process_id, task_type, status, scheduled_time, running_time, "
        "    args, metadata, last_retry_interval, attempts_count, context)"
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "
        "  ON CONFLICT (process_id) DO NOTHING RETURNING " ++ Returning,
        [
            TaskId, ProcessId, TaskType, Status, unixtime_to_datetime(ScheduledTs), unixtime_to_datetime(RunningTs),
            Args, json_encode(MetaData), LastRetryInterval, AttemptsCount, Context
        ]
    ).

do_save_schedule(Connection, Table, Task) ->
    do_save_schedule(Connection, Table, Task, "task_id").

do_save_schedule(Connection, Table, Task, Returning) ->
    #{
        task_id := TaskId,
        process_id := ProcessId,
        task_type := TaskType,
        status := Status,
        scheduled_time := ScheduledTs,
        last_retry_interval := LastRetryInterval,
        attempts_count := AttemptsCount
    } = Task,
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++ " "
        "  (task_id, process_id, task_type, status, scheduled_time, args, metadata, last_retry_interval, attempts_count, context)"
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING " ++ Returning,
        [
            TaskId, ProcessId, TaskType, Status, unixtime_to_datetime(ScheduledTs), Args,
            json_encode(MetaData), LastRetryInterval, AttemptsCount, Context
        ]
    ).

do_get_task_result(Connection, Table, {idempotency_key, IdempotencyKey}) ->
    epg_pool:query(
        Connection,
        "SELECT response FROM " ++ Table ++ " WHERE idempotency_key = $1",
        [IdempotencyKey]
    );
do_get_task_result(Connection, Table, {task_id, TaskId}) ->
    epg_pool:query(
        Connection,
        "SELECT response FROM " ++ Table ++ " WHERE task_id = $1",
        [TaskId]
    ).

do_get_task(Connection, Table, TaskId) ->
    epg_pool:query(
        Connection,
        "SELECT * FROM " ++ Table ++ " WHERE task_id = $1",
        [TaskId]
    ).

do_update_process(Connection, ProcessesTable, Process) ->
    #{
        process_id := ProcessId,
        status := Status
    } = Process,
    Detail = maps:get(detail, Process, null),
    AuxState = maps:get(aux_state, Process, null),
    MetaData = maps:get(metadata, Process, null),
    CorruptedBy = maps:get(corrupted_by, Process, null),
    epg_pool:query(
        Connection,
        "UPDATE " ++ ProcessesTable ++ " SET status = $1, detail = $2, aux_state = $3, metadata = $4, corrupted_by = $5 "
        "WHERE process_id = $6",
        [Status, Detail, AuxState, json_encode(MetaData), CorruptedBy, ProcessId]
    ).

do_save_event(Connection, EventsTable, ProcessId, TaskId, Event) ->
    #{
        event_id := EventId,
        timestamp := EventTs,
        payload := Payload
    } = Event,
    MetaData = maps:get(metadata, Event, null),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ EventsTable ++ " (process_id, task_id, event_id, timestamp, payload, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        [ProcessId, TaskId, EventId, unixtime_to_datetime(EventTs), Payload, json_encode(MetaData)]
    ).

do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult) ->
    #{
        task_id := TaskId,
        process_id := ProcessId,
        status := Status
    } = TaskResult,
    Response = maps:get(response, TaskResult, null),
    FinishedTime = maps:get(finished_time, TaskResult, erlang:system_time(second)),
    {ok, _} = epg_pool:query(
        Connection,
        "WITH deleted AS("
        "  DELETE FROM " ++ RunningTable ++ " WHERE process_id = $4"
        "  )"
        "UPDATE " ++ TaskTable ++ " SET status = $1, response = $2, finished_time = $3 WHERE task_id = $5",
        [Status, Response, unixtime_to_datetime(FinishedTime), ProcessId, TaskId]
    ),
    case Status of
        <<"error">> ->
            %% do nothing
            {ok, 0, [], []};
        _ ->
            %% search waiting call
            RunningTime = unixtime_to_text(erlang:system_time(second)),
            epg_pool:query(
                Connection,
                "WITH postponed_tasks AS ("
                "  DELETE FROM " ++ ScheduleTable ++ " WHERE task_id = "
                "    (SELECT min(task_id) FROM " ++ ScheduleTable ++
                "      WHERE process_id = $1 AND status = 'waiting' AND task_type IN ('call', 'repair')) "
                "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                "      last_retry_interval, attempts_count, context"
                "  ) "
                "INSERT INTO " ++ RunningTable ++ " (task_id, process_id, task_type, status, scheduled_time, running_time, args, "
                "  metadata, last_retry_interval, attempts_count, context) SELECT * FROM postponed_tasks RETURNING *",
                [ProcessId, RunningTime]
            )
    end.

do_block_timer(Connection, ScheduleTable, ProcessId) ->
    {ok, _, _Columns, Rows} = epg_pool:query(
        Connection,
        "UPDATE " ++ ScheduleTable ++ " SET status = 'blocked' WHERE task_type IN ('timeout', 'remove') AND "
        "process_id = $1 AND status = 'waiting' RETURNING task_id",
        [ProcessId]
    ),
    case Rows of
        [] -> {ok, null};
        [{TaskId}] -> {ok, TaskId}
    end.

do_unlock_timer(Connection, ScheduleTable, ProcessId) ->
    epg_pool:query(
        Connection,
        "UPDATE " ++ ScheduleTable ++ " SET status = 'waiting' "
        "WHERE process_id = $1 AND status = 'blocked'",
        [ProcessId]
    ).

do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId) ->
    epg_pool:query(
        Connection,
        "WITH deleted_tasks as("
        "  DELETE FROM " ++ ScheduleTable ++ " WHERE process_id = $1 AND task_type IN ('timeout', 'remove') "
        "    AND (status = 'waiting' OR status = 'blocked') RETURNING task_id"
        "  ) "
        "MERGE INTO " ++ TaskTable ++ " as tt USING deleted_tasks as dt ON tt.task_id = dt.task_id "
        "  WHEN MATCHED THEN UPDATE SET status = 'cancelled'",
        [ProcessId]
    ).

do_cancel_calls(Connection, TaskTable, ScheduleTable, ProcessId) ->
    epg_pool:query(
        Connection,
        "WITH deleted_tasks as("
        "  DELETE FROM " ++ ScheduleTable ++ " WHERE process_id = $1 AND task_type NOT IN ('timeout', 'remove') AND status = 'waiting' "
        "    RETURNING task_id"
        "  ) "
        "MERGE INTO " ++ TaskTable ++ " as tt USING deleted_tasks as dt ON tt.task_id = dt.task_id "
        "  WHEN MATCHED THEN UPDATE SET status = 'cancelled'",
        [ProcessId]
    ).

to_maps(Columns, Rows, TransformRowFun) ->
    ColNumbers = erlang:length(Columns),
    Seq = lists:seq(1, ColNumbers),
    lists:map(
        fun(Row) ->
            Data = lists:foldl(
                fun(Pos, Acc) ->
                    #column{name = Field, type = Type} = lists:nth(Pos, Columns),
                    case convert(Type, erlang:element(Pos, Row)) of
                        null -> Acc;
                        Value -> Acc#{Field => Value}
                    end
                end,
                #{},
                Seq
            ),
            TransformRowFun(Data)
        end,
        Rows
    ).

%% for reference https://github.com/epgsql/epgsql#data-representation
convert(_Type, null) ->
    null;
convert(timestamp, Value) ->
    daytime_to_unixtime(Value);
convert(timestamptz, Value) ->
    daytime_to_unixtime(Value);
convert(jsonb, Value) ->
    jsx:decode(Value, [return_maps]);
convert(json, Value) ->
    jsx:decode(Value, [return_maps]);
convert(_Type, Value) ->
    Value.

daytime_to_unixtime({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    daytime_to_unixtime({Date, {Hour, Minute, trunc(Second)}});
daytime_to_unixtime(Daytime) ->
    to_unixtime(calendar:datetime_to_gregorian_seconds(Daytime)).

to_unixtime(Time) when is_integer(Time) ->
    Time - ?EPOCH_DIFF.

unixtime_to_datetime(null) ->
    null;
unixtime_to_datetime(TimestampSec) ->
    calendar:gregorian_seconds_to_datetime(TimestampSec + ?EPOCH_DIFF).

unixtime_to_text(TimestampSec) ->
    {
        {Year, Month, Day},
        {Hour, Minute, Seconds}
    } = calendar:gregorian_seconds_to_datetime(TimestampSec + ?EPOCH_DIFF),
    <<
        (integer_to_binary(Year))/binary, "-",
        (maybe_add_zero(Month))/binary, "-",
        (maybe_add_zero(Day))/binary, " ",
        (maybe_add_zero(Hour))/binary, "-",
        (maybe_add_zero(Minute))/binary, "-",
        (maybe_add_zero(Seconds))/binary
    >>.

maybe_add_zero(Val) when Val < 10 ->
    <<"0", (integer_to_binary(Val))/binary>>;
maybe_add_zero(Val) ->
    integer_to_binary(Val).

json_encode(null) ->
    null;
json_encode(MetaData) ->
    jsx:encode(MetaData).

%% Marshalling

marshal_task(Task) ->
    maps:fold(fun
        (_, null, Acc) -> Acc;
        (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
        (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
        (<<"task_type">>, TaskType, Acc) -> Acc#{task_type => TaskType};
        (<<"status">>, Status, Acc) -> Acc#{status => Status};
        (<<"scheduled_time">>, Ts, Acc) -> Acc#{scheduled_time => Ts};
        (<<"running_time">>, Ts, Acc) -> Acc#{running_time => Ts};
        (<<"args">>, Args, Acc) -> Acc#{args => Args};
        (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
        (<<"idempotency_key">>, IdempotencyKey, Acc) -> Acc#{idempotency_key => IdempotencyKey};
        (<<"response">>, Response, Acc) -> Acc#{response => Response};
        (<<"blocked_task">>, BlockedTaskId, Acc) -> Acc#{blocked_task => BlockedTaskId};
        (<<"last_retry_interval">>, LastRetryInterval, Acc) -> Acc#{last_retry_interval => LastRetryInterval};
        (<<"attempts_count">>, AttemptsCount, Acc) -> Acc#{attempts_count => AttemptsCount};
        (<<"context">>, Context, Acc) -> Acc#{context => Context};
        (_, _, Acc) -> Acc
    end, #{}, Task).

marshal_process(Process) ->
    maps:fold(fun
        (_, null, Acc) -> Acc;
        (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
        (<<"status">>, Status, Acc) -> Acc#{status => Status};
        (<<"detail">>, Detail, Acc) -> Acc#{detail => Detail};
        (<<"aux_state">>, AuxState, Acc) -> Acc#{aux_state => AuxState};
        (<<"p_meta">>, Meta, Acc) -> Acc#{metadata => Meta};
        (<<"corrupted_by">>, CorruptedBy, Acc) -> Acc#{corrupted_by => CorruptedBy};
        (_, _, Acc) -> Acc
    end, #{}, Process).

marshal_event(Event) ->
    maps:fold(fun
        (_, null, Acc) -> Acc;
        (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
        (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
        (<<"event_id">>, EventId, Acc) -> Acc#{event_id => EventId};
        (<<"timestamp">>, Ts, Acc) -> Acc#{timestamp => Ts};
        (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
        (<<"payload">>, Payload, Acc) -> Acc#{payload => Payload};
        (_, _, Acc) -> Acc
    end, #{}, Event).
%%

get_pool(internal, #{pool := Pool}) ->
    Pool;
get_pool(external, #{pool := BasePool} = PgOpts) ->
    maps:get(front_pool, PgOpts, BasePool);
get_pool(scan, #{pool := BasePool} = PgOpts) ->
    maps:get(scan_pool, PgOpts, BasePool).
