-module(prg_pg_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("progressor/include/progressor.hrl").

%% API
%% Task management
-export([get_task_result/3]).
-export([save_task/3]).
-export([get_task/3]).

%% Process management
-export([get_process_status/3]).
-export([get_process/4]).
-export([remove_process/3]).

%% Complex operations
-export([search_tasks/4]).
-export([prepare_init/4]).
-export([prepare_call/4]).
-export([prepare_repair/4]).
-export([complete_and_continue/6]).
-export([complete_and_suspend/5]).
-export([complete_and_error/4]).
-export([complete_and_unlock/5]).

%% Init operations
-export([db_init/2]).

%-ifdef(TEST).
-export([cleanup/2]).
%-endif.

-type pg_opts() :: #{pool := atom()}.

-define(PROTECT_TIMEOUT, 5). %% second

%% Task management
-spec get_task_result(pg_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, term()} | {error, _Reason}.
get_task_result(#{pool := Pool}, NsId, KeyOrId) ->
    TaskTable = construct_table_name(NsId, "_tasks"),
    case do_get_task_result(Pool, TaskTable, KeyOrId) of
        {ok, _, []} ->
            {error, not_found};
        {ok, _, [{null}]} ->
            {error, in_progress};
        {ok, _, [{Value}]} ->
            {ok, binary_to_term(Value)}
    end.

-spec get_task(pg_opts(), namespace_id(), task_id()) -> {ok, task()} | {error, _Reason}.
get_task(#{pool := Pool}, NsId, TaskId) ->
    TaskTable = construct_table_name(NsId, "_tasks"),
    case do_get_task(Pool, TaskTable, TaskId) of
        {ok, _, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            [Task] = to_maps(Columns, Rows, fun marshal_task/1),
            {ok, Task}
    end.

-spec save_task(pg_opts(), namespace_id(), task()) -> {ok, task_id()}.
save_task(#{pool := Pool}, NsId, Task) ->
    Table = construct_table_name(NsId, "_tasks"),
    {ok, _, _, [{TaskId}]} = do_save_task(Pool, Table, Task),
    {ok, TaskId}.

%% Process management
-spec get_process_status(pg_opts(), namespace_id(), id()) -> term().
get_process_status(#{pool := Pool}, NsId, Id) ->
    Table = construct_table_name(NsId, "_processes"),
    {ok, _Columns, Rows} = epgsql_pool:query(
        Pool,
        "SELECT status from " ++ Table ++ " WHERE process_id = $1",
        [Id]
    ),
    case Rows of
        [] -> {error, <<"process not found">>};
        [{Status}] -> {ok, Status}
    end.

-spec get_process(pg_opts(), namespace_id(), id(), history_range()) -> {ok, process()}.
get_process(#{pool := Pool}, NsId, ProcessId, HistoryRange) ->
    EventsTable = construct_table_name(NsId, "_events"),
    ProcessesTable = construct_table_name(NsId, "_processes"),
    RangeCondition = create_range_condition(HistoryRange),
    %% TODO optimize request
    {ok, Columns, Rows} = epgsql_pool:query(
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
remove_process(#{pool := Pool}, NsId, ProcessId) ->
    LocksTable = construct_table_name(NsId, "_locks"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    EventsTable = construct_table_name(NsId, "_events"),
    ProcessesTable = construct_table_name(NsId, "_processes"),
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _L} = epgsql_pool:query(Connection, "DELETE FROM " ++ LocksTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _E} = epgsql_pool:query(Connection, "DELETE FROM " ++ EventsTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _T} = epgsql_pool:query(Connection, "DELETE FROM " ++ TaskTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _P} = epgsql_pool:query(Connection, "DELETE FROM " ++ ProcessesTable ++ " WHERE process_id = $1", [ProcessId])
        end
    ),
    ok.

%% Complex operations
-spec search_tasks(pg_opts(), namespace_id(), timeout_sec(), pos_integer()) -> [task()].
search_tasks(#{pool := Pool}, NsId, Timeout, Limit) ->
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    ProcessesTable = construct_table_name(NsId, "_processes"),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    TsBackward = unixtime_to_datetime(NowSec - (Timeout + ?PROTECT_TIMEOUT)),
    {ok, Columns, Rows} = _Res = epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            %% TODO maybe separated process for zombie collection
            %% zombie task with type init, call, notify, repair change status to error, and change process status to error
            {ok, _} = epgsql_pool:query(
                Connection,
                "WITH zombie_tasks as ("
                "  UPDATE " ++ TaskTable ++ " SET status = 'error'"
                "    WHERE running_time < $1 AND status = 'running' AND task_type IN ('init', 'call', 'notify', 'repair')"
                "    RETURNING process_id"
                "), "
                "t AS (DELETE FROM " ++ LocksTable ++ " WHERE process_id IN (SELECT process_id FROM zombie_tasks)) "
                "UPDATE " ++ ProcessesTable ++ " SET status = 'error', detail = 'unexpected error' "
                "WHERE process_id IN (SELECT process_id FROM zombie_tasks)",
                [TsBackward]
            ),
            {ok, _, _} = epgsql_pool:query(
                %% timeout tasks
                Connection,
                "WITH running_tasks as("
                "  UPDATE " ++ TaskTable ++ " SET status = 'running', running_time = $1 WHERE task_id IN "
                "    (SELECT task_id FROM " ++ TaskTable ++ " WHERE "
                       %% condition for normal scheduled timeout tasks
                "      (status = 'waiting' AND scheduled_time <= $1 AND task_type IN ('timeout', 'remove')) "
                       %% condition for zombie timeout tasks
                "      OR (status = 'running' AND running_time < $2 AND task_type IN ('timeout', 'remove')) "
                "      ORDER BY scheduled_time ASC LIMIT $3"
                "    ) RETURNING * "
                "), "
                "t2 AS (INSERT INTO " ++ LocksTable ++ " (process_id, task_id) SELECT process_id, task_id FROM running_tasks "
                "  ON CONFLICT (process_id) DO NOTHING) " %% zombie timers already lock process
                "SELECT * FROM running_tasks",
                [Now, TsBackward, Limit]
            )
        end
    ),
    to_maps(Columns, Rows, fun marshal_task/1).

-spec prepare_init(pg_opts(), namespace_id(), process(), task()) -> {ok, task_id()} | {error, _Reason}.
prepare_init(#{pool := Pool}, NsId, #{process_id := ProcessId} = Process, InitTask) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            case do_save_process(Connection, ProcessesTable, Process) of
                {ok, _} ->
                    {ok, _, _, [{TaskId}]} = do_save_task(Connection, TaskTable, InitTask),
                    {ok, _} = do_lock_process(Connection, LocksTable, ProcessId, TaskId),
                    {ok, TaskId};
                {error, #error{codename = unique_violation}} ->
                    {error, <<"process already exists">>}
            end
        end
    ).

-spec prepare_call(pg_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Error}.
prepare_call(#{pool := Pool}, NsId, ProcessId, Task) ->
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    %% TODO optimize request
    case try_lock_process(Pool, TaskTable, LocksTable, ProcessId, Task) of
        {ok, TaskId} ->
            {ok, {continue, TaskId}};
        error ->
            do_save_postponed_task(Pool, TaskTable, Task)
    end.

-spec prepare_repair(pg_opts(), namespace_id(), id(), task()) -> {ok, task_id()} | {error, _Reason}.
prepare_repair(#{pool := Pool}, NsId, ProcessId, RepairTask) ->
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    case try_lock_process(Pool, TaskTable, LocksTable, ProcessId, RepairTask) of
        {ok, TaskId} ->
            {ok, TaskId};
        error ->
            {error, <<"process is running">>}
    end.

-spec complete_and_continue(pg_opts(), namespace_id(), task_result(), process(), [event()], task()) ->
    {ok, [task()]}.
complete_and_continue(#{pool := Pool}, NsId, TaskResult, Process, Events, NextTask) ->
    % update completed task and process, cancel blocked and waiting tasks, save new task
    ProcessesTable = construct_table_name(NsId, "_processes"),
    EventsTable = construct_table_name(NsId, "_events"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    %% TODO optimize request
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            {ok, _} = do_cancel_timer(Connection, TaskTable, ProcessId),
            case do_complete_task(Connection, TaskTable, LocksTable, TaskResult#{process_id => ProcessId}) of
                {ok, _, []} ->
                    case NextTask of
                        #{status := <<"running">>} ->
                            {ok, _, Columns, Rows} = do_save_task(Connection, TaskTable, NextTask, " * "),
                            [#{task_id := NextTaskId} | _] = Marshaled = to_maps(Columns, Rows, fun marshal_task/1),
                            {ok, _} = do_lock_process(Connection, LocksTable, ProcessId, NextTaskId),
                            {ok, Marshaled};
                        #{status := <<"waiting">>} ->
                            {ok, _, _, _} = do_save_task(Connection, TaskTable, NextTask),
                            {ok, []}
                    end;
                {ok, Columns, Rows} ->
                    {ok, _, _, _} = do_save_task(Connection, TaskTable, NextTask#{status => <<"blocked">>}),
                    {ok, to_maps(Columns, Rows, fun marshal_task/1)}
            end
        end
    ).

-spec complete_and_suspend(pg_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_suspend(#{pool := Pool}, NsId, TaskResult, Process, Events) ->
    % update completed task and process, cancel blocked and waiting tasks
    ProcessesTable = construct_table_name(NsId, "_processes"),
    EventsTable = construct_table_name(NsId, "_events"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    %% TODO optimize request
    {ok, Columns, Rows} = epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            {ok, _} = do_cancel_timer(Connection, TaskTable, ProcessId),
            {ok, _, _} = do_complete_task(Connection, TaskTable, LocksTable, TaskResult#{process_id => ProcessId})
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec complete_and_error(pg_opts(), namespace_id(), task_result(), process()) -> ok.
complete_and_error(#{pool := Pool}, NsId, TaskResult, Process) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    #{process_id := ProcessId} = Process,
    %% TODO optimize request
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            {ok, _} = do_block_timer(Connection, TaskTable, ProcessId),
            {ok, _} = do_cancel_waiting_calls(Connection, TaskTable, ProcessId),
            {ok, _, _} = do_complete_task(Connection, TaskTable, LocksTable,TaskResult#{process_id => ProcessId})
        end
    ),
    ok.

-spec complete_and_unlock(pg_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_unlock(#{pool := Pool}, NsId, TaskResult, Process, Events) ->
    % update completed task and process, unlock blocked task
    ProcessesTable = construct_table_name(NsId, "_processes"),
    EventsTable = construct_table_name(NsId, "_events"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = Process,
    %% TODO optimize request
    {ok, Columns, Rows} = epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, Process),
            lists:foreach(fun(Ev) ->
                {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
            end, Events),
            case do_complete_task(Connection, TaskTable, LocksTable, TaskResult#{process_id => ProcessId}) of
                {ok, _, []} ->
                    {ok, _} = do_unlock_timer(Connection, TaskTable, ProcessId),
                    {ok, [], []};
                {ok, _Col, _Row} = OK ->
                    %% if postponed call exists then timer remain blocked
                    OK
            end
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec db_init(pg_opts(), namespace_id()) -> ok.
db_init(#{pool := Pool}, NsId) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    EventsTable = construct_table_name(NsId, "_events"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            %% create type process_status if not exists
            {ok, _, [{IsProcessStatusExists}]} = epgsql_pool:query(
                Connection,
                "select exists (select 1 from pg_type where typname = 'process_status')"
            ),
            case IsProcessStatusExists of
                true ->
                    ok;
                false ->
                    {ok, _, _} = epgsql_pool:query(
                        Connection,
                        "CREATE TYPE process_status AS ENUM ('running', 'error')"
                    )
            end,
            %% create type task_status if not exists
            {ok, _, [{IsTaskStatusExists}]} = epgsql_pool:query(
                Connection,
                "select exists (select 1 from pg_type where typname = 'task_status')"
            ),
            case IsTaskStatusExists of
                true ->
                    ok;
                false ->
                    {ok, _, _} = epgsql_pool:query(
                        Connection,
                        "CREATE TYPE task_status AS ENUM "
                        "('waiting', 'running', 'blocked', 'error', 'finished', 'cancelled')"
                    )
            end,
            %% create type task_type if not exists
            {ok, _, [{IsTaskTypeExists}]} = epgsql_pool:query(
                Connection,
                "select exists (select 1 from pg_type where typname = 'task_type')"
            ),
            case IsTaskTypeExists of
                true ->
                    ok;
                false ->
                    {ok, _, _} = epgsql_pool:query(
                        Connection,
                        "CREATE TYPE task_type AS ENUM ('init', 'timeout', 'call', 'notify', 'repair', 'remove')"
                    )
            end,
            %% create processes table
            {ok, _, _} = epgsql_pool:query(
                Connection,
                "CREATE TABLE IF NOT EXISTS " ++ ProcessesTable ++ " ("
                "process_id VARCHAR(80) PRIMARY KEY, "
                "status process_status NOT NULL, "
                "detail TEXT, "
                "aux_state BYTEA, "
                "metadata JSONB)"
            ),
            %% create tasks table
            {ok, _, _} = epgsql_pool:query(
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
            {ok, _, _} = epgsql_pool:query(
                Connection,
                "ALTER TABLE " ++ ProcessesTable ++
                " ADD COLUMN IF NOT EXISTS corrupted_by BIGINT REFERENCES " ++ TaskTable ++ "(task_id)"
            ),
            %% create events table
            {ok, _, _} = epgsql_pool:query(
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
            %% create index
            {ok, _, _} = epgsql_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS process_idx on " ++ EventsTable ++ " USING HASH (process_id)"
            ),
            %% create locks table
            {ok, _, _} = epgsql_pool:query(
                Connection,
                "CREATE TABLE IF NOT EXISTS " ++ LocksTable ++ " ("
                "process_id VARCHAR(80) PRIMARY KEY, "
                "task_id BIGINT NOT NULL, "
                "FOREIGN KEY (process_id) REFERENCES " ++ ProcessesTable ++ " (process_id), "
                "FOREIGN KEY (task_id) REFERENCES " ++ TaskTable ++ " (task_id))"
            )


        end),
    ok.

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(#{pool := Pool}, NsId) ->
    ProcessesTable = construct_table_name(NsId, "_processes"),
    EventsTable = construct_table_name(NsId, "_events"),
    TaskTable = construct_table_name(NsId, "_tasks"),
    LocksTable = construct_table_name(NsId, "_locks"),
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _} = epgsql_pool:query(Connection, "ALTER TABLE " ++ ProcessesTable ++ " DROP COLUMN corrupted_by"),
            {ok, _, _} = epgsql_pool:query(Connection, "DROP TABLE " ++ EventsTable),
            {ok, _, _} = epgsql_pool:query(Connection, "DROP TABLE " ++ LocksTable),
            {ok, _, _} = epgsql_pool:query(Connection, "DROP TABLE " ++ TaskTable),
            {ok, _, _} = epgsql_pool:query(Connection, "DROP TABLE " ++ ProcessesTable)
        end
    ),
    _ = epgsql_pool:query(Pool, "DROP TYPE task_type, task_status, process_status"),
    ok.

%-endif.

%% Internal functions

construct_table_name(NsId, Postfix) ->
    "\"" ++ erlang:atom_to_list(NsId) ++ Postfix ++ "\"".

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
    epgsql_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++ " (process_id, status, detail, aux_state, metadata) VALUES ($1, $2, $3, $4, $5)",
        [ProcessId, Status, Detail, AuxState, json_encode(Meta)]
    ).
%%
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
    epgsql_pool:query(
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

do_get_task_result(Connection, Table, {idempotency_key, IdempotencyKey}) ->
    epgsql_pool:query(
        Connection,
        "SELECT response FROM " ++ Table ++ " WHERE idempotency_key = $1",
        [IdempotencyKey]
    );
do_get_task_result(Connection, Table, {task_id, TaskId}) ->
    epgsql_pool:query(
        Connection,
        "SELECT response FROM " ++ Table ++ " WHERE task_id = $1",
        [TaskId]
    ).

do_get_task(Connection, Table, TaskId) ->
    epgsql_pool:query(
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
    epgsql_pool:query(
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
    epgsql_pool:query(
        Connection,
        "INSERT INTO " ++ EventsTable ++ " (process_id, task_id, event_id, timestamp, payload, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        [ProcessId, TaskId, EventId, unixtime_to_datetime(EventTs), Payload, json_encode(MetaData)]
    ).

do_complete_task(Connection, TaskTable, LocksTable, TaskResult) ->
    #{
        process_id := ProcessId,
        task_id := TaskId,
        status := Status
    } = TaskResult,
    Response = maps:get(response, TaskResult, null),
    FinishedTime = maps:get(finished_time, TaskResult, null),
    {ok, _} = epgsql_pool:query(
        Connection,
        %% save task result
        "WITH completed_tasks as ("
        "  UPDATE " ++ TaskTable ++ " SET status = $1, response = $2, finished_time = $3 "
        "  WHERE task_id = $4"
        ") "
        %% unlock process
        "DELETE FROM " ++ LocksTable ++ " WHERE task_id = $4",
        [Status, Response, unixtime_to_datetime(FinishedTime), TaskId]
    ),
    case Status of
        <<"error">> ->
            %% do nothing
            {ok, [], []};
        _ ->
            RunningTime = erlang:system_time(second),
            epgsql_pool:query(
                Connection,
                %% search postponed call
                "WITH postponed_tasks AS ("
                "  UPDATE " ++ TaskTable ++ " SET status = 'running', running_time = $2 WHERE task_id IN "
                "    (SELECT task_id FROM " ++ TaskTable ++ " WHERE "
                "      (process_id = $1 AND status = 'waiting' AND task_type IN ('call', 'repair', 'notify')) "
                "      ORDER BY task_id ASC LIMIT 1) RETURNING *"
                "), "
                %% lock process if postponed call exists
                "t AS (INSERT INTO " ++ LocksTable ++ " (process_id, task_id) SELECT process_id, task_id FROM postponed_tasks) "
                %% return postponed call
                "SELECT * FROM postponed_tasks",
                [ProcessId, unixtime_to_datetime(RunningTime)]
            )
    end.

do_update_task(Connection, TaskTable, Task) ->
    #{
        task_id := TaskId,
        status := Status
    } = Task,
    BlockedTask = maps:get(blocked_task, Task, null),
    epgsql_pool:query(
        Connection,
        "UPDATE " ++ TaskTable ++ " SET status = $1, blocked_task = $2 "
        "WHERE task_id = $3",
        [Status, BlockedTask, TaskId]
    ).

try_lock_process(Pool, TaskTable, LocksTable, ProcessId, Task) ->
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _, [{TaskId}]} = do_save_task(Connection, TaskTable, Task),
            case do_lock_process(Connection, LocksTable, ProcessId, TaskId) of
                {ok, _} ->
                    {ok, BlockedTaskId} = do_block_timer(Connection, TaskTable, ProcessId),
                    {ok, _} = do_update_task(Connection, TaskTable, Task#{task_id => TaskId, blocked_task => BlockedTaskId}),
                    {ok, TaskId};
                {error, #error{codename = unique_violation}} ->
                        error
            end
        end
    ).

do_lock_process(Connection, LocksTable, ProcessId, TaskId) ->
    epgsql_pool:query(
        Connection,
        "INSERT INTO " ++ LocksTable ++ " (process_id, task_id) VALUES ($1, $2)",
        [ProcessId, TaskId]
    ).

do_save_postponed_task(Pool, TaskTable, Task) ->
    epgsql_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _, [{TaskId}]} = do_save_task(Connection, TaskTable, Task),
            {ok, _} = do_update_task(Connection, TaskTable, Task#{task_id => TaskId, status => <<"waiting">>}),
            {ok, {postpone, TaskId}}
        end
    ).

do_block_timer(Connection, TaskTable, ProcessId) ->
    {ok, _, _Columns, Rows} = epgsql_pool:query(
        Connection,
        "UPDATE " ++ TaskTable ++ " SET status = 'blocked' WHERE task_type IN ('timeout', 'remove') AND "
        "process_id = $1 AND status = 'waiting' RETURNING task_id",
        [ProcessId]
    ),
    case Rows of
        [] -> {ok, null};
        [{TaskId}] -> {ok, TaskId}
    end.
%%
do_unlock_timer(Connection, TaskTable, ProcessId) ->
    epgsql_pool:query(
        Connection,
        "UPDATE " ++ TaskTable ++ " SET status = 'waiting' "
        "WHERE process_id = $1 AND status = 'blocked'",
        [ProcessId]
    ).
%%
do_cancel_timer(Connection, TaskTable, ProcessId) ->
    epgsql_pool:query(
        Connection,
        "UPDATE " ++ TaskTable ++ " SET status = 'cancelled' "
        "WHERE process_id = $1 AND task_type IN ('timeout', 'remove') AND (status = 'waiting' OR status = 'blocked')",
        [ProcessId]
    ).
%%
do_cancel_waiting_calls(Connection, TaskTable, ProcessId) ->
    epgsql_pool:query(
        Connection,
        "UPDATE " ++ TaskTable ++ " SET status = 'cancelled' "
        "WHERE process_id = $1 AND task_type NOT IN ('timeout', 'remove') AND status = 'waiting'",
        [ProcessId]
    ).

%do_block_error_timer(Connection, TaskTable, ProcessId) ->
%    {ok, _, _Columns, Rows} = epgsql_pool:query(
%        Connection,
%        "UPDATE " ++ TaskTable ++ " SET status = 'blocked' WHERE task_id IN "
%        "  (SELECT task_id FROM " ++ TaskTable ++ " WHERE process_id = $1 AND status = 'error' AND task_type IN ('timeout', 'remove') "
%        "   ORDER BY task_id DESC LIMIT 1) "
%        "RETURNING task_id",
%        [ProcessId]
%    ),
%    case Rows of
%        [] -> {ok, null};
%        [{TaskId}] -> {ok, TaskId}
%    end.

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
