-module(prg_pg_backend).

-include_lib("epgsql/include/epgsql.hrl").
-include_lib("progressor/include/progressor.hrl").

%% API

%% api handler functions
-export([health_check/1]).
-export([get_task_result/3]).
-export([get_process_status/3]).
-export([prepare_init/4]).
-export([prepare_call/4]).
-export([prepare_repair/4]).
-export([put_process_data/4]).
-export([process_trace/3]).
-export([repair_process/3]).

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
-export([capture_task/3]).

%% shared functions
-export([get_task/4]).
-export([get_process/5]).

%% Init operations
-export([db_init/2]).

-export([cleanup/2]).

-type pg_opts() :: #{
    pool := atom(),
    front_pool => atom(),
    scan_pool => atom(),
    cache => DbRef :: atom()
}.

-export_type([pg_opts/0]).

%% second
-define(PROTECT_TIMEOUT, 5).

-spec health_check(pg_opts()) -> ok | {error, Reason :: term()} | no_return().
health_check(PgOpts) ->
    Pool = get_pool(internal, PgOpts),
    case epg_pool:query(Pool, "SELECT 1") of
        {ok, _, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

-spec get_task_result(pg_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, term()} | {error, _Reason}.
get_task_result(PgOpts, NsId, KeyOrId) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable
    } = prg_pg_utils:tables(NsId),
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
    #{
        tasks := TaskTable
    } = prg_pg_utils:tables(NsId),
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
    #{
        processes := Table
    } = prg_pg_utils:tables(NsId),
    {ok, _Columns, Rows} = epg_pool:query(
        Pool,
        "SELECT status from " ++ Table ++ " WHERE process_id = $1",
        [Id]
    ),
    case Rows of
        [] -> {error, <<"process not found">>};
        [{Status}] -> {ok, Status}
    end.

-spec get_process(recipient(), pg_opts(), namespace_id(), id(), history_range()) ->
    {ok, process()} | {error, _Reason}.
get_process(external = Recipient, #{cache := _DbRef} = PgOpts, NsId, ProcessId, HistoryRange) ->
    case prg_pg_cache:get(NsId, ProcessId, HistoryRange) of
        undefined ->
            get_process(Recipient, maps:without([cache], PgOpts), NsId, ProcessId, HistoryRange);
        {ok, _} = Response ->
            Response
    end;
get_process(Recipient, PgOpts, NsId, ProcessId, HistoryRange) ->
    Pool = get_pool(Recipient, PgOpts),
    #{
        processes := ProcessesTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    RangeCondition = create_range_condition(HistoryRange),
    RawResult = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            case do_get_process(Connection, ProcessesTable, ProcessId) of
                {ok, _, []} ->
                    {error, <<"process not found">>};
                {ok, ColumnsPr, RowsPr} ->
                    {ok, _, _} =
                        {ok, ColumnstEv, RowsEv} = do_get_events(Connection, EventsTable, ProcessId, RangeCondition),
                    LastEventId = get_last_event_id(Connection, EventsTable, ProcessId),
                    {ok, {ColumnsPr, RowsPr}, {ColumnstEv, RowsEv}, LastEventId}
            end
        end
    ),
    parse_process_info(RawResult, HistoryRange).

-spec repair_process(pg_opts(), namespace_id(), id()) -> ok | no_return().
repair_process(PgOpts, NsId, ProcessId) ->
    Pool = get_pool(external, PgOpts),
    #{processes := ProcessesTable} = prg_pg_utils:tables(NsId),
    {ok, 1} = epg_pool:query(
        Pool,
        "UPDATE " ++ ProcessesTable ++
            " SET status = 'running', previous_status = 'error', corrupted_by = null, detail = null "
            "WHERE process_id = $1",
        [ProcessId]
    ),
    ok.

-spec put_process_data(
    pg_opts(),
    namespace_id(),
    id(),
    #{process := process(), init_task := task(), active_task => task() | undefined}
) ->
    {ok, _Result} | {error, _Reason}.
put_process_data(PgOpts, NsId, ProcessId, ProcessData) ->
    #{
        process := #{
            history := Events
        } = Process,
        init_task := InitTask
    } = ProcessData,
    ActiveTask = maps:get(active_task, ProcessData, undefined),
    Pool = get_pool(external, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            case do_save_process(Connection, ProcessesTable, Process) of
                {ok, _} ->
                    {ok, _, _, [{InitTaskId}]} = do_save_task(Connection, TaskTable, InitTask),
                    lists:foreach(
                        fun(Ev) ->
                            {ok, _} = do_save_event(Connection, EventsTable, ProcessId, InitTaskId, Ev)
                        end,
                        Events
                    ),
                    ok = maybe_schedule_task(Connection, TaskTable, ScheduleTable, ActiveTask),
                    {ok, ok};
                {error, #error{codename = unique_violation}} ->
                    {error, <<"process already exists">>}
            end
        end
    ).

-spec process_trace(pg_opts(), namespace_id(), id()) -> {ok, process_flat_trace()} | {error, _Reason}.
process_trace(PgOpts, NsId, ProcessId) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    Result = epg_pool:query(
        Pool,
        "SELECT "
        "    nt.*,"
        "    ne.event_id,"
        "    ne.timestamp AS event_timestamp,"
        "    ne.metadata AS event_metadata,"
        "    ne.payload AS event_payload "
        "FROM " ++ TaskTable ++
            " nt "
            "LEFT JOIN " ++ EventsTable ++
            " ne "
            "ON nt.task_id = ne.task_id AND nt.process_id = ne.process_id "
            "WHERE nt.process_id = $1 ORDER BY nt.task_id, ne.event_id",
        [ProcessId]
    ),
    case Result of
        {ok, _, []} ->
            {error, <<"process not found">>};
        {ok, Columns, Rows} ->
            {ok, to_maps(Columns, Rows, fun marshal_trace/1)};
        Error ->
            logger:warning("Process tracing error: ~p", [Error]),
            {error, unexpected_result}
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
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _R} =
                epg_pool:query(Connection, "DELETE FROM " ++ RunningTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _S} =
                epg_pool:query(Connection, "DELETE FROM " ++ ScheduleTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _E} =
                epg_pool:query(Connection, "DELETE FROM " ++ EventsTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _T} = epg_pool:query(Connection, "DELETE FROM " ++ TaskTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _P} =
                epg_pool:query(Connection, "DELETE FROM " ++ ProcessesTable ++ " WHERE process_id = $1", [ProcessId])
        end
    ),
    ok.

-spec collect_zombies(pg_opts(), namespace_id(), timeout_sec()) -> ok.
collect_zombies(PgOpts, NsId, Timeout) ->
    Pool = get_pool(scan, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    TsBackward = unixtime_to_datetime(NowSec - (Timeout + ?PROTECT_TIMEOUT)),
    {ok, _, _} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            epg_pool:query(
                Connection,
                "WITH zombie_tasks as ("
                "  DELETE FROM " ++ RunningTable ++
                    " WHERE running_time < $1 "
                    "    RETURNING process_id, task_id"
                    "  ), "
                    "  t1 AS (UPDATE " ++ TaskTable ++
                    " SET status = 'cancelled' WHERE status IN ('waiting', 'blocked') "
                    "    AND process_id IN (SELECT process_id FROM zombie_tasks)),"
                    "  t2 AS (UPDATE " ++ TaskTable ++
                    " SET status = 'error', finished_time = $2 WHERE task_id IN (SELECT task_id FROM zombie_tasks)),"
                    "  t3 AS (DELETE FROM " ++ ScheduleTable ++
                    " WHERE process_id IN (SELECT process_id FROM zombie_tasks))"
                    "MERGE INTO " ++ ProcessesTable ++
                    " AS pt USING zombie_tasks AS zt ON pt.process_id = zt.process_id "
                    "  WHEN MATCHED THEN UPDATE SET"
                    "  status = 'error',"
                    "  detail = 'zombie detected',"
                    "  corrupted_by = zt.task_id",
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
    } = prg_pg_utils:tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    NowText = unixtime_to_text(NowSec),
    {ok, _, Columns, Rows} =
        _Res = epg_pool:transaction(
            Pool,
            fun(Connection) ->
                {ok, _, _, _} = epg_pool:query(
                    Connection,
                    "WITH tasks_for_run as("
                    "  DELETE FROM " ++ ScheduleTable ++
                        " WHERE task_id IN "
                        "    (SELECT task_id FROM " ++ ScheduleTable ++
                        " WHERE status = 'waiting' AND scheduled_time <= $1 "
                        "      AND task_type IN ('timeout', 'remove') AND process_id NOT IN (SELECT process_id FROM " ++
                        RunningTable ++
                        " )"
                        "      ORDER BY scheduled_time ASC LIMIT $3)"
                        "    RETURNING"
                        "      task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                        "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                        "      last_retry_interval, attempts_count, context"
                        "  ) "
                        "INSERT INTO " ++ RunningTable ++
                        "  (task_id, process_id, task_type, status, scheduled_time, running_time,"
                        "   args, metadata, last_retry_interval, attempts_count, context) "
                        "  SELECT * FROM tasks_for_run RETURNING *",
                    [Now, NowText, Limit]
                )
            end
        ),
    to_maps(Columns, Rows, fun marshal_task/1).

-spec capture_task(pg_opts(), namespace_id(), task_id()) -> [task()].
capture_task(PgOpts, NsId, TaskId) ->
    Pool = get_pool(internal, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    NowSec = erlang:system_time(second),
    NowText = unixtime_to_text(NowSec),
    {ok, Columns, Rows} =
        _Res = epg_pool:transaction(
            Pool,
            fun(Connection) ->
                epg_pool:query(
                    Connection,
                    "WITH current_task AS (SELECT * FROM " ++ ScheduleTable ++
                        " WHERE task_id = $2 FOR UPDATE), "
                        "deleted_from_schedule AS ("
                        "  DELETE FROM " ++ ScheduleTable ++
                        " WHERE task_id = $2 AND status = 'waiting' "
                        "    RETURNING"
                        "      task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                        "      TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                        "      last_retry_interval, attempts_count, context"
                        "), "
                        "inserted_to_running AS ("
                        "  INSERT INTO " ++ RunningTable ++
                        "    (task_id, process_id, task_type, status, scheduled_time, running_time,"
                        "       args, metadata, last_retry_interval, attempts_count, context) "
                        "     SELECT * FROM deleted_from_schedule RETURNING * "
                        ") "
                        "SELECT * FROM current_task c FULL OUTER JOIN inserted_to_running i ON c.task_id = i.task_id",
                    [NowText, TaskId]
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
    } = prg_pg_utils:tables(NsId),
    NowSec = erlang:system_time(second),
    Now = unixtime_to_text(NowSec),
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _, _} = epg_pool:query(
                Connection,
                "WITH tasks_for_run as("
                "  DELETE FROM " ++ ScheduleTable ++
                    " WHERE task_id IN "
                    "    (SELECT min(task_id) FROM " ++ ScheduleTable ++
                    " WHERE status = 'waiting' AND task_type IN ('init', 'call', 'repair')"
                    "      AND process_id NOT IN (SELECT process_id FROM " ++ RunningTable ++
                    " )"
                    "      GROUP BY process_id ORDER BY min ASC LIMIT $2"
                    "    ) "
                    "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                    "      TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                    "      last_retry_interval, attempts_count, context"
                    "  ) "
                    "INSERT INTO " ++ RunningTable ++
                    "  (task_id, process_id, task_type, status, scheduled_time, running_time, args,"
                    "   metadata, last_retry_interval, attempts_count, context) "
                    "  SELECT * FROM tasks_for_run RETURNING *",
                [Now, Limit]
            )
        end
    ),
    to_maps(Columns, Rows, fun marshal_task/1).

-spec prepare_init(pg_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_init(PgOpts, NsId, ProcessId, Task) ->
    Pool = get_pool(external, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    Process = ?NEW_PROCESS(ProcessId),
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
    } = prg_pg_utils:tables(NsId),
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
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(C) ->
            {ok, _, _, [{TaskId}]} = do_save_task(C, TaskTable, Task),
            {ok, _BlockedTaskId} = do_block_timer(C, ScheduleTable, ProcessId),
            case do_save_running(C, RunningTable, Task#{task_id => TaskId}) of
                {ok, _, _, []} ->
                    {ok, _, _, _} =
                        do_save_schedule(C, ScheduleTable, Task#{task_id => TaskId, status => <<"waiting">>}),
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
    } = prg_pg_utils:tables(NsId),
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
    } = prg_pg_utils:tables(NsId),
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

-spec complete_and_continue(pg_opts(), namespace_id(), task_result(), process_updates(), [event()], task()) ->
    {ok, [task()]}.
complete_and_continue(PgOpts, NsId, TaskResult, ProcessUpdates, Events, NextTask) ->
    % update completed task and process,
    % cancel blocked and waiting timers,
    % save new timer,
    % return continuation call if exists otherwise current task
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            %% TODO implement via batch execute
            lists:foreach(
                fun(Ev) ->
                    {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
                end,
                Events
            ),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _, [{NextTaskId}]} = do_save_task(Connection, TaskTable, NextTask),
            case
                do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult#{
                    process_id => ProcessId
                })
            of
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
                            do_save_schedule(
                                Connection,
                                ScheduleTable,
                                NextTask#{task_id => NextTaskId},
                                " * "
                            )
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

-spec complete_and_suspend(pg_opts(), namespace_id(), task_result(), process_updates(), [event()]) ->
    {ok, [task()]}.
complete_and_suspend(PgOpts, NsId, TaskResult, ProcessUpdates, Events) ->
    % update completed task and process, cancel blocked and waiting timers
    % Only task timers (timeout/remove) are cancelled, task calls must be processed!!!
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            lists:foreach(
                fun(Ev) ->
                    {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
                end,
                Events
            ),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult#{process_id => ProcessId})
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec complete_and_error(pg_opts(), namespace_id(), task_result(), process_updates()) -> ok.
complete_and_error(PgOpts, NsId, TaskResult, ProcessUpdates) ->
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, _, _, _} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _} = do_cancel_calls(Connection, TaskTable, ScheduleTable, ProcessId),
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

-spec complete_and_unlock(pg_opts(), namespace_id(), task_result(), process_updates(), [event()]) ->
    {ok, [task()]}.
complete_and_unlock(PgOpts, NsId, TaskResult, ProcessUpdates, Events) ->
    % update completed task and process, unlock blocked task
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            lists:foreach(
                fun(Ev) ->
                    {ok, _} = do_save_event(Connection, EventsTable, ProcessId, TaskId, Ev)
                end,
                Events
            ),
            Completion = do_complete_task(
                Connection,
                TaskTable,
                ScheduleTable,
                RunningTable,
                TaskResult#{process_id => ProcessId}
            ),
            case Completion of
                {ok, _, _, []} ->
                    do_unlock_timer(Connection, ScheduleTable, RunningTable, ProcessId);
                {ok, _, Col, Row} ->
                    %% if postponed call exists then timer remain blocked
                    {ok, Col, Row}
            end
        end
    ),
    {ok, to_maps(Columns, Rows, fun marshal_task/1)}.

-spec db_init(pg_opts(), namespace_id()) -> ok.
db_init(PgOpts, NsId) ->
    prg_pg_migration:db_init(PgOpts, NsId).

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(PgOpts, NsId) ->
    prg_pg_migration:cleanup(PgOpts, NsId).

%-endif.

%% Internal functions

create_range_condition(Range) ->
    after_id(Range) ++ direction(Range) ++ limit(Range).

after_id(#{offset := After} = Range) ->
    Direction = maps:get(direction, Range, forward),
    " AND event_id " ++ operator(Direction) ++ integer_to_list(After) ++ " ";
after_id(_) ->
    " ".

operator(forward) ->
    " > ";
operator(backward) ->
    " < ".

limit(#{limit := Limit}) ->
    " LIMIT " ++ integer_to_list(Limit) ++ " ";
limit(_) ->
    " ".

direction(#{direction := backward}) ->
    " ORDER BY event_id DESC ";
direction(_) ->
    " ORDER BY event_id ASC ".

do_get_process(Connection, Table, ProcessId) ->
    epg_pool:query(
        Connection,
        "SELECT * FROM " ++ Table ++ " WHERE process_id = $1",
        [ProcessId]
    ).

parse_process_info(RawResult, HistoryRange) ->
    case RawResult of
        {error, _} = Error ->
            Error;
        {ok, {ProcColumns, ProcRows}, {EventsColumns, EventsRows}, LastEventId} ->
            [Process] = to_maps(ProcColumns, ProcRows, fun marshal_process/1),
            History = to_maps(EventsColumns, EventsRows, fun marshal_event/1),
            {ok, Process#{history => History, last_event_id => LastEventId, range => HistoryRange}}
    end.

do_get_events(Connection, EventsTable, ProcessId, RangeCondition) ->
    SQL = "SELECT * FROM " ++ EventsTable ++ " WHERE process_id = $1 " ++ RangeCondition,
    epg_pool:query(
        Connection,
        SQL,
        [ProcessId]
    ).

get_last_event_id(Connection, EventsTable, ProcessId) ->
    SQL = "SELECT max(event_id) FROM " ++ EventsTable ++ " WHERE process_id = $1",
    Result = epg_pool:query(
        Connection,
        SQL,
        [ProcessId]
    ),
    case Result of
        {ok, _, [{null}]} ->
            0;
        {ok, _, [{Value}]} ->
            Value
    end.

do_save_process(Connection, Table, Process) ->
    #{
        process_id := ProcessId,
        status := Status
    } = Process,
    Detail = maps:get(detail, Process, null),
    AuxState = maps:get(aux_state, Process, null),
    Meta = maps:get(metadata, Process, null),
    CreatedAtTs = maps:get(created_at, Process, erlang:system_time(second)),
    CreatedAt = unixtime_to_datetime(CreatedAtTs),
    PreviousStatus = maps:get(previous_status, Process, Status),
    StatusChangedAt = unixtime_to_datetime(maps:get(status_changed_at, Process, CreatedAtTs)),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++
            " "
            "  (process_id, status, detail, aux_state, metadata, created_at, previous_status, status_changed_at) "
            "  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        [ProcessId, Status, Detail, AuxState, json_encode(Meta), CreatedAt, PreviousStatus, StatusChangedAt]
    ).

maybe_schedule_task(_Connection, _TaskTable, _ScheduleTable, undefined) ->
    ok;
maybe_schedule_task(Connection, TaskTable, ScheduleTable, Task) ->
    {ok, _, _, [{TaskId}]} = do_save_task(Connection, TaskTable, Task),
    {ok, _, _, _} = do_save_schedule(Connection, ScheduleTable, Task#{task_id => TaskId}),
    ok.

do_save_task(Connection, Table, Task) ->
    do_save_task(Connection, Table, Task, " task_id ").

do_save_task(Connection, Table, Task, Returning) ->
    #{
        process_id := ProcessId,
        task_type := TaskType,
        status := Status,
        last_retry_interval := LastRetryInterval,
        attempts_count := AttemptsCount
    } = Task,
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    IdempotencyKey = maps:get(idempotency_key, Task, null),
    BlockedTask = maps:get(blocked_task, Task, null),
    ScheduledTs = unixtime_to_datetime(maps:get(scheduled_time, Task, null)),
    RunningTs = unixtime_to_datetime(maps:get(running_time, Task, null)),
    FinishedTs = unixtime_to_datetime(maps:get(finished_time, Task, null)),
    Response = maps:get(response, Task, null),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++
            " "
            "  (process_id, task_type, status, scheduled_time, running_time, finished_time, args, "
            "   metadata, idempotency_key, blocked_task, response, last_retry_interval, attempts_count, context)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING " ++ Returning,
        [
            ProcessId,
            TaskType,
            Status,
            ScheduledTs,
            RunningTs,
            FinishedTs,
            Args,
            json_encode(MetaData),
            IdempotencyKey,
            BlockedTask,
            Response,
            LastRetryInterval,
            AttemptsCount,
            Context
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
        "INSERT INTO " ++ Table ++
            " "
            "  (task_id, process_id, task_type, status, scheduled_time, running_time, "
            "    args, metadata, last_retry_interval, attempts_count, context)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "
            "  ON CONFLICT (process_id) DO NOTHING RETURNING " ++ Returning,
        [
            TaskId,
            ProcessId,
            TaskType,
            Status,
            unixtime_to_datetime(ScheduledTs),
            unixtime_to_datetime(RunningTs),
            Args,
            json_encode(MetaData),
            LastRetryInterval,
            AttemptsCount,
            Context
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
        "INSERT INTO " ++ Table ++
            " "
            "  (task_id, process_id, task_type, status, scheduled_time, args,"
            "   metadata, last_retry_interval, attempts_count, context)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING " ++ Returning,
        [
            TaskId,
            ProcessId,
            TaskType,
            Status,
            unixtime_to_datetime(ScheduledTs),
            Args,
            json_encode(MetaData),
            LastRetryInterval,
            AttemptsCount,
            Context
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
    {ProcessId, Updates} = maps:take(process_id, Process),
    {SQL, Params} = construct_process_update_req(ProcessesTable, ProcessId, maps:with(?CLEAR_PROCESS_KEYS, Updates)),
    case Params of
        [] ->
            {ok, 0};
        _ ->
            epg_pool:query(
                Connection,
                SQL,
                Params
            )
    end.

construct_process_update_req(_ProcessesTable, _ProcessId, Updates) when map_size(Updates) =:= 0 ->
    {"", []};
construct_process_update_req(ProcessesTable, ProcessId, Updates) ->
    InitSql = "UPDATE " ++ ProcessesTable ++ " SET ",
    {SQL0, Params0, Counter0} = maps:fold(
        fun(Key, Value, {SQL, Params, ParamsCount}) ->
            Next = ParamsCount + 1,
            Pos = "$" ++ erlang:integer_to_list(Next),
            Lead =
                case ParamsCount of
                    0 -> " ";
                    _ -> ", "
                end,
            {
                SQL ++ Lead ++ erlang:atom_to_list(Key) ++ " = " ++ Pos,
                Params ++ [convert_process_updates(Key, Value)],
                Next
            }
        end,
        {InitSql, [], 0},
        Updates
    ),
    LastPos = "$" ++ erlang:integer_to_list(Counter0 + 1),
    {
        SQL0 ++ " WHERE process_id = " ++ LastPos,
        Params0 ++ [ProcessId]
    }.

convert_process_updates(_Key, undefined) ->
    null;
convert_process_updates(metadata, Value) ->
    json_encode(Value);
convert_process_updates(Key, Value) when Key =:= created_at; Key =:= status_changed_at ->
    unixtime_to_datetime(Value);
convert_process_updates(_Key, Value) ->
    Value.

do_save_event(Connection, EventsTable, ProcessId, TaskId, Event) ->
    #{
        event_id := EventId,
        timestamp := EventTs,
        payload := Payload
    } = Event,
    MetaData = maps:get(metadata, Event, null),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ EventsTable ++
            " (process_id, task_id, event_id, timestamp, payload, metadata) "
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
        "  DELETE FROM " ++ RunningTable ++
            " WHERE process_id = $4"
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
                "  DELETE FROM " ++ ScheduleTable ++
                    " WHERE task_id = "
                    "    (SELECT min(task_id) FROM " ++ ScheduleTable ++
                    "      WHERE process_id = $1 AND status = 'waiting' AND task_type IN ('call', 'repair')) "
                    "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                    "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
                    "      last_retry_interval, attempts_count, context"
                    "  ) "
                    "INSERT INTO " ++ RunningTable ++
                    " (task_id, process_id, task_type, status, scheduled_time, running_time, args, "
                    "  metadata, last_retry_interval, attempts_count, context)"
                    " SELECT * FROM postponed_tasks RETURNING *",
                [ProcessId, RunningTime]
            )
    end.

do_block_timer(Connection, ScheduleTable, ProcessId) ->
    {ok, _, _Columns, Rows} = epg_pool:query(
        Connection,
        "UPDATE " ++ ScheduleTable ++
            " SET status = 'blocked' WHERE task_type IN ('timeout', 'remove') AND "
            "process_id = $1 AND status = 'waiting' RETURNING task_id",
        [ProcessId]
    ),
    case Rows of
        [] -> {ok, null};
        [{TaskId}] -> {ok, TaskId}
    end.

do_unlock_timer(Connection, ScheduleTable, RunningTable, ProcessId) ->
    NowSec = erlang:system_time(second),
    Now = unixtime_to_datetime(NowSec),
    NowText = unixtime_to_text(NowSec),
    epg_pool:query(
        Connection,
        "WITH unblocked_task AS (UPDATE" ++ ScheduleTable ++
            " SET status = 'waiting' "
            "  WHERE process_id = $1 AND status = 'blocked' AND scheduled_time > $2 RETURNING * "
            "), "
            "delayed_task AS ("
            "  DELETE FROM " ++ ScheduleTable ++
            "  WHERE process_id = $1 AND status = 'blocked' AND scheduled_time <= $2"
            "    RETURNING"
            "      task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
            "      TO_TIMESTAMP($3, 'YYYY-MM-DD HH24:MI:SS') as running_time, args, metadata, "
            "      last_retry_interval, attempts_count, context"
            "), "
            "running_task AS ("
            "  INSERT INTO " ++ RunningTable ++
            "    (task_id, process_id, task_type, status, scheduled_time, running_time,"
            "       args, metadata, last_retry_interval, attempts_count, context) "
            "     SELECT * FROM delayed_task RETURNING * "
            ") "
            "SELECT * FROM unblocked_task ut FULL OUTER JOIN running_task rt ON ut.task_id = rt.task_id",
        [ProcessId, Now, NowText]
    ).

do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId) ->
    epg_pool:query(
        Connection,
        "WITH deleted_tasks as("
        "  DELETE FROM " ++ ScheduleTable ++
            " WHERE process_id = $1 AND task_type IN ('timeout', 'remove') "
            "    AND (status = 'waiting' OR status = 'blocked') RETURNING task_id"
            "  ) "
            "MERGE INTO " ++ TaskTable ++
            " as tt USING deleted_tasks as dt ON tt.task_id = dt.task_id "
            "  WHEN MATCHED THEN UPDATE SET status = 'cancelled'",
        [ProcessId]
    ).

do_cancel_calls(Connection, TaskTable, ScheduleTable, ProcessId) ->
    epg_pool:query(
        Connection,
        "WITH deleted_tasks as("
        "  DELETE FROM " ++ ScheduleTable ++
            " WHERE process_id = $1 AND task_type NOT IN ('timeout', 'remove') AND status = 'waiting' "
            "    RETURNING task_id"
            "  ) "
            "MERGE INTO " ++ TaskTable ++
            " as tt USING deleted_tasks as dt ON tt.task_id = dt.task_id "
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
        (integer_to_binary(Year))/binary,
        "-",
        (maybe_add_zero(Month))/binary,
        "-",
        (maybe_add_zero(Day))/binary,
        " ",
        (maybe_add_zero(Hour))/binary,
        "-",
        (maybe_add_zero(Minute))/binary,
        "-",
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
    maps:fold(
        fun
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
        end,
        #{},
        Task
    ).

marshal_process(Process) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"status">>, Status, Acc) -> Acc#{status => Status};
            (<<"detail">>, Detail, Acc) -> Acc#{detail => Detail};
            (<<"aux_state">>, AuxState, Acc) -> Acc#{aux_state => AuxState};
            (<<"metadata">>, Meta, Acc) -> Acc#{metadata => Meta};
            (<<"corrupted_by">>, CorruptedBy, Acc) -> Acc#{corrupted_by => CorruptedBy};
            (<<"initialization">>, TaskId, Acc) -> Acc#{initialization => TaskId};
            (<<"previous_status">>, PrevStatus, Acc) -> Acc#{previous_status => PrevStatus};
            (_, _, Acc) -> Acc
        end,
        #{},
        Process
    ).

marshal_event(Event) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"event_id">>, EventId, Acc) -> Acc#{event_id => EventId};
            (<<"timestamp">>, Ts, Acc) -> Acc#{timestamp => Ts};
            (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
            (<<"payload">>, Payload, Acc) -> Acc#{payload => Payload};
            (_, _, Acc) -> Acc
        end,
        #{},
        Event
    ).

marshal_trace(Trace) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"task_type">>, TaskType, Acc) -> Acc#{task_type => TaskType};
            (<<"status">>, TaskStatus, Acc) -> Acc#{task_status => TaskStatus};
            (<<"scheduled_time">>, ScheduledTs, Acc) -> Acc#{scheduled => ScheduledTs};
            (<<"running_time">>, RunningTs, Acc) -> Acc#{running => RunningTs};
            (<<"finished_time">>, FinishedTs, Acc) -> Acc#{finished => FinishedTs};
            (<<"args">>, Args, Acc) -> Acc#{args => Args};
            (<<"metadata">>, Meta, Acc) -> Acc#{task_metadata => Meta};
            (<<"idempotency_key">>, Key, Acc) -> Acc#{idempotency_key => Key};
            (<<"response">>, Response, Acc) -> Acc#{response => binary_to_term(Response)};
            (<<"last_retry_interval">>, Interval, Acc) -> Acc#{retry_interval => Interval};
            (<<"attempts_count">>, Attempts, Acc) -> Acc#{retry_attempts => Attempts};
            (<<"event_id">>, EventId, Acc) -> Acc#{event_id => EventId};
            (<<"event_timestamp">>, Ts, Acc) -> Acc#{event_timestamp => Ts};
            (<<"event_metadata">>, Meta, Acc) -> Acc#{event_metadata => Meta};
            (<<"event_payload">>, Payload, Acc) -> Acc#{event_payload => Payload};
            (_, _, Acc) -> Acc
        end,
        #{},
        Trace
    ).

%%

get_pool(internal, #{pool := Pool}) ->
    Pool;
get_pool(external, #{pool := BasePool} = PgOpts) ->
    maps:get(front_pool, PgOpts, BasePool);
get_pool(scan, #{pool := BasePool} = PgOpts) ->
    maps:get(scan_pool, PgOpts, BasePool).
