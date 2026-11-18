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
-export([reschedule_task/3]).

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
            [Task] = to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1),
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

-spec get_process(recipient(), pg_opts(), namespace_id(), id(), generation()) ->
    {ok, process()} | {error, _Reason}.
get_process(external = Recipient, #{cache := _DbRef} = PgOpts, NsId, ProcessId, Generation) ->
    case prg_pg_cache:get(NsId, ProcessId, Generation) of
        undefined ->
            get_process(Recipient, maps:without([cache], PgOpts), NsId, ProcessId, Generation);
        {ok, _} = Response ->
            Response
    end;
get_process(Recipient, PgOpts, NsId, ProcessId, Generation) ->
    Pool = get_pool(Recipient, PgOpts),
    #{
        processes := ProcessesTable,
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    GenCondition = generation_condition(Generation),
    RawResult = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            case do_get_process(Connection, ProcessesTable, ProcessId) of
                {ok, _, []} ->
                    {error, <<"process not found">>};
                {ok, ColumnsPr, RowsPr} ->
                    {ok, _, _} =
                        {ok, ColumnstSt, RowsSt} = do_get_state(Connection, GensTable, ProcessId, GenCondition),
                    {ok, {ColumnsPr, RowsPr}, {ColumnstSt, RowsSt}}
            end
        end
    ),
    case RawResult of
        {error, _} = Error ->
            Error;
        {ok, {ProcColumns, ProcRows}, {StateColumns, StateRows}} ->
            [Process] = to_maps(ProcColumns, ProcRows, fun prg_pg_utils:marshal_process/1),
            case to_maps(StateColumns, StateRows, fun prg_pg_utils:marshal_process_state/1) of
                [] when Generation =:= latest, not is_map_key(current_generation, Process) ->
                    {ok, Process};
                [] when Generation =:= latest ->
                    {error, {current_generation_not_found, maps:get(current_generation, Process)}};
                [] ->
                    {error, {generation_not_found, Generation}};
                [ProcState] ->
                    {ok, Process#{state => ProcState}}
            end
    end.

-spec repair_process(pg_opts(), namespace_id(), id()) -> ok | no_return().
repair_process(PgOpts, NsId, ProcessId) ->
    Pool = get_pool(external, PgOpts),
    #{processes := ProcessesTable} = prg_pg_utils:tables(NsId),
    {ok, 1} = epg_pool:query(
        Pool,
        "UPDATE " ++ ProcessesTable ++
            " SET status = 'running', previous_status = 'error', "
            "corrupted_by = null, corrupted_type = null, detail = null "
            "WHERE process_id = $1",
        [ProcessId]
    ),
    ok.

-spec process_trace(pg_opts(), namespace_id(), id()) -> {ok, process_flat_trace()} | {error, _Reason}.
process_trace(PgOpts, NsId, ProcessId) ->
    Pool = get_pool(external, PgOpts),
    #{
        tasks := TaskTable,
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    Result = epg_pool:query(
        Pool,
        "SELECT "
        "    nt.*,"
        "    ne.generation AS state_generation,"
        "    ne.timestamp AS state_timestamp,"
        "    ne.metadata AS state_metadata,"
        "    ne.payload AS state_payload "
        "FROM " ++ TaskTable ++
            " nt "
            "LEFT JOIN " ++ GensTable ++
            " ne "
            "ON nt.task_id = ne.task_id AND nt.process_id = ne.process_id "
            "WHERE nt.process_id = $1 ORDER BY nt.running_time, nt.finished_time, nt.task_id, ne.generation",
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
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _R} =
                epg_pool:query(Connection, "DELETE FROM " ++ RunningTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _S} =
                epg_pool:query(Connection, "DELETE FROM " ++ ScheduleTable ++ " WHERE process_id = $1", [ProcessId]),
            {ok, _E} =
                epg_pool:query(Connection, "DELETE FROM " ++ GensTable ++ " WHERE process_id = $1", [ProcessId]),
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
    NowMicroSec = erlang:system_time(microsecond),
    Now = unixtime_to_datetime(NowMicroSec),
    TsBackward = unixtime_to_datetime(NowMicroSec - ((Timeout + ?PROTECT_TIMEOUT) * 1000000)),
    {ok, _, _} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            epg_pool:query(
                Connection,
                "WITH zombie_tasks as ("
                "  DELETE FROM " ++ RunningTable ++
                    " WHERE running_time < $1 "
                    "    RETURNING process_id, task_id, task_type"
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
                    "  corrupted_by = zt.task_id, corrupted_type = zt.task_type",
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
    NowMicroSec = erlang:system_time(microsecond),
    Now = unixtime_to_datetime(NowMicroSec),
    NowText = unixtime_to_text(NowMicroSec),
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
                        "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS.US') as running_time, "
                        "      args, generation, metadata, "
                        "      last_retry_interval, attempts_count, context"
                        "  ) "
                        "INSERT INTO " ++ RunningTable ++
                        "  (task_id, process_id, task_type, status, scheduled_time, running_time,"
                        "   args, generation, metadata, last_retry_interval, attempts_count, context) "
                        "  SELECT * FROM tasks_for_run RETURNING *",
                    [Now, NowText, Limit]
                )
            end
        ),
    to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1).

-spec capture_task(pg_opts(), namespace_id(), task_id()) -> [task()].
capture_task(PgOpts, NsId, TaskId) ->
    Pool = get_pool(internal, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    NowMicroSec = erlang:system_time(microsecond),
    NowText = unixtime_to_text(NowMicroSec),
    {ok, Columns, Rows} =
        _Res = epg_pool:transaction(
            Pool,
            fun(Connection) ->
                epg_pool:query(
                    Connection,
                    "WITH current_task AS (SELECT * FROM " ++ ScheduleTable ++
                        " WHERE task_id = $2 AND status = 'waiting' FOR UPDATE), "
                        "deleted_from_schedule AS ("
                        "  DELETE FROM " ++ ScheduleTable ++
                        " WHERE task_id = $2 AND status = 'waiting' "
                        "    RETURNING"
                        "      task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                        "      TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS.US') as running_time, "
                        "      args, generation, metadata, "
                        "      last_retry_interval, attempts_count, context"
                        "), "
                        "inserted_to_running AS ("
                        "  INSERT INTO " ++ RunningTable ++
                        "    (task_id, process_id, task_type, status, scheduled_time, running_time,"
                        "       args, generation, metadata, last_retry_interval, attempts_count, context) "
                        "     SELECT * FROM deleted_from_schedule RETURNING * "
                        ") "
                        "SELECT * FROM current_task c FULL OUTER JOIN inserted_to_running i ON c.task_id = i.task_id",
                    [NowText, TaskId]
                )
            end
        ),
    to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1).

-spec reschedule_task(pg_opts(), namespace_id(), task()) -> ok | no_return().
reschedule_task(PgOpts, NsId, #{task_id := TaskId} = Task) ->
    Pool = get_pool(internal, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    ScheduledTask = maps:merge(
        maps:without([running_time], Task),
        #{status => <<"waiting">>}
    ),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_delete_running(Connection, RunningTable, TaskId),
            {ok, _, _, _} = do_save_schedule(Connection, ScheduleTable, ScheduledTask)
        end
    ),
    ok.

-spec search_calls(pg_opts(), namespace_id(), pos_integer()) -> [task()].
search_calls(PgOpts, NsId, Limit) ->
    Pool = get_pool(scan, PgOpts),
    #{
        schedule := ScheduleTable,
        running := RunningTable
    } = prg_pg_utils:tables(NsId),
    NowMicroSec = erlang:system_time(microsecond),
    Now = unixtime_to_text(NowMicroSec),
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
                    "      TO_TIMESTAMP($1, 'YYYY-MM-DD HH24:MI:SS.US') as running_time, "
                    "      args, generation, metadata, "
                    "      last_retry_interval, attempts_count, context"
                    "  ) "
                    "INSERT INTO " ++ RunningTable ++
                    "  (task_id, process_id, task_type, status, scheduled_time, running_time, args,"
                    "   generation, metadata, last_retry_interval, attempts_count, context) "
                    "  SELECT * FROM tasks_for_run RETURNING *",
                [Now, Limit]
            )
        end
    ),
    to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1).

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

-spec complete_and_continue(
    pg_opts(), namespace_id(), task_result(), process_updates(), optional_process_state(), task()
) ->
    {ok, [task()]}.
complete_and_continue(PgOpts, NsId, TaskResult, ProcessUpdates, NextState, NextTask) ->
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
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            {ok, _} = do_save_generation(Connection, GensTable, ProcessId, TaskId, NextState),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            {ok, _, _, [{NextTaskId}]} = do_save_task(Connection, TaskTable, NextTask),
            case
                do_complete_task(
                    Connection,
                    TaskTable,
                    ScheduleTable,
                    RunningTable,
                    TaskResult#{process_id => ProcessId}
                )
            of
                {ok, _, _, []} ->
                    case NextTask of
                        #{status := <<"running">>} ->
                            do_save_running(
                                Connection,
                                RunningTable,
                                NextTask#{task_id => NextTaskId, running_time => erlang:system_time(microsecond)},
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
    {ok, to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1)}.

-spec complete_and_suspend(pg_opts(), namespace_id(), task_result(), process_updates(), process_state()) ->
    {ok, [task()]}.
complete_and_suspend(PgOpts, NsId, TaskResult, ProcessUpdates, NextState) ->
    % update completed task and process, cancel blocked and waiting timers
    % Only task timers (timeout/remove) are cancelled, task calls must be processed!!!
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, _, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            {ok, _} = do_save_generation(Connection, GensTable, ProcessId, TaskId, NextState),
            {ok, _, _} = do_cancel_timer(Connection, TaskTable, ScheduleTable, ProcessId),
            do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult#{process_id => ProcessId})
        end
    ),
    {ok, to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1)}.

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

-spec complete_and_unlock(pg_opts(), namespace_id(), task_result(), process_updates(), process_state()) ->
    {ok, [task()]}.
complete_and_unlock(PgOpts, NsId, TaskResult, ProcessUpdates, NextState) ->
    % update completed task and process, unlock blocked task
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    #{task_id := TaskId} = TaskResult,
    #{process_id := ProcessId} = ProcessUpdates,
    {ok, Columns, Rows} = epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _} = do_update_process(Connection, ProcessesTable, ProcessUpdates),
            {ok, _} = do_save_generation(Connection, GensTable, ProcessId, TaskId, NextState),
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
    {ok, to_maps(Columns, Rows, fun prg_pg_utils:marshal_task/1)}.

-spec db_init(pg_opts(), namespace_id()) -> ok.
db_init(PgOpts, NsId) ->
    Pool = get_pool(internal, PgOpts),
    {ok, Pools} = application:get_env(epg_connector, pools),
    {ok, Databases} = application:get_env(epg_connector, databases),
    #{database := DbRef} = maps:get(Pool, Pools),
    DbOpts = maps:get(DbRef, Databases),

    PrivDir = code:priv_dir(progressor),
    MigrationsDir = filename:join([PrivDir, "migrations"]),
    MigrationOpts = [{namespace, NsId}],
    Realm = erlang:atom_to_binary(NsId),

    {ok, _} = epg_migrator:perform(Realm, DbOpts, MigrationOpts, MigrationsDir),
    ok.

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(PgOpts, NsId) ->
    Pool = get_pool(internal, PgOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        generations := GensTable
    } = prg_pg_utils:tables(NsId),
    epg_pool:transaction(
        Pool,
        fun(Connection) ->
            {ok, _, _} = epg_pool:query(Connection, "ALTER TABLE " ++ ProcessesTable ++ " DROP COLUMN corrupted_by"),
            {ok, _, _} = epg_pool:query(Connection, "DROP TABLE " ++ GensTable),
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

generation_condition(latest) ->
    " ORDER BY generation DESC LIMIT 1";
generation_condition(Generation) ->
    " AND generation = " ++ integer_to_list(Generation) ++ " ".

do_get_state(Connection, GensTable, ProcessId, GenCondition) ->
    SQL = "SELECT * FROM " ++ GensTable ++ " WHERE process_id = $1 " ++ GenCondition,
    epg_pool:query(
        Connection,
        SQL,
        [ProcessId]
    ).

do_get_process(Connection, Table, ProcessId) ->
    epg_pool:query(
        Connection,
        "SELECT * from " ++ Table ++ " WHERE process_id = $1",
        [ProcessId]
    ).

do_save_process(Connection, Table, Process) ->
    #{
        process_id := ProcessId,
        status := Status
    } = Process,
    Detail = maps:get(detail, Process, null),
    AuxState = maps:get(aux_state, Process, null),
    Meta = maps:get(metadata, Process, null),
    CreatedAtTs = maps:get(created_at, Process, erlang:system_time(microsecond)),
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
    Generation = maps:get(generation, Task, null),
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
            "   metadata, idempotency_key, blocked_task, response, last_retry_interval, "
            "   attempts_count, context, generation)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) RETURNING " ++ Returning,
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
            Context,
            Generation
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
    Generation = maps:get(generation, Task, null),
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    RunningTs = erlang:system_time(microsecond),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++
            " "
            "  (task_id, process_id, task_type, status, scheduled_time, running_time, "
            "    args, metadata, last_retry_interval, attempts_count, context, generation)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) "
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
            Context,
            Generation
        ]
    ).

do_delete_running(Connection, Table, TaskId) ->
    epg_pool:query(
        Connection,
        "DELETE FROM " ++ Table ++ " WHERE task_id = $1",
        [TaskId]
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
    Generation = maps:get(generation, Task, null),
    Args = maps:get(args, Task, null),
    MetaData = maps:get(metadata, Task, null),
    Context = maps:get(context, Task, <<>>),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ Table ++
            " "
            "  (task_id, process_id, task_type, status, scheduled_time, args,"
            "   metadata, last_retry_interval, attempts_count, context, generation)"
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING " ++ Returning,
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
            Context,
            Generation
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

do_save_generation(_Connection, _GensTable, _ProcessId, _TaskId, undefined) ->
    {ok, 0};
do_save_generation(Connection, GensTable, ProcessId, TaskId, ProcessState) ->
    #{
        generation := Generation,
        timestamp := GenTs,
        payload := Payload
    } = ProcessState,
    MetaData = maps:get(metadata, ProcessState, null),
    epg_pool:query(
        Connection,
        "INSERT INTO " ++ GensTable ++
            " (process_id, task_id, generation, timestamp, payload, metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
        [ProcessId, TaskId, Generation, unixtime_to_datetime(GenTs), Payload, json_encode(MetaData)]
    ).

do_complete_task(Connection, TaskTable, ScheduleTable, RunningTable, TaskResult) ->
    #{
        task_id := TaskId,
        process_id := ProcessId,
        status := Status
    } = TaskResult,
    Generation = maps:get(generation, TaskResult, null),
    Response = maps:get(response, TaskResult, null),
    RunningTime1 = maps:get(running_time, TaskResult, null),
    FinishedTime = maps:get(finished_time, TaskResult, erlang:system_time(microsecond)),
    {ok, _} = epg_pool:query(
        Connection,
        "WITH deleted AS("
        "  DELETE FROM " ++ RunningTable ++
            " WHERE process_id = $1"
            "  )"
            "UPDATE " ++ TaskTable ++
            " SET status = $2, response = $3, "
            " running_time = $4, finished_time = $5, generation = $7 WHERE task_id = $6",
        [
            ProcessId,
            Status,
            Response,
            unixtime_to_datetime(RunningTime1),
            unixtime_to_datetime(FinishedTime),
            TaskId,
            Generation
        ]
    ),
    case Status of
        <<"error">> ->
            %% do nothing
            {ok, 0, [], []};
        _ ->
            %% search waiting call
            RunningTime = unixtime_to_text(erlang:system_time(microsecond)),
            epg_pool:query(
                Connection,
                "WITH postponed_tasks AS ("
                "  DELETE FROM " ++ ScheduleTable ++
                    " WHERE task_id = "
                    "    (SELECT min(task_id) FROM " ++ ScheduleTable ++
                    "      WHERE process_id = $1 AND status = 'waiting' AND task_type IN ('call', 'repair')) "
                    "    RETURNING task_id, process_id, task_type, 'running'::task_status as status, scheduled_time, "
                    "      TO_TIMESTAMP($2, 'YYYY-MM-DD HH24:MI:SS.US') as running_time, "
                    "      args, generation, metadata, "
                    "      last_retry_interval, attempts_count, context"
                    "  ) "
                    "INSERT INTO " ++ RunningTable ++
                    " (task_id, process_id, task_type, status, scheduled_time, running_time, args, "
                    "  generation, metadata, last_retry_interval, attempts_count, context)"
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
    NowMicroSec = erlang:system_time(microsecond),
    Now = unixtime_to_datetime(NowMicroSec),
    NowText = unixtime_to_text(NowMicroSec),
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
            "      TO_TIMESTAMP($3, 'YYYY-MM-DD HH24:MI:SS.US') as running_time, "
            "      args, generation, metadata, "
            "      last_retry_interval, attempts_count, context"
            "), "
            "running_task AS ("
            "  INSERT INTO " ++ RunningTable ++
            "    (task_id, process_id, task_type, status, scheduled_time, running_time,"
            "       args, generation, metadata, last_retry_interval, attempts_count, context) "
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

daytime_to_unixtime({Date, {Hour, Minute, SecondWithMicro}}) ->
    MicroPart = trunc(SecondWithMicro * 1000000) rem 1000000,
    GregorianSeconds = calendar:datetime_to_gregorian_seconds({Date, {Hour, Minute, trunc(SecondWithMicro)}}),
    (GregorianSeconds - ?EPOCH_DIFF) * 1000000 + MicroPart.

unixtime_to_datetime(null) ->
    null;
unixtime_to_datetime(Timestamp) ->
    {Sec, MicroPart} = prg_utils:split_timestamp(Timestamp),
    {Date, {Hour, Minute, Second}} = calendar:gregorian_seconds_to_datetime(Sec + ?EPOCH_DIFF),
    {Date, {Hour, Minute, Second + MicroPart / 1000000}}.

unixtime_to_text(Timestamp) ->
    {Sec, MicroPart} = prg_utils:split_timestamp(Timestamp),
    {{Year, Month, Day}, {Hour, Minute, Seconds}} =
        calendar:gregorian_seconds_to_datetime(Sec + ?EPOCH_DIFF),
    <<
        (integer_to_binary(Year))/binary,
        "-",
        (maybe_add_zero(Month))/binary,
        "-",
        (maybe_add_zero(Day))/binary,
        " ",
        (maybe_add_zero(Hour))/binary,
        ":",
        (maybe_add_zero(Minute))/binary,
        ":",
        (maybe_add_zero(Seconds))/binary,
        ".",
        (prg_utils:format_microseconds(MicroPart))/binary
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

marshal_trace(Trace) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"task_type">>, TaskType, Acc) -> Acc#{task_type => TaskType};
            (<<"status">>, TaskStatus, Acc) -> Acc#{task_status => TaskStatus};
            (<<"scheduled_time">>, ScheduledTs, Acc) -> Acc#{scheduled => prg_utils:to_microseconds(ScheduledTs)};
            (<<"running_time">>, RunningTs, Acc) -> Acc#{running => prg_utils:to_microseconds(RunningTs)};
            (<<"finished_time">>, FinishedTs, Acc) -> Acc#{finished => prg_utils:to_microseconds(FinishedTs)};
            (<<"args">>, Args, Acc) -> Acc#{args => Args};
            (<<"metadata">>, Meta, Acc) -> Acc#{task_metadata => Meta};
            (<<"context">>, Context, Acc) -> Acc#{context => Context};
            (<<"idempotency_key">>, Key, Acc) -> Acc#{idempotency_key => Key};
            (<<"response">>, Response, Acc) -> Acc#{response => binary_to_term(Response)};
            (<<"last_retry_interval">>, Interval, Acc) -> Acc#{retry_interval => Interval};
            (<<"attempts_count">>, Attempts, Acc) -> Acc#{retry_attempts => Attempts};
            (<<"state_generation">>, Generation, Acc) -> Acc#{state_generation => Generation};
            (<<"generation">>, Generation, Acc) -> Acc#{generation => Generation};
            (<<"state_timestamp">>, Ts, Acc) -> Acc#{state_timestamp => prg_utils:to_microseconds(Ts)};
            (<<"state_metadata">>, Meta, Acc) -> Acc#{state_metadata => Meta};
            (<<"state_payload">>, Payload, Acc) -> Acc#{state_payload => Payload};
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
