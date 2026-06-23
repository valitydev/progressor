-module(prg_worker).

-behaviour(gen_server).

-include("progressor.hrl").

-export([start_link/2]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).
-export([handle_continue/2]).

-export([process_task/3]).
-export([continuation_task/1]).
-export([next_task/1]).
-export([process_scheduled_task/3]).

-record(prg_worker_state, {ns_id, ns_opts, process, sidecar_pid, continuation}).

-define(DEFAULT_RANGE, #{direction => forward}).
%% 1 second
-define(MIN_SCHEDULE_STEP_US, 1000000).
%% 10 millisecond
-define(SCHEDULE_DEFENSE_INTERVAL_US, 10000).
-define(SCHEDULE_DEFENSE_INTERVAL_MS, ?SCHEDULE_DEFENSE_INTERVAL_US div 1000).
%% Used to prevent timing errors caused by scheduler overhead
-define(EFFECTIVE_SCHEDULE_STEP_US, ?MIN_SCHEDULE_STEP_US - ?SCHEDULE_DEFENSE_INTERVAL_US).

%%%
%%% API
%%%

-spec process_task(pid(), task_header(), task()) -> ok.
process_task(Worker, TaskHeader, #{process_id := _ProcessId, task_id := _TaskId} = Task) ->
    gen_server:cast(Worker, {process_task, TaskHeader, Task, otel_ctx:get_current()}).

-spec continuation_task(pid()) -> ok.
continuation_task(Worker) ->
    gen_server:cast(Worker, {continuation_task, otel_ctx:get_current()}).

-spec next_task(pid()) -> ok.
next_task(Worker) ->
    gen_server:cast(Worker, next_task).

-spec process_scheduled_task(pid(), id(), task_id()) -> ok.
process_scheduled_task(Worker, ProcessId, TaskId) ->
    gen_server:cast(Worker, {process_scheduled_task, ProcessId, TaskId, otel_ctx:get_current()}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(NsId, NsOpts) ->
    gen_server:start_link(?MODULE, [NsId, NsOpts], []).

init([NsId, NsOpts]) ->
    {ok,
        #prg_worker_state{
            ns_id = NsId,
            ns_opts = NsOpts
        },
        {continue, do_start}}.

handle_continue(do_start, #prg_worker_state{ns_id = NsId} = State) ->
    %% FIXME Worker w/o OTEL context, since it is not passed to init w/ `start_child`
    {ok, Pid} = prg_worker_sidecar:start_link(),
    case prg_scheduler:pop_task(NsId, self()) of
        {TaskHeader, Task} ->
            ok = process_task(self(), TaskHeader, Task);
        not_found ->
            skip
    end,
    {noreply, State#prg_worker_state{sidecar_pid = Pid}}.

handle_call(_Request, _From, #prg_worker_state{} = State) ->
    {reply, ok, State}.

handle_cast(
    {process_task, TaskHeader, Task, OtelCtx},
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, process_step_timeout := TimeoutSec} = _NsOpts,
        sidecar_pid = Pid
    } = State
) ->
    _ = otel_ctx:attach(OtelCtx),
    Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
    ProcessId = maps:get(process_id, Task),
    HistoryRange = maps:get(range, maps:get(metadata, Task, #{}), #{}),
    {ok, Process} = prg_worker_sidecar:get_process(Pid, Deadline, StorageOpts, NsId, ProcessId, HistoryRange),
    NewState = do_process_task(TaskHeader, Task, Deadline, State#prg_worker_state{process = Process}),
    {noreply, NewState};
handle_cast(
    {continuation_task, OtelCtx},
    #prg_worker_state{ns_opts = #{process_step_timeout := TimeoutSec}, continuation = {TaskHeader, Task}} = State
) ->
    _ = otel_ctx:attach(OtelCtx),
    Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
    NewState = do_process_task(TaskHeader, Task, Deadline, State),
    {noreply, NewState};
handle_cast(
    {process_scheduled_task, ProcessId, TaskId, OtelCtx},
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, process_step_timeout := TimeoutSec} = _NsOpts,
        sidecar_pid = Pid
    } = State
) ->
    _ = otel_ctx:attach(OtelCtx),
    try prg_storage:capture_task(StorageOpts, NsId, TaskId) of
        [] ->
            %% task cancelled, blocked, already running or finished
            ok = next_task(self()),
            {noreply, State};
        [#{status := <<"running">>} = Task] ->
            Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
            HistoryRange = maps:get(range, maps:get(metadata, Task, #{}), #{}),
            {ok, Process} = prg_worker_sidecar:get_process(Pid, Deadline, StorageOpts, NsId, ProcessId, HistoryRange),
            TaskHeader = create_header(Task),
            NewState = do_process_task(TaskHeader, Task, Deadline, State#prg_worker_state{process = Process}),
            {noreply, NewState}
    catch
        Class:Term:Stacktrace ->
            logger:error("process ~p. task capturing exception: ~p", [ProcessId, [Class, Term, Stacktrace]]),
            ok = next_task(self()),
            {noreply, State}
    end;
handle_cast(next_task, #prg_worker_state{sidecar_pid = CurrentPid} = State) ->
    %% kill sidecar and restart to clear memory
    true = erlang:unlink(CurrentPid),
    true = erlang:exit(CurrentPid, kill),
    {stop, normal, State#prg_worker_state{continuation = undefined}}.

handle_info(_Info, #prg_worker_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #prg_worker_state{continuation = {{TaskType, _}, Task}} = State) when
    TaskType =:= timeout;
    TaskType =:= remove
->
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts},
        continuation = {_, #{task_id := TaskId}},
        process = #{process_id := ProcessId}
    } = State,
    try prg_storage:reschedule_task(StorageOpts, NsId, Task) of
        ok ->
            logger:warning("process ~p reschedule task ~p when terminate", [ProcessId, TaskId])
    catch
        Class:Term:Trace ->
            logger:error(
                "process ~p reschedule task ~p error: ~p",
                [ProcessId, TaskId, {Class, Term, Trace}]
            )
    end,
    ok;
terminate(_Reason, #prg_worker_state{} = _State) ->
    ok.

code_change(_OldVsn, #prg_worker_state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% remove process by timer
do_process_task(
    _TaskHeader,
    #{task_type := <<"remove">>} = _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = _Process,
        sidecar_pid = Pid
    } = State
) ->
    ok = prg_worker_sidecar:lifecycle_sink(Pid, Deadline, NsOpts, remove, ProcessId),
    ok = prg_worker_sidecar:remove_process(Pid, Deadline, StorageOpts, NsId, ProcessId),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined};
do_process_task(
    TaskHeader,
    Task,
    Deadline,
    #prg_worker_state{
        ns_id = _NsId,
        ns_opts = NsOpts,
        process = Process,
        sidecar_pid = Pid
    } = State
) ->
    Args = maps:get(args, Task, <<>>),
    Ctx = maps:get(context, Task, <<>>),
    Request = {extract_task_type(TaskHeader), Args, Process},
    Result = prg_worker_sidecar:process(Pid, Deadline, NsOpts, Request, Ctx),
    State1 = maybe_restore_history(Task, State),
    case Result of
        {ok, Intent} ->
            handle_result_success(Intent, TaskHeader, Task, Deadline, State1);
        {error, _} ->
            handle_result_error(Result, TaskHeader, Task, Deadline, State1)
    end.

maybe_restore_history(#{metadata := #{range := Range}}, State) when Range =:= ?DEFAULT_RANGE ->
    State;
%% if task range is defined then need restore full history for continuation
maybe_restore_history(
    #{metadata := #{range := Range}},
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, process_step_timeout := TimeoutSec} = _NsOpts,
        sidecar_pid = Pid,
        process = #{process_id := ProcessId}
    } = State
) when map_size(Range) > 0 ->
    Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
    {ok, ProcessUpd} = prg_worker_sidecar:get_process(Pid, Deadline, StorageOpts, NsId, ProcessId, #{}),
    State#prg_worker_state{process = ProcessUpd};
%% if task range undefined then history is full
maybe_restore_history(_, State) ->
    State.

handle_result_success(Intent, TaskHeader, Task, Deadline, State) ->
    Action = maps:get(action, Intent, idle),
    dispatch_action(Action, Intent, TaskHeader, Task, Deadline, State).

dispatch_action(idle, Intent, TaskHeader, Task, Deadline, State) ->
    success_and_unlock(Intent, TaskHeader, Task, Deadline, State);
dispatch_action(suspend, Intent, TaskHeader, Task, Deadline, State) ->
    success_and_suspend(Intent, TaskHeader, Task, Deadline, State);
dispatch_action(remove, Intent, TaskHeader, Task, Deadline, State) ->
    success_and_remove(Intent, TaskHeader, Task, Deadline, State);
dispatch_action(timeout, Intent, TaskHeader, Task, Deadline, State) ->
    success_and_continue(
        Intent, TaskHeader, Task, Deadline, State, timeout, erlang:system_time(microsecond)
    );
dispatch_action({schedule, #{at := Timestamp0, action := Action}}, Intent, TaskHeader, Task, Deadline, State) ->
    success_and_continue(Intent, TaskHeader, Task, Deadline, State, Action, Timestamp0).

handle_result_error(Result, {TaskType, _} = TaskHeader, Task, Deadline, State) when
    TaskType =:= timeout;
    TaskType =:= remove
->
    error_and_retry(Result, TaskHeader, Task, Deadline, State);
handle_result_error(Result, {TaskType, _} = TaskHeader, Task, Deadline, State) when
    TaskType =:= init;
    TaskType =:= call;
    TaskType =:= repair
->
    error_and_stop(Result, TaskHeader, Task, Deadline, State).

success_and_continue(Intent, TaskHeader, Task, Deadline, State, Action, Timestamp0) ->
    #{events := Events} = Intent,
    #{context := Context} = Task,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId, status := OldStatus} = Process,
        sidecar_pid = Pid
    } = State,
    Timestamp = prg_utils:to_microseconds(Timestamp0),
    Now = erlang:system_time(microsecond),
    {#{status := NewStatus} = ProcessUpdated, Updates} = update_process(Process, Intent),
    Response = response(Intent),
    TaskResult = task_result(Task, <<"finished">>, Response),
    NewTask = #{
        process_id => ProcessId,
        task_type => prg_utils:action_to_task_type(Action),
        status => create_status(Timestamp, Now),
        scheduled_time => Timestamp,
        context => Context,
        last_retry_interval => 0,
        attempts_count => 0
    },
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, lifecycle_event(TaskHeader, OldStatus, NewStatus), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    SaveResult = prg_worker_sidecar:complete_and_continue(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        Updates,
        Events,
        NewTask
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, [#{status := <<"waiting">>, task_id := NextTaskId, scheduled_time := Ts} | _]} ->
            %% if status=waiting then expression (Ts - Now) div 1000
            %% is guaranteed to return >= 1000 because see create_status/1
            RunAfterMs = (Ts - Now) div 1000 - ?SCHEDULE_DEFENSE_INTERVAL_MS,
            ok = prg_scheduler:schedule_task(NsId, ProcessId, NextTaskId, RunAfterMs),
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self()),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)},
                continuation = {create_header(ContinuationTask), ContinuationTask}
            }
    end.

success_and_remove(Intent, TaskHeader, _Task, Deadline, State) ->
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = _Process,
        sidecar_pid = Pid
    } = State,
    Response = response(Intent),
    ok = prg_worker_sidecar:lifecycle_sink(Pid, Deadline, NsOpts, remove, ProcessId),
    ok = prg_worker_sidecar:remove_process(Pid, Deadline, StorageOpts, NsId, ProcessId),
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined}.

success_and_suspend(Intent, TaskHeader, Task, Deadline, State) ->
    #{events := Events} = Intent,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId, status := OldStatus} = Process,
        sidecar_pid = Pid
    } = State,
    {#{status := NewStatus} = ProcessUpdated, Updates} = update_process(Process, Intent),
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, lifecycle_event(TaskHeader, OldStatus, NewStatus), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    Response = response(Intent),
    TaskResult = task_result(Task, <<"finished">>, Response),
    SaveResult = prg_worker_sidecar:complete_and_suspend(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        Updates,
        Events
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, []} ->
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self()),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)},
                continuation = {create_header(ContinuationTask), ContinuationTask}
            }
    end.

success_and_unlock(
    Intent,
    {repair, _} = TaskHeader,
    Task,
    Deadline,
    #prg_worker_state{process = #{corrupted_by := ErrorTaskId}} = State
) ->
    %% machinegun legacy behaviour
    #{events := Events} = Intent,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State,
    Now = erlang:system_time(microsecond),
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, repair, ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    {ProcessUpdated, Updates} = update_process(Process, Intent),
    Response = response(Intent),
    TaskResult = task_result(Task, <<"finished">>, Response),
    {ok, ErrorTask} = prg_worker_sidecar:get_task(Pid, Deadline, StorageOpts, NsId, ErrorTaskId),
    case ErrorTask of
        #{task_type := Type} when Type =:= <<"timeout">>; Type =:= <<"remove">> ->
            NewTask0 = maps:with(
                [process_id, task_type, scheduled_time, args, metadata, context], ErrorTask
            ),
            NewTask = NewTask0#{
                status => <<"running">>,
                running_time => Now,
                last_retry_interval => 0,
                attempts_count => 0
            },
            %% FIXME Otel must drop trace here - right before moving to other tasks
            {ok, [ContinuationTask | _]} = prg_worker_sidecar:complete_and_continue(
                Pid,
                Deadline,
                StorageOpts,
                NsId,
                TaskResult,
                Updates,
                Events,
                NewTask
            ),
            _ = maybe_reply(TaskHeader, Response),
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self()),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)},
                continuation = {create_header(ContinuationTask), ContinuationTask}
            };
        _ ->
            {ok, []} = prg_worker_sidecar:complete_and_unlock(
                Pid,
                Deadline,
                StorageOpts,
                NsId,
                TaskResult,
                Updates,
                Events
            ),
            _ = maybe_reply(TaskHeader, Response),
            ok = next_task(self()),
            State#prg_worker_state{process = undefined}
    end;
success_and_unlock(Intent, TaskHeader, Task, Deadline, State) ->
    #{events := Events} = Intent,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId, status := OldStatus} = Process,
        sidecar_pid = Pid
    } = State,
    Now = erlang:system_time(microsecond),
    {#{status := NewStatus} = ProcessUpdated, Updates} = update_process(Process, Intent),
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, lifecycle_event(TaskHeader, OldStatus, NewStatus), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    Response = response(Intent),
    TaskResult = task_result(Task, <<"finished">>, Response),
    SaveResult = prg_worker_sidecar:complete_and_unlock(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        Updates,
        Events
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, []} ->
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [#{status := <<"waiting">>, task_id := NextTaskId, scheduled_time := Ts} | _]} ->
            case (Ts - Now) div 1000 of
                Timeout when Timeout =< ?SCHEDULE_DEFENSE_INTERVAL_MS ->
                    process_scheduled_task(self(), ProcessId, NextTaskId),
                    State#prg_worker_state{process = undefined};
                Timeout when Timeout > ?SCHEDULE_DEFENSE_INTERVAL_MS ->
                    RunAfterMs = Timeout - ?SCHEDULE_DEFENSE_INTERVAL_MS,
                    ok = prg_scheduler:schedule_task(NsId, ProcessId, NextTaskId, RunAfterMs),
                    ok = next_task(self()),
                    State#prg_worker_state{process = undefined}
            end;
        {ok, [#{status := <<"running">>} = ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self()),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)},
                continuation = {create_header(ContinuationTask), ContinuationTask}
            }
    end.

error_and_stop({error, Reason} = Response, TaskHeader, Task, Deadline, State) ->
    {TaskType, _} = TaskHeader,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State,
    {_ProcessUpdated, Updates} =
        case TaskType of
            repair ->
                {Process, #{process_id => ProcessId}};
            _ ->
                Detail = prg_utils:format(Reason),
                ok = prg_worker_sidecar:lifecycle_sink(
                    Pid, Deadline, NsOpts, {error, Detail}, ProcessId
                ),
                update_process(Process, {error, {Detail, undefined}})
        end,
    TaskResult = task_result(Task, <<"error">>, Response),
    ok = prg_worker_sidecar:complete_and_error(
        Pid, Deadline, StorageOpts, NsId, TaskResult, Updates
    ),
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined}.

error_and_retry({error, Reason} = Response, TaskHeader, Task, Deadline, State) ->
    #{task_id := TaskId} = Task,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, retry_policy := RetryPolicy} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State,
    TaskResult = task_result(Task, <<"error">>, Response),
    _ =
        case check_retryable(TaskHeader, Task, RetryPolicy, Reason) of
            not_retryable ->
                Detail = prg_utils:format(Reason),
                {_ProcessUpdated, Updates} = update_process(Process, {error, {Detail, TaskId}}),
                ok = prg_worker_sidecar:lifecycle_sink(Pid, Deadline, NsOpts, {error, Detail}, ProcessId),
                ok = prg_worker_sidecar:complete_and_error(
                    Pid, Deadline, StorageOpts, NsId, TaskResult, Updates
                );
            NewTask ->
                Updates = #{process_id => ProcessId},
                %% prg_storage guarantees that when saving a task with the error status,
                %% all deferred tasks of all types will be completed with the canceled status,
                %% so calling complete_and_continue is guaranteed to return the retrieval task,
                %% and not any other deferred task
                {ok, [
                    #{
                        status := <<"waiting">>,
                        task_id := NextTaskId,
                        scheduled_time := Ts
                    }
                ]} = prg_worker_sidecar:complete_and_continue(
                    Pid,
                    Deadline,
                    StorageOpts,
                    NsId,
                    TaskResult,
                    Updates,
                    [],
                    NewTask
                ),
                Now = erlang:system_time(microsecond),
                %% The retry policy only supports a second time scale
                %% this ensures that the result of the expression (Ts - Now) div 1000
                %% will be greater or approximately equal 1000
                RunAfterMs = (Ts - Now) div 1000 - ?SCHEDULE_DEFENSE_INTERVAL_MS,
                ok = prg_scheduler:schedule_task(NsId, ProcessId, NextTaskId, RunAfterMs)
        end,
    ok = next_task(self()),
    State#prg_worker_state{process = undefined}.

update_process(#{status := <<"error">>, process_id := ProcessId} = Process, {error, _}) ->
    %% process error when already broken
    {Process, #{process_id => ProcessId}};
update_process(#{status := Status, process_id := ProcessId} = Process, {error, {Detail, Cause}}) when
    Status =:= <<"running">>;
    Status =:= <<"init">>
->
    %% process broken (transition from running/init to error)
    StatusChangedAt = erlang:system_time(microsecond),
    ProcessUpdates = #{
        process_id => ProcessId,
        status => <<"error">>,
        previous_status => Status,
        status_changed_at => StatusChangedAt,
        detail => Detail
    },
    case Cause of
        undefined ->
            {maps:merge(Process, ProcessUpdates), ProcessUpdates};
        TaskId ->
            Updates = ProcessUpdates#{corrupted_by => TaskId},
            {maps:merge(Process, Updates), Updates}
    end;
update_process(#{status := <<"error">>, process_id := ProcessId} = Process, Intent) ->
    %% process repaired (transition from error to running)
    StatusChangedAt = erlang:system_time(microsecond),
    NewProcess = maps:without(
        [detail, corrupted_by],
        Process#{status => <<"running">>, previous_status := <<"error">>, status_changed_at => StatusChangedAt}
    ),
    ProcessUpdates = #{
        process_id => ProcessId,
        status => <<"running">>,
        previous_status => <<"error">>,
        status_changed_at => StatusChangedAt,
        detail => undefined,
        corrupted_by => undefined
    },
    update_process_from_intent(NewProcess, ProcessUpdates, Intent);
update_process(#{status := <<"init">>, process_id := ProcessId} = Process, Intent) ->
    %% transition from init to running
    StatusChangedAt = erlang:system_time(microsecond),
    ProcessUpdates = #{
        process_id => ProcessId,
        status => <<"running">>,
        previous_status => <<"init">>,
        status_changed_at => StatusChangedAt
    },
    NewProcess = maps:merge(Process, ProcessUpdates),
    update_process_from_intent(NewProcess, ProcessUpdates, Intent);
update_process(#{previous_status := <<"init">>, status := <<"running">>, process_id := ProcessId} = Process, Intent) ->
    %% first transition from running to running, need to update previous_status
    ProcessUpdates = #{
        process_id => ProcessId,
        status => <<"running">>,
        previous_status => <<"running">>
    },
    NewProcess = maps:merge(Process, ProcessUpdates),
    update_process_from_intent(NewProcess, ProcessUpdates, Intent);
update_process(#{status := <<"running">>, process_id := ProcessId} = Process, Intent) ->
    %% normal work
    update_process_from_intent(Process, #{process_id => ProcessId}, Intent).

update_process_from_intent(Process, ProcessUpdates, Intent) ->
    maps:fold(
        fun
            (metadata, Meta, {#{metadata := OldMeta} = Proc, Updates}) when Meta =/= OldMeta ->
                {Proc#{metadata => Meta}, Updates#{metadata => Meta}};
            (aux_state, AuxState, {#{aux_state := OldAuxState} = Proc, Updates}) when AuxState =/= OldAuxState ->
                {Proc#{aux_state => AuxState}, Updates#{aux_state => AuxState}};
            (metadata, Meta, {Proc, Updates}) ->
                {Proc#{metadata => Meta}, Updates#{metadata => Meta}};
            (aux_state, AuxState, {Proc, Updates}) ->
                {Proc#{aux_state => AuxState}, Updates#{aux_state => AuxState}};
            (_K, _V, Acc) ->
                Acc
        end,
        {Process, ProcessUpdates},
        Intent
    ).

task_result(#{task_id := TaskId, running_time := RunningTime}, Status, Response) ->
    #{
        task_id => TaskId,
        response => term_to_binary(Response),
        running_time => RunningTime,
        finished_time => erlang:system_time(microsecond),
        status => Status
    }.

-spec maybe_reply(task_header(), term()) -> term().
maybe_reply({_, undefined}, _) ->
    undefined;
maybe_reply({_, {Receiver, Ref}}, Response) ->
    progressor:reply(Receiver, {Ref, Response}).

response(#{response := {error, _} = Error}) ->
    Error;
response(#{response := Data}) ->
    {ok, Data};
response(Intent) when not is_map_key(response, Intent) ->
    {ok, ok}.

extract_task_type({TaskType, _}) ->
    TaskType.

check_retryable(TaskHeader, #{last_retry_interval := LastInterval} = Task, RetryPolicy, Error) ->
    Now = erlang:system_time(microsecond),
    ProcessId = maps:get(process_id, Task),
    TimeoutSec =
        case LastInterval =:= 0 of
            true -> maps:get(initial_timeout, RetryPolicy);
            false -> trunc(LastInterval * maps:get(backoff_coefficient, RetryPolicy))
        end,
    Attempts = maps:get(attempts_count, Task) + 1,
    logger:info("check retryable ~p for error: ~p, last retry interval: ~p sec, attempt: ~p", [
        ProcessId, Error, LastInterval, Attempts
    ]),
    case is_retryable(Error, TaskHeader, RetryPolicy, TimeoutSec, Attempts) of
        true ->
            maps:with(
                [
                    process_id,
                    task_type,
                    status,
                    scheduled_time,
                    args,
                    last_retry_interval,
                    attempts_count,
                    metadata,
                    context
                ],
                Task#{
                    status => <<"waiting">>,
                    scheduled_time => Now + (TimeoutSec * 1000000),
                    last_retry_interval => TimeoutSec,
                    attempts_count => Attempts
                }
            );
        false ->
            not_retryable
    end.

%% machinegun legacy
-define(WOODY_ERROR(Class), {exception, _, {woody_error, {_, Class, _}}}).
-define(TEST_POLICY(Error, RetryPolicy, Timeout, Attempts),
    (Timeout < maps:get(max_timeout, RetryPolicy, infinity) andalso
        Attempts < maps:get(max_attempts, RetryPolicy, infinity) andalso
        not lists:any(fun(E) -> Error =:= E end, maps:get(non_retryable_errors, RetryPolicy, [])))
).

is_retryable(?WOODY_ERROR(result_unexpected), _TaskHeader, _RetryPolicy, _Timeout, _Attempts) ->
    false;
is_retryable(?WOODY_ERROR(resource_unavailable) = Error, {timeout, undefined}, RetryPolicy, Timeout, Attempts) ->
    ?TEST_POLICY(Error, RetryPolicy, Timeout, Attempts);
is_retryable(?WOODY_ERROR(result_unknown) = Error, {timeout, undefined}, RetryPolicy, Timeout, Attempts) ->
    ?TEST_POLICY(Error, RetryPolicy, Timeout, Attempts);
is_retryable({exception, _, _}, _TaskHeader, _RetryPolicy, _Timeout, _Attempts) ->
    false;
is_retryable(Error, {timeout, undefined}, RetryPolicy, Timeout, Attempts) ->
    ?TEST_POLICY(Error, RetryPolicy, Timeout, Attempts);
is_retryable(_Error, _TaskHeader, _RetryPolicy, _Timeout, _Attempts) ->
    false.

%% Sub-second schedules are coerced to immediate execution: if the gap to
%% `scheduled_time` is below ~1s (scheduler overhead), the task stays `running`
%% instead of `waiting`.
create_status(Timestamp, Now) when Timestamp =< Now ->
    <<"running">>;
create_status(Timestamp, Now) ->
    case (Timestamp - Now) >= ?EFFECTIVE_SCHEDULE_STEP_US of
        true ->
            <<"waiting">>;
        false ->
            <<"running">>
    end.

create_header(#{task_type := <<"timeout">>}) ->
    {timeout, undefined};
create_header(#{task_type := <<"call">>}) ->
    {call, undefined};
create_header(#{task_type := <<"remove">>}) ->
    {remove, undefined};
create_header(#{task_type := <<"init">>}) ->
    {init, undefined};
create_header(#{task_type := <<"repair">>}) ->
    {repair, undefined};
create_header(#{task_type := <<"notify">>}) ->
    {notify, undefined}.

last_event_id([]) ->
    0;
last_event_id(History) ->
    [#{event_id := Id} | _] = lists:reverse(History),
    Id.

lifecycle_event({timeout, _}, <<"error">>, <<"running">>) ->
    repair;
lifecycle_event({timeout, _}, _, _) ->
    timeout;
lifecycle_event({TaskType, _}, _, _) ->
    TaskType.
