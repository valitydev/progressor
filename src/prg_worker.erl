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
-export([continuation_task/3]).
-export([next_task/1]).

-record(prg_worker_state, {ns_id, ns_opts, process, sidecar_pid}).

-define(DEFAULT_RANGE, #{direction => forward}).

%%%
%%% API
%%%

-spec process_task(pid(), task_header(), task()) -> ok.
process_task(Worker, TaskHeader, #{process_id := _ProcessId, task_id := _TaskId} = Task) ->
    gen_server:cast(Worker, {process_task, TaskHeader, Task}).

-spec continuation_task(pid(), task_header(), task()) -> ok.
continuation_task(Worker, TaskHeader, Task) ->
    gen_server:cast(Worker, {continuation_task, TaskHeader, Task}).

-spec next_task(pid()) -> ok.
next_task(Worker) ->
    gen_server:cast(Worker, next_task).

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
    {process_task, TaskHeader, Task},
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, process_step_timeout := TimeoutSec} = _NsOpts,
        sidecar_pid = Pid
    } = State
) ->
    Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
    ProcessId = maps:get(process_id, Task),
    HistoryRange = maps:get(range, maps:get(metadata, Task, #{}), #{}),
    {ok, Process} = prg_worker_sidecar:get_process(Pid, Deadline, StorageOpts, NsId, ProcessId, HistoryRange),
    NewState = do_process_task(TaskHeader, Task, Deadline, State#prg_worker_state{process = Process}),
    {noreply, NewState};
handle_cast(
    {continuation_task, TaskHeader, Task},
    #prg_worker_state{
        ns_opts = #{process_step_timeout := TimeoutSec}
    } = State
) ->
    Deadline = erlang:system_time(millisecond) + TimeoutSec * 1000,
    NewState = do_process_task(TaskHeader, Task, Deadline, State),
    {noreply, NewState};
handle_cast(next_task, #prg_worker_state{sidecar_pid = CurrentPid}) ->
    %% kill sidecar and restart to clear memory
    true = erlang:unlink(CurrentPid),
    true = erlang:exit(CurrentPid, kill),
    exit(normal).

handle_info(_Info, #prg_worker_state{} = State) ->
    {noreply, State}.

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
    handle_result(Result, TaskHeader, Task, Deadline, State1).

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

%% success result with timer
handle_result(
    {ok, #{action := #{set_timer := Timestamp} = Action, events := Events} = Result},
    TaskHeader,
    #{task_id := TaskId, context := Context},
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State
) ->
    Now = erlang:system_time(second),
    ProcessUpdated = update_process(
        maps:without([detail, corrupted_by], Process#{status => <<"running">>}), Result
    ),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => Now,
        status => <<"finished">>
    },
    NewTask = #{
        process_id => ProcessId,
        task_type => action_to_task_type(Action),
        status => create_status(Timestamp, Now),
        scheduled_time => Timestamp,
        context => Context,
        last_retry_interval => 0,
        attempts_count => 0
    },
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, extract_task_type(TaskHeader), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    %% just for tests
    ok = maybe_wait_call(application:get_env(progressor, call_wait_timeout, undefined)),
    %%
    SaveResult = prg_worker_sidecar:complete_and_continue(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        ProcessUpdated,
        Events,
        NewTask
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, []} ->
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self(), create_header(ContinuationTask), ContinuationTask),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)}
            }
    end;
%% success result with undefined timer and remove action
handle_result(
    {ok, #{action := #{remove := true}} = Result},
    TaskHeader,
    _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = _Process,
        sidecar_pid = Pid
    } = State
) ->
    Response = response(maps:get(response, Result, undefined)),
    ok = prg_worker_sidecar:lifecycle_sink(Pid, Deadline, NsOpts, remove, ProcessId),
    ok = prg_worker_sidecar:remove_process(Pid, Deadline, StorageOpts, NsId, ProcessId),
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined};
%% success result with unset_timer action
handle_result(
    {ok, #{events := Events, action := unset_timer} = Result},
    TaskHeader,
    #{task_id := TaskId} = _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State
) ->
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, extract_task_type(TaskHeader), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    ProcessUpdated = update_process(
        maps:without([detail, corrupted_by], Process#{status => <<"running">>}), Result
    ),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"finished">>
    },
    SaveResult = prg_worker_sidecar:complete_and_suspend(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        ProcessUpdated,
        Events
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, []} ->
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self(), create_header(ContinuationTask), ContinuationTask),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)}
            }
    end;
%% success repair with corrupted task and undefined action
handle_result(
    {ok, #{events := Events} = Result},
    {repair, _} = TaskHeader,
    #{task_id := TaskId} = _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId, corrupted_by := ErrorTaskId} = Process,
        sidecar_pid = Pid
    } = State
) ->
    Now = erlang:system_time(second),
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, extract_task_type(TaskHeader), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    ProcessUpdated = update_process(
        maps:without([detail, corrupted_by], Process#{status => <<"running">>}), Result
    ),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"finished">>
    },
    {ok, ErrorTask} = prg_worker_sidecar:get_task(Pid, Deadline, StorageOpts, NsId, ErrorTaskId),
    case ErrorTask of
        #{task_type := Type} when Type =:= <<"timeout">>; Type =:= <<"remove">> ->
            %% machinegun legacy behaviour
            NewTask0 = maps:with(
                [process_id, task_type, scheduled_time, args, metadata, context], ErrorTask
            ),
            NewTask = NewTask0#{
                status => <<"running">>,
                running_time => Now,
                last_retry_interval => 0,
                attempts_count => 0
            },
            {ok, [ContinuationTask | _]} = prg_worker_sidecar:complete_and_continue(
                Pid,
                Deadline,
                StorageOpts,
                NsId,
                TaskResult,
                ProcessUpdated,
                Events,
                NewTask
            ),
            _ = maybe_reply(TaskHeader, Response),
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self(), create_header(ContinuationTask), ContinuationTask),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)}
            };
        _ ->
            {ok, []} = prg_worker_sidecar:complete_and_unlock(
                Pid,
                Deadline,
                StorageOpts,
                NsId,
                TaskResult,
                ProcessUpdated,
                Events
            ),
            _ = maybe_reply(TaskHeader, Response),
            ok = next_task(self()),
            State#prg_worker_state{process = undefined}
    end;
%% success result with undefined action
handle_result(
    {ok, #{events := Events} = Result},
    TaskHeader,
    #{task_id := TaskId} = _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State
) ->
    ok = prg_worker_sidecar:lifecycle_sink(
        Pid, Deadline, NsOpts, extract_task_type(TaskHeader), ProcessId
    ),
    ok = prg_worker_sidecar:event_sink(Pid, Deadline, NsOpts, ProcessId, Events),
    ProcessUpdated = update_process(
        maps:without([detail, corrupted_by], Process#{status => <<"running">>}), Result
    ),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"finished">>
    },
    SaveResult = prg_worker_sidecar:complete_and_unlock(
        Pid,
        Deadline,
        StorageOpts,
        NsId,
        TaskResult,
        ProcessUpdated,
        Events
    ),
    _ = maybe_reply(TaskHeader, Response),
    case SaveResult of
        {ok, []} ->
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        {ok, [ContinuationTask | _]} ->
            NewHistory = maps:get(history, Process) ++ Events,
            ok = continuation_task(self(), create_header(ContinuationTask), ContinuationTask),
            State#prg_worker_state{
                process = ProcessUpdated#{history => NewHistory, last_event_id => last_event_id(NewHistory)}
            }
    end;
%% calls processing error
handle_result(
    {error, Reason} = Response,
    {TaskType, _} = TaskHeader,
    #{task_id := TaskId} = _Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State
) when
    TaskType =:= init;
    TaskType =:= call;
    TaskType =:= notify;
    TaskType =:= repair
->
    ProcessUpdated =
        case TaskType of
            repair ->
                Process;
            _ ->
                Detail = prg_utils:format(Reason),
                ok = prg_worker_sidecar:lifecycle_sink(
                    Pid, Deadline, NsOpts, {error, Detail}, ProcessId
                ),
                Process#{status => <<"error">>, detail => Detail}
        end,
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"error">>
    },
    ok = prg_worker_sidecar:complete_and_error(
        Pid, Deadline, StorageOpts, NsId, TaskResult, ProcessUpdated
    ),
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined};
%% timeout/remove processing error
handle_result(
    {error, Reason} = Response,
    {TaskType, _} = TaskHeader,
    #{task_id := TaskId} = Task,
    Deadline,
    #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, retry_policy := RetryPolicy} = NsOpts,
        process = #{process_id := ProcessId} = Process,
        sidecar_pid = Pid
    } = State
) when TaskType =:= timeout; TaskType =:= remove ->
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"error">>
    },
    _ =
        case check_retryable(TaskHeader, Task, RetryPolicy, Reason) of
            not_retryable ->
                Detail = prg_utils:format(Reason),
                ProcessUpdated = Process#{
                    status => <<"error">>, detail => Detail, corrupted_by => TaskId
                },
                ok = prg_worker_sidecar:lifecycle_sink(Pid, Deadline, NsOpts, {error, Detail}, ProcessId),
                ok = prg_worker_sidecar:complete_and_error(
                    Pid, Deadline, StorageOpts, NsId, TaskResult, ProcessUpdated
                );
            NewTask ->
                {ok, _} = prg_worker_sidecar:complete_and_continue(
                    Pid,
                    Deadline,
                    StorageOpts,
                    NsId,
                    TaskResult,
                    Process,
                    [],
                    NewTask
                )
        end,
    ok = next_task(self()),
    State#prg_worker_state{process = undefined}.

update_process(Process, Result) ->
    maps:fold(
        fun
            (metadata, Meta, Acc) -> Acc#{metadata => Meta};
            (aux_state, AuxState, Acc) -> Acc#{aux_state => AuxState};
            (_, _, Acc) -> Acc
        end,
        Process,
        Result
    ).

-spec maybe_reply(task_header(), term()) -> term().
maybe_reply({_, undefined}, _) ->
    undefined;
maybe_reply({_, {Receiver, Ref}}, Response) ->
    progressor:reply(Receiver, {Ref, Response}).

response({error, _} = Error) ->
    Error;
response(undefined) ->
    {ok, ok};
response(Data) ->
    {ok, Data}.

extract_task_type({TaskType, _}) ->
    TaskType.

check_retryable(TaskHeader, #{last_retry_interval := LastInterval} = Task, RetryPolicy, Error) ->
    Now = erlang:system_time(second),
    Timeout =
        case LastInterval =:= 0 of
            true -> maps:get(initial_timeout, RetryPolicy);
            false -> trunc(LastInterval * maps:get(backoff_coefficient, RetryPolicy))
        end,
    Attempts = maps:get(attempts_count, Task) + 1,
    case is_retryable(Error, TaskHeader, RetryPolicy, Timeout, Attempts) of
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
                    metadata
                ],
                Task#{
                    status => <<"waiting">>,
                    scheduled_time => Now + Timeout,
                    last_retry_interval => Timeout,
                    attempts_count => Attempts
                }
            );
        false ->
            not_retryable
    end.

%% machinegun legacy
-define(WOODY_ERROR(Class), {exception, _, {woody_error, Class, _}}).
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

create_status(Timestamp, Now) when Timestamp =< Now ->
    <<"running">>;
create_status(_Timestamp, _Now) ->
    <<"waiting">>.

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
%%
action_to_task_type(#{remove := true}) ->
    <<"remove">>;
action_to_task_type(#{set_timer := _}) ->
    <<"timeout">>.

maybe_wait_call(undefined) ->
    ok;
maybe_wait_call(Timeout) ->
    timer:sleep(Timeout).

last_event_id([]) ->
    0;
last_event_id(History) ->
    [#{event_id := Id} | _] = lists:reverse(History),
    Id.
