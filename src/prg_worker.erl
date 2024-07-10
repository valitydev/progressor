-module(prg_worker).

-behaviour(gen_server).

-include("progressor.hrl").

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([handle_continue/2]).

-export([process_task/3]).

-define(SERVER, ?MODULE).

-record(prg_worker_state, {ns_id, ns_opts, num, process, processor_pid}).

%%%
%%% API
%%%

-spec process_task(pid(), task_header(), task()) -> ok.
process_task(Worker, TaskHeader, Task) ->
    gen_server:cast(Worker, {process_task, TaskHeader, Task}).

-spec continuation_task(pid(), task()) -> ok.
continuation_task(Worker, Task) ->
    gen_server:cast(Worker, {continuation_task, Task}).

-spec next_task(pid()) -> ok.
next_task(Worker) ->
    gen_server:cast(Worker, next_task).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(NsId, NsOpts, Num) ->
    gen_server:start_link(?MODULE, [NsId, NsOpts, Num], []).

init([NsId, NsOpts, Num]) ->
    {ok, #prg_worker_state{
        ns_id = NsId,
        ns_opts = NsOpts,
        num = Num
    }, {continue, do_start}}.

handle_continue(do_start, State = #prg_worker_state{ns_id = NsId}) ->
    {ok, Pid} = prg_processor:start_link(),
    case prg_scheduler:pop_task(NsId, self()) of
        {TaskHeader, Task} ->
            ok = process_task(self(), TaskHeader, Task);
        not_found ->
            skip
    end,
    {noreply, State#prg_worker_state{processor_pid = Pid}}.

handle_call(_Request, _From, State = #prg_worker_state{}) ->
    {reply, ok, State}.

handle_cast({process_task, TaskHeader, Task}, State = #prg_worker_state{ns_id = NsId, ns_opts = NsOpts}) ->
    StorageOpts = maps:get(storage, NsOpts),
    {ok, Process} = prg_storage:get_process(StorageOpts, NsId, maps:get(process_id, Task)),
    NewState = do_process_task(TaskHeader, Task, State#prg_worker_state{process = Process}),
    {noreply, NewState};
handle_cast({continuation_task, Task}, State = #prg_worker_state{}) ->
    NewState = do_process_task({timeout, undefined}, Task, State),
    {noreply, NewState};
handle_cast(next_task, State = #prg_worker_state{ns_id = NsId, ns_opts = NsOpts}) ->
    NewState =
        case prg_scheduler:pop_task(NsId, self()) of
            {TaskHeader, Task} ->
                StorageOpts = maps:get(storage, NsOpts),
                {ok, Process} = prg_storage:get_process(StorageOpts, NsId, maps:get(process_id, Task)),
                do_process_task(TaskHeader, Task, State#prg_worker_state{process = Process});
            not_found ->
                State
        end,
    {noreply, NewState};
handle_cast(_Request, State = #prg_worker_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #prg_worker_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_worker_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_worker_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_process_task(TaskHeader, Task,
    State = #prg_worker_state{
        ns_opts = #{processor := Processor, process_step_timeout := TimeoutSec},
        process = Process,
        processor_pid = Pid
    }
) ->
    Args = maps:get(args, Task, <<>>),
    handle_result(
        prg_processor:process(Pid, Processor, {extract_task_type(TaskHeader), Args, Process}, TimeoutSec * 1000),
        TaskHeader,
        Task,
        State
    ).

%% success result with continuation
handle_result(
    {ok, #{action := #{set_timer := Timestamp}, events := Events} = Result},
    TaskHeader,
    #{task_id := TaskId} = Task,
    State = #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = _NsOpts,
        process = #{process_id := ProcessId} = Process
    }
) ->
    Now = erlang:system_time(second),
    ProcessUpdated = update_process(Process#{status => <<"running">>}, Result),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => Now,
        status => <<"finished">>
    },
    NewTask = maps:merge(
        #{
            process_id => ProcessId,
            task_type => <<"timeout">>,
            scheduled_time => Timestamp,
            last_retry_interval => 0,
            attempts_count => 0
        },
        maps:with([metadata], Task)
    ),
    case operation(Timestamp, Now) of
        postpone ->
            %% save waiting task and go to next task
            {ok, _} = prg_storage:complete_and_continue(StorageOpts, NsId, TaskResult, ProcessUpdated, Events,
                NewTask#{status => <<"waiting">>}),
            _ = maybe_reply(TaskHeader, Response),
            ok = next_task(self()),
            State#prg_worker_state{process = undefined};
        continuation ->
            %% save running task and try continuation
            NextTask0 = NewTask#{status => <<"running">>, running_time => Timestamp},
            {ok, NextTaskId} = prg_storage:complete_and_continue(StorageOpts, NsId, TaskResult, ProcessUpdated, Events,
                NextTask0),
            NextTask = NextTask0#{task_id => NextTaskId},
            _ = maybe_reply(TaskHeader, Response),
            case prg_scheduler:continuation_task(NsId, self(), NextTask) of
                ok ->
                    NewHistory = maps:get(history, Process) ++ Events,
                    ok = continuation_task(self(), NextTask),
                    State#prg_worker_state{process = ProcessUpdated#{history => NewHistory}};
                {OtherTaskHeader, OtherTask} ->
                    ok = process_task(self(), OtherTaskHeader, OtherTask),
                    State#prg_worker_state{process = undefined}
            end
    end;

%% success result with unset_timer or undefined action
handle_result(
    {ok, #{events := Events} = Result},
    TaskHeader,
    #{task_id := TaskId} = _Task,
    State = #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = _NsOpts,
        process = Process
    }
) ->
    ProcessUpdated = update_process(Process#{status => <<"running">>}, Result),
    Response = response(maps:get(response, Result, undefined)),
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"finished">>
    },
    case maps:get(action, Result, undefined) of
        unset_timer ->
            ok = prg_storage:complete_and_suspend(StorageOpts, NsId, TaskResult, ProcessUpdated, Events);
        undefined ->
            ok = prg_storage:complete_and_unlock(StorageOpts, NsId, TaskResult, ProcessUpdated, Events)
    end,
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined};

%% calls processing error
handle_result(
    {error, Reason} = Response,
    {TaskType, _} = TaskHeader,
    #{task_id := TaskId} = _Task,
    State = #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts} = _NsOpts,
        process = Process
    }
) when
    TaskType =:= init;
    TaskType =:= call;
    TaskType =:= notify;
    TaskType =:= repair
->
    ProcessUpdated = Process#{status => <<"error">>, detail => prg_utils:format(Reason)},
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"error">>
    },
    ok = prg_storage:complete_and_suspend(StorageOpts, NsId, TaskResult, ProcessUpdated, []),
    _ = maybe_reply(TaskHeader, Response),
    ok = next_task(self()),
    State#prg_worker_state{process = undefined};

%% timeout processing error
handle_result(
    {error, Reason} = Response,
    {TaskType, _} = TaskHeader,
    #{task_id := TaskId} = Task,
    State = #prg_worker_state{
        ns_id = NsId,
        ns_opts = #{storage := StorageOpts, retry_policy := RetryPolicy},
        process = Process
    }
) when TaskType =:= timeout ->
    TaskResult = #{
        task_id => TaskId,
        response => term_to_binary(Response),
        finished_time => erlang:system_time(second),
        status => <<"error">>
    },
    case check_retryable(TaskHeader, Task, RetryPolicy, Reason) of
        not_retryable ->
            ProcessUpdated = Process#{status => <<"error">>, detail => prg_utils:format(Reason)},
            ok = prg_storage:complete_and_suspend(StorageOpts, NsId, TaskResult, ProcessUpdated, []);
        NewTask ->
            {ok, _NextTaskId} = prg_storage:complete_and_continue(StorageOpts, NsId, TaskResult, Process, [], NewTask)
    end,
    ok = next_task(self()),
    State#prg_worker_state{process = undefined}.


update_process(Process, Result) ->
    case Result of
        #{metadata := Meta, aux_state := AuxState} ->
            Process#{metadata => Meta, aux_state => AuxState};
        #{metadata := Meta} ->
            Process#{metadata => Meta};
        #{aux_state := AuxState} ->
            Process#{aux_state => AuxState};
        _ ->
            Process
    end.

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
                [process_id, task_type, status, scheduled_time, args, last_retry_interval, attempts_count, metadata],
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

is_retryable({exception, _, _}, _TaskHeader, _RetryPolicy, _Timeout, _Attempts) ->
    false;
is_retryable(Error, {timeout, undefined}, RetryPolicy, Timeout, Attempts) ->
    Timeout < maps:get(max_timeout, RetryPolicy, infinity) andalso
        Attempts < maps:get(max_attempts, RetryPolicy, infinity) andalso
        not lists:any(fun(E) -> Error =:= E end, maps:get(non_retryable_errors, RetryPolicy, []));
is_retryable(_Error, _TaskHeader, _RetryPolicy, _Timeout, _Attempts) ->
    false.

operation(Timestamp, Now) when Timestamp =< Now ->
    continuation;
operation(_Timestamp, _Now) ->
    postpone.

