-module(prg_worker_sidecar).

-behaviour(gen_server).

-include("progressor.hrl").
-include("otel.hrl").

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Processor functions wrapper
-export([process/5]).
%% Storage functions wrapper
-export([complete_and_continue/8]).
-export([complete_and_suspend/7]).
-export([complete_and_unlock/7]).
-export([complete_and_error/6]).
-export([remove_process/5]).
%% Notifier functions wrapper
-export([event_sink/5]).
-export([lifecycle_sink/5]).
%%
-export([get_process/5]).
-export([get_process/6]).
-export([get_task/5]).

-type context() :: binary().
-type args() :: term().
-type request() :: {task_t(), args(), process()}.

-record(prg_sidecar_state, {}).

-define(DEFAULT_DELAY, 3000).
-define(PROCESSING_KEY, progressor_task_processing_duration_ms).
-define(COMPLETION_KEY, progressor_task_completion_duration_ms).
-define(REMOVING_KEY, progressor_process_removing_duration_ms).
-define(NOTIFICATION_KEY, progressor_notification_duration_ms).

-dialyzer({nowarn_function, do_with_retry/2}).
%% API

%% processor wrapper
-spec process(pid(), timestamp_ms(), namespace_opts(), request(), context()) ->
    {ok, _Result} | {error, _Reason} | no_return().
process(Pid, Deadline, #{namespace := NS} = NsOpts, {TaskType, _, _} = Request, Context) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(Pid, {process, NsOpts, Request, Context, ?current_otel_ctx}, Timeout)
    end,
    prg_utils:with_observe(Fun, ?PROCESSING_KEY, [NS, erlang:atom_to_list(TaskType)]).

%% storage wrappers
-spec complete_and_continue(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process_updates(),
    [event()],
    task()
) -> {ok, [task()]} | no_return().
complete_and_continue(Pid, _Deadline, StorageOpts, NsId, TaskResult, ProcessUpdates, Events, Task) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_continue, StorageOpts, NsId, TaskResult, ProcessUpdates, Events, Task, ?current_otel_ctx},
            infinity
        )
    end,
    prg_utils:with_observe(Fun, ?COMPLETION_KEY, [
        erlang:atom_to_list(NsId), "complete_and_continue"
    ]).

-spec complete_and_suspend(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process_updates(),
    [event()]
) -> {ok, [task()]} | no_return().
complete_and_suspend(Pid, _Deadline, StorageOpts, NsId, TaskResult, ProcessUpdates, Events) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_suspend, StorageOpts, NsId, TaskResult, ProcessUpdates, Events, ?current_otel_ctx},
            infinity
        )
    end,
    prg_utils:with_observe(Fun, ?COMPLETION_KEY, [erlang:atom_to_list(NsId), "complete_and_suspend"]).

-spec complete_and_unlock(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process_updates(),
    [event()]
) -> {ok, [task()]} | no_return().
complete_and_unlock(Pid, _Deadline, StorageOpts, NsId, TaskResult, ProcessUpdates, Events) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_unlock, StorageOpts, NsId, TaskResult, ProcessUpdates, Events, ?current_otel_ctx},
            infinity
        )
    end,
    prg_utils:with_observe(Fun, ?COMPLETION_KEY, [erlang:atom_to_list(NsId), "complete_and_unlock"]).

-spec complete_and_error(
    pid(), timestamp_ms(), storage_opts(), namespace_id(), task_result(), process_updates()
) ->
    ok | no_return().
complete_and_error(Pid, _Deadline, StorageOpts, NsId, TaskResult, ProcessUpdates) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_error, StorageOpts, NsId, TaskResult, ProcessUpdates, ?current_otel_ctx},
            infinity
        )
    end,
    prg_utils:with_observe(Fun, ?COMPLETION_KEY, [erlang:atom_to_list(NsId), "complete_and_error"]).

-spec remove_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id()) ->
    ok | no_return().
remove_process(Pid, _Deadline, StorageOpts, NsId, ProcessId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(Pid, {remove_process, StorageOpts, NsId, ProcessId, ?current_otel_ctx}, infinity)
    end,
    prg_utils:with_observe(Fun, ?REMOVING_KEY, [erlang:atom_to_list(NsId)]).

%% notifier wrappers

-spec event_sink(pid(), timestamp_ms(), namespace_opts(), id(), [event()]) -> ok | no_return().
event_sink(Pid, Deadline, #{namespace := NS} = NsOpts, ProcessId, Events) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(Pid, {event_sink, NsOpts, ProcessId, Events, ?current_otel_ctx}, Timeout)
    end,
    prg_utils:with_observe(Fun, ?NOTIFICATION_KEY, [NS, "event_sink"]).

-spec lifecycle_sink(pid(), timestamp_ms(), namespace_opts(), task_t() | {error, _Reason}, id()) ->
    ok | no_return().
lifecycle_sink(Pid, Deadline, #{namespace := NS} = NsOpts, TaskType, ProcessId) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(Pid, {lifecycle_sink, NsOpts, TaskType, ProcessId, ?current_otel_ctx}, Timeout)
    end,
    prg_utils:with_observe(Fun, ?NOTIFICATION_KEY, [NS, "lifecycle_sink"]).
%%

-spec get_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id()) ->
    {ok, process()} | {error, _Reason}.
get_process(Pid, _Deadline, StorageOpts, NsId, ProcessId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {get_process, StorageOpts, NsId, ProcessId, #{}, ?current_otel_ctx}, infinity).

-spec get_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id(), history_range()) ->
    {ok, process()} | {error, _Reason}.
get_process(Pid, _Deadline, StorageOpts, NsId, ProcessId, HistoryRange) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {get_process, StorageOpts, NsId, ProcessId, HistoryRange, ?current_otel_ctx}, infinity).

-spec get_task(pid(), timestamp_ms(), storage_opts(), namespace_id(), task_id()) ->
    {ok, task()} | {error, _Reason}.
get_task(Pid, _Deadline, StorageOpts, NsId, TaskId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {get_task, StorageOpts, NsId, TaskId, ?current_otel_ctx}, infinity).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #prg_sidecar_state{}}.

handle_call(
    {
        process,
        #{processor := #{client := Handler, options := Options}, namespace := _NsName} = _NsOpts,
        Request,
        Ctx,
        OtelCtx
    },
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"process">>, fun() ->
        Response =
            try Handler:process(Request, Options, Ctx) of
                {ok, _Result} = OK ->
                    OK;
                {error, Reason} = ERR ->
                    logger:error("processor error: ~p", [Reason]),
                    ERR;
                Unsupported ->
                    logger:error("processor unexpected result: ~p", [Unsupported]),
                    _ = ?span_exception(error, badmatch, <<"unsupported_result">>, []),
                    {error, <<"unsupported_result">>}
            catch
                Class:Term:Trace ->
                    logger:error("processor exception: ~p", [[Class, Term, Trace]]),
                    _ = ?span_exception(Class, Term, Trace),
                    {error, {exception, Class, Term}}
            end,
        {reply, Response, State}
    end);
handle_call(
    {complete_and_continue, StorageOpts, NsId, TaskResult, Process, Events, Task, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"complete and continue">>, fun() ->
        Fun = fun() ->
            prg_storage:complete_and_continue(StorageOpts, NsId, TaskResult, Process, Events, Task)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {remove_process, StorageOpts, NsId, ProcessId, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"remove_process">>, fun() ->
        Fun = fun() ->
            prg_storage:remove_process(StorageOpts, NsId, ProcessId)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {get_process, StorageOpts, NsId, ProcessId, HistoryRange, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"get process">>, fun() ->
        Fun = fun() ->
            prg_storage:get_process(internal, StorageOpts, NsId, ProcessId, HistoryRange)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {get_task, StorageOpts, NsId, TaskId, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"get task">>, fun() ->
        Fun = fun() ->
            prg_storage:get_task(StorageOpts, NsId, TaskId)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {complete_and_suspend, StorageOpts, NsId, TaskResult, Process, Events, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"complete and suspend">>, fun() ->
        Fun = fun() ->
            prg_storage:complete_and_suspend(StorageOpts, NsId, TaskResult, Process, Events)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {complete_and_unlock, StorageOpts, NsId, TaskResult, Process, Events, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"complete and unlock">>, fun() ->
        Fun = fun() ->
            prg_storage:complete_and_unlock(StorageOpts, NsId, TaskResult, Process, Events)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call(
    {complete_and_error, StorageOpts, NsId, TaskResult, Process, OtelCtx},
    _From,
    #prg_sidecar_state{} = State
) ->
    ?with_span(OtelCtx, <<"complete and error">>, fun() ->
        Fun = fun() ->
            prg_storage:complete_and_error(StorageOpts, NsId, TaskResult, Process)
        end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call({event_sink, NsOpts, ProcessId, Events, OtelCtx}, _From, State) ->
    ?with_span(OtelCtx, <<"event sink">>, fun() ->
        Fun = fun() -> prg_notifier:event_sink(NsOpts, ProcessId, Events) end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end);
handle_call({lifecycle_sink, NsOpts, TaskType, ProcessId, OtelCtx}, _From, State) ->
    ?with_span(OtelCtx, <<"lifecycle sink">>, fun() ->
        Fun = fun() -> prg_notifier:lifecycle_sink(NsOpts, TaskType, ProcessId) end,
        Response = do_with_retry(Fun, ?DEFAULT_DELAY),
        {reply, Response, State}
    end).

handle_cast(_Request, #prg_sidecar_state{} = State) ->
    {noreply, State}.

handle_info(_Info, #prg_sidecar_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #prg_sidecar_state{} = _State) ->
    ok.

code_change(_OldVsn, #prg_sidecar_state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_with_retry(Fun, Delay) ->
    _ = ?span_event(<<"try">>),
    try Fun() of
        ok = Result ->
            Result;
        {ok, _} = Result ->
            Result;
        Error ->
            _ = logger:error("result processing error: ~p", [Error]),
            _ = ?span_event(<<"retryable error">>),
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    catch
        Class:Error:Trace ->
            _ = logger:error("result processing exception: ~p", [[Class, Error, Trace]]),
            _ = ?span_exception(Class, Error, Trace),
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    end.
