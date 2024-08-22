-module(prg_worker_sidecar).

-behaviour(gen_server).

-include("progressor.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

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

-type context() :: binary().
-type args() :: binary().
-type request() :: {task_t(), args(), process()}.

-record(prg_processor_state, {}).

-define(DEFAULT_DELAY, 3000).

%% API

%% processor wrapper
-spec process(pid(), non_neg_integer(), namespace_opts(), request(), context()) ->
    {ok, _Result} | {error, _Reason} | no_return().
process(Pid, Deadline, NsOpts, Request, Context) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {process, NsOpts, Request, Context}, Timeout).

%% storage wrappers
-spec complete_and_continue(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process(),
    [event()],
    task()
) -> {ok, task_id()}.
complete_and_continue(Pid, Deadline, StorageOpts, NsId, TaskResult, Process, Events, Task) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {complete_and_continue, StorageOpts, NsId, TaskResult, Process, Events, Task}, Timeout).

-spec remove_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id()) ->
    ok | no_return().
remove_process(Pid, Deadline, StorageOpts, NsId, ProcessId) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {remove_process, StorageOpts, NsId, ProcessId}, Timeout).

-spec complete_and_suspend(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process(),
    [event()]
) -> ok | no_return().
complete_and_suspend(Pid, Deadline, StorageOpts, NsId, TaskResult, Process, Events) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {complete_and_suspend, StorageOpts, NsId, TaskResult, Process, Events}, Timeout).

-spec complete_and_unlock(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process(),
    [event()]
) -> ok | no_return().
complete_and_unlock(Pid, Deadline, StorageOpts, NsId, TaskResult, Process, Events) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {complete_and_unlock, StorageOpts, NsId, TaskResult, Process, Events}, Timeout).

-spec complete_and_error(pid(), timestamp_ms(), storage_opts(), namespace_id(), task_result(), process()) ->
    ok | no_return().
complete_and_error(Pid, Deadline, StorageOpts, NsId, TaskResult, Process) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {complete_and_error, StorageOpts, NsId, TaskResult, Process}, Timeout).

%% notifier wrappers

-spec event_sink(pid(), timestamp_ms(), namespace_opts(), id(), [event()]) -> ok | no_return().
event_sink(Pid, Deadline, NsOpts, ProcessId, Events) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {event_sink, NsOpts, ProcessId, Events}, Timeout).

-spec lifecycle_sink(pid(), timestamp_ms(), namespace_opts(), task_t() | {error, _Reason}, id()) ->
    ok | no_return().
lifecycle_sink(Pid, Deadline, NsOpts, TaskType, ProcessId) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    gen_server:call(Pid, {lifecycle_sink, NsOpts, TaskType, ProcessId}, Timeout).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #prg_processor_state{}}.

handle_call(
    {
        process,
        #{processor := #{client := Handler, options := Options}, namespace := _NsName} = _NsOpts,
        Request,
        Ctx
    },
    _From,
    State = #prg_processor_state{}
) ->
    Response =
        try Handler:process(Request, Options, Ctx) of
            {ok, _Result} = OK -> OK;
            {error, _Reason} = ERR -> ERR;
            _Unsupported -> {error, <<"unsupported_result">>}
        catch
            Class:Term ->
                {error, {exception, Class, Term}}
        end,
    {reply, Response, State};

handle_call(
    {complete_and_continue, StorageOpts, NsId, TaskResult, Process, Events, Task},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:complete_and_continue(StorageOpts, NsId, TaskResult, Process, Events, Task)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call(
    {remove_process, StorageOpts, NsId, ProcessId},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:remove_process(StorageOpts, NsId, ProcessId)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call(
    {complete_and_suspend, StorageOpts, NsId, TaskResult, Process, Events},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:complete_and_suspend(StorageOpts, NsId, TaskResult, Process, Events)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call(
    {complete_and_unlock, StorageOpts, NsId, TaskResult, Process, Events},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:complete_and_unlock(StorageOpts, NsId, TaskResult, Process, Events)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call(
    {complete_and_error, StorageOpts, NsId, TaskResult, Process},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:complete_and_error(StorageOpts, NsId, TaskResult, Process)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call({event_sink, NsOpts, ProcessId, Events}, _From, State) ->
    Fun = fun() -> prg_notifier:event_sink(NsOpts, ProcessId, Events) end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call({lifecycle_sink, NsOpts, TaskType, ProcessId}, _From, State) ->
    Fun = fun() -> prg_notifier:lifecycle_sink(NsOpts, TaskType, ProcessId) end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State}.

handle_cast(_Request, State = #prg_processor_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #prg_processor_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_processor_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_processor_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_with_retry(Fun, Delay) ->
    try Fun() of
        ok = Result ->
            Result;
        {ok, _} = Result ->
            Result;
        _Error ->
            io:format(user, "ERROR: ~p~n", [_Error]),
            %% TODO log
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    catch
        _Class:_Error ->
            io:format(user, "EXCEPTION: ~p~n", [[_Class, _Error]]),
            %% TODO log
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    end.