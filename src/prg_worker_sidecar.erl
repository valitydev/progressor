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
%%
-export([get_process/5]).
-export([get_task/5]).

-type context() :: binary().
-type args() :: binary().
-type request() :: {task_t(), args(), process()}.

-record(prg_processor_state, {}).

-define(DEFAULT_DELAY, 3000).

-dialyzer({nowarn_function, do_with_retry/2}).
%% API

%% processor wrapper
-spec process(pid(), non_neg_integer(), namespace_opts(), request(), context()) ->
    {ok, _Result} | {error, _Reason} | no_return().
process(Pid, Deadline, #{namespace := NS} = NsOpts, {TaskType, _, #{process_id := ProcessId}} = Request, Context) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {process, NsOpts, Request, Context}, Timeout) end,
    do_with_log(Fun, "processor result: ~p", [NS, ProcessId, TaskType]).

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
) -> {ok, [task()]} | no_return().
complete_and_continue(Pid, _Deadline, StorageOpts, NsId, TaskResult, #{process_id := ProcId} = Process, Events, Task) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_continue, StorageOpts, NsId, TaskResult, Process, Events, Task}, infinity
        )
    end,
    do_with_log(Fun, "complete_and_continue result: ~p", [NsId, ProcId]).

-spec complete_and_suspend(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process(),
    [event()]
) -> {ok, [task()]} | no_return().
complete_and_suspend(Pid, _Deadline, StorageOpts, NsId, TaskResult, #{process_id := ProcId} = Process, Events) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_suspend, StorageOpts, NsId, TaskResult, Process, Events}, infinity
        )
    end,
    do_with_log(Fun, "complete_and_suspend result: ~p", [NsId, ProcId]).

-spec complete_and_unlock(
    pid(),
    timestamp_ms(),
    storage_opts(),
    namespace_id(),
    task_result(),
    process(),
    [event()]
) -> {ok, [task()]} | no_return().
complete_and_unlock(Pid, _Deadline, StorageOpts, NsId, TaskResult, #{process_id := ProcId} = Process, Events) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_unlock, StorageOpts, NsId, TaskResult, Process, Events}, infinity
        )
    end,
    do_with_log(Fun, "complete_and_unlock result: ~p", [NsId, ProcId]).

-spec complete_and_error(pid(), timestamp_ms(), storage_opts(), namespace_id(), task_result(), process()) ->
    ok | no_return().
complete_and_error(Pid, _Deadline, StorageOpts, NsId, TaskResult, #{process_id := ProcId} = Process) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() ->
        gen_server:call(
            Pid,
            {complete_and_error, StorageOpts, NsId, TaskResult, Process}, infinity
        )
    end,
    do_with_log(Fun, "complete_and_error result: ~p", [NsId, ProcId]).

-spec remove_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id()) ->
    ok | no_return().
remove_process(Pid, _Deadline, StorageOpts, NsId, ProcessId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {remove_process, StorageOpts, NsId, ProcessId}, infinity) end,
    do_with_log(Fun, "complete_and_error result: ~p", [NsId, ProcessId]).

%% notifier wrappers

-spec event_sink(pid(), timestamp_ms(), namespace_opts(), id(), [event()]) -> ok | no_return().
event_sink(Pid, Deadline, NsOpts, ProcessId, Events) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {event_sink, NsOpts, ProcessId, Events}, Timeout) end,
    do_with_log(Fun, "event_sink result: ~p").

-spec lifecycle_sink(pid(), timestamp_ms(), namespace_opts(), task_t() | {error, _Reason}, id()) ->
    ok | no_return().
lifecycle_sink(Pid, Deadline, NsOpts, TaskType, ProcessId) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {lifecycle_sink, NsOpts, TaskType, ProcessId}, Timeout) end,
    do_with_log(Fun, "lifecycle_sink result: ~p").
%%

-spec get_process(pid(), timestamp_ms(), storage_opts(), namespace_id(), id()) ->
    {ok, process()} | {error, _Reason}.
get_process(Pid, _Deadline, StorageOpts, NsId, ProcessId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {get_process, StorageOpts, NsId, ProcessId}, infinity) end,
    do_with_log(Fun, "get_process result: ~p", [NsId, ProcessId]).

-spec get_task(pid(), timestamp_ms(), storage_opts(), namespace_id(), task_id()) ->
    {ok, process()} | {error, _Reason}.
get_task(Pid, _Deadline, StorageOpts, NsId, TaskId) ->
    %% Timeout = Deadline - erlang:system_time(millisecond),
    Fun = fun() -> gen_server:call(Pid, {get_task, StorageOpts, NsId, TaskId}, infinity) end,
    do_with_log(Fun, "get_task result: ~p", [NsId]).


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
            Unsupported ->
                logger:error("processor unexpected result: ~p", [Unsupported]),
                {error, <<"unsupported_result">>}
        catch
            Class:Term:Trace ->
                logger:error("processor exception: ~p", [[Class, Term, Trace]]),
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
    {get_process, StorageOpts, NsId, ProcessId},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:get_process(StorageOpts, NsId, ProcessId)
    end,
    Response = do_with_retry(Fun, ?DEFAULT_DELAY),
    {reply, Response, State};

handle_call(
    {get_task, StorageOpts, NsId, TaskId},
    _From,
    State = #prg_processor_state{}
) ->
    Fun = fun() ->
        prg_storage:get_task(StorageOpts, NsId, TaskId)
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
        Error ->
            _ = logger:error("result processing error: ~p", [Error]),
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    catch
        Class:Error:Trace ->
            _ = logger:error("result processing exception: ~p", [[Class, Error, Trace]]),
            timer:sleep(Delay),
            do_with_retry(Fun, Delay)
    end.
%%

do_with_log(Fun, Format) ->
    do_with_log(Fun, Format, []).

do_with_log(Fun, Format, Params) ->
    Result = Fun(),
    logger:debug(Format, [Params ++ [Result]]),
    Result.
