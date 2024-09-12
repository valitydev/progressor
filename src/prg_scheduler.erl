-module(prg_scheduler).

-include("progressor.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% API
-export([push_task/3]).
-export([pop_task/2]).
-export([continuation_task/3]).

-record(prg_scheduler_state, {ns_id, ns_opts, ready, free_workers, rescan_timeout}).

%%%
%%% API
%%%

-spec push_task(namespace_id(), task_header(), task()) -> ok.
push_task(NsId, TaskHeader, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:cast(RegName, {push_task, TaskHeader, Task}).

-spec pop_task(namespace_id(), pid()) -> {task_header(), task()} | not_found.
pop_task(NsId, Worker) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {pop_task, Worker}).

-spec continuation_task(namespace_id(), pid(), task()) -> {task_header(), task()} | ok.
continuation_task(NsId, Worker, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {continuation_task, Worker, Task}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link({NsId, _NsOpts} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:start_link({local, RegName}, ?MODULE, NS, []).

init({NsId, #{task_scan_timeout := RescanTimeoutSec} = Opts}) ->
    _ = start_workers(NsId, Opts),
    RescanTimeoutMs = RescanTimeoutSec  * 1000,
    State = #prg_scheduler_state{
        ns_id = NsId,
        ns_opts = Opts,
        ready = queue:new(),
        free_workers = queue:new(),
        rescan_timeout = RescanTimeoutMs
    },
    _ = start_rescan_timers(RescanTimeoutMs),
    _ = start_rescan_calls((RescanTimeoutMs div 3) + 1),
    {ok, State}.

handle_call({pop_task, Worker}, _From, State) ->
    case queue:out(State#prg_scheduler_state.ready) of
        {{value, TaskData}, NewReady} ->
            {reply, TaskData, State#prg_scheduler_state{ready = NewReady}};
        {empty, _} ->
            Workers = State#prg_scheduler_state.free_workers,
            {reply, not_found, State#prg_scheduler_state{free_workers = queue:in(Worker, Workers)}}
    end;
handle_call({continuation_task, Task}, _From, State) ->
    case queue:out(State#prg_scheduler_state.ready) of
        {{value, TaskData}, NewReady} ->
            {reply, TaskData, State#prg_scheduler_state{
                ready = queue:in({header(), Task}, NewReady)
            }};
        {empty, _} ->
            {reply, ok, State}
    end;
handle_call(_Request, _From, State = #prg_scheduler_state{}) ->
    {reply, ok, State}.

handle_cast({push_task, TaskHeader, Task}, State) ->
    NewState = do_push_task(TaskHeader, Task, State),
    {noreply, NewState};
handle_cast(_Request, State = #prg_scheduler_state{}) ->
    {noreply, State}.

handle_info(
    {timeout, _TimerRef, rescan_timers},
    State = #prg_scheduler_state{ns_id = NsId, ns_opts = NsOpts, free_workers = Workers, rescan_timeout = RescanTimeout}
) ->
    NewState =
        case queue:len(Workers) of
            0 ->
                %% all workers is busy
                State;
            N ->
                Calls = search_calls(N, NsId, NsOpts),
                Timers = search_timers(N - erlang:length(Calls), NsId, NsOpts),
                Tasks = Calls ++ Timers,
                lists:foldl(fun(#{task_type := Type} = Task, Acc) ->
                    do_push_task(header(Type), Task, Acc)
                end, State, Tasks)
        end,
    _ = start_rescan_timers(RescanTimeout),
    {noreply, NewState};
%%
handle_info(
    {timeout, _TimerRef, rescan_calls},
    State = #prg_scheduler_state{ns_id = NsId, ns_opts = NsOpts, free_workers = Workers, rescan_timeout = RescanTimeout}
) ->
    NewState =
        case queue:len(Workers) of
            0 ->
                %% all workers is busy
                State;
            N ->
                Calls = search_calls(N, NsId, NsOpts),
                lists:foldl(fun(#{task_type := Type} = Task, Acc) ->
                    do_push_task(header(Type), Task, Acc)
                end, State, Calls)
        end,
    _ = start_rescan_calls((RescanTimeout div 3) + 1),
    {noreply, NewState};

handle_info(_Info, State = #prg_scheduler_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_scheduler_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_scheduler_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_workers(NsId, NsOpts) ->
    WorkerPoolSize = maps:get(worker_pool_size, NsOpts, ?DEFAULT_WORKER_POOL_SIZE),
    WorkerSup = prg_utils:registered_name(NsId, "_worker_sup"),
    lists:foreach(fun(N) ->
        supervisor:start_child(WorkerSup, [N])
    end, lists:seq(1, WorkerPoolSize)).

search_timers(
    FreeWorkersCount,
    NsId,
    #{
        storage := StorageOpts,
        process_step_timeout := TimeoutSec,
        task_scan_timeout := ScanTimeoutSec
    }
) ->
    try
        prg_storage:search_timers(StorageOpts, NsId, TimeoutSec + ScanTimeoutSec, FreeWorkersCount) of
            Result when is_list(Result) ->
                Result;
            Unexpected ->
                logger:error("search timers error: ~p", [Unexpected]),
                []
    catch
        Class:Reason:Trace ->
            logger:error("search timers exception: ~p", [[Class, Reason, Trace]]),
            []
    end.

search_calls(
    FreeWorkersCount,
    NsId,
    #{storage := StorageOpts}
) ->
    try prg_storage:search_calls(StorageOpts, NsId, FreeWorkersCount) of
        Result when is_list(Result) ->
            Result;
        Unexpected ->
            logger:error("search calls error: ~p", [Unexpected]),
            []
    catch
        Class:Reason:Trace ->
            logger:error("search calls exception: ~p", [[Class, Reason, Trace]]),
            []
    end.

do_push_task(TaskHeader, Task, State) ->
    FreeWorkers = State#prg_scheduler_state.free_workers,
    case queue:out(FreeWorkers) of
        {{value, Worker}, NewQueue} ->
            ok = prg_worker:process_task(Worker, TaskHeader, Task),
            State#prg_scheduler_state{
                free_workers = NewQueue
            };
        {empty, _} ->
            OldReady = State#prg_scheduler_state.ready,
            State#prg_scheduler_state{
                ready = queue:in({TaskHeader, Task}, OldReady)
            }
        end.

start_rescan_timers(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), rescan_timers).

start_rescan_calls(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), rescan_calls).
%%

header() ->
    header(<<"timeout">>).

header(Type) ->
    {erlang:binary_to_atom(Type), undefined}.
