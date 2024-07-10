-module(prg_scheduler).

-include("progressor.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([handle_continue/2]).

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

init({NsId, #{process_step_timeout := StepTimeoutSec} = Opts}) ->
    _ = start_workers(NsId, Opts),
    RescanTimeoutMs = StepTimeoutSec  * 500, %% 1/2 step timeout in milliseconds
    State = #prg_scheduler_state{
        ns_id = NsId,
        ns_opts = Opts,
        ready = queue:new(),
        free_workers = queue:new(),
        rescan_timeout = RescanTimeoutMs
    },
    {ok, State, {continue, search_tasks}}.

handle_continue(search_tasks, State = #prg_scheduler_state{ns_id = NsId, ns_opts = NsOpts, rescan_timeout = RescanTimeoutMs}) ->
    Tasks = search_tasks(NsId, NsOpts),
    NewState = lists:foldl(fun(Task, Acc) -> do_push_task(header(), Task, Acc) end, State, Tasks),
    _ = start_rescan_timer(RescanTimeoutMs),
    {noreply, NewState}.

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
    {timeout, _TimerRef, rescan_timer},
    State = #prg_scheduler_state{ns_id = NsId, ns_opts = NsOpts, free_workers = Workers, rescan_timeout = RescanTimeout}
) ->
    NewState =
        case queue:is_empty(Workers) of
            true ->
                %% all workers is busy
                _ = start_rescan_timer(RescanTimeout),
                State;
            false ->
                Tasks = search_tasks(NsId, NsOpts),
                _ = start_rescan_timer(RescanTimeout),
                lists:foldl(fun(Task, Acc) -> do_push_task(header(), Task, Acc) end, State, Tasks)
        end,
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

search_tasks(
    NsId,
    #{
        storage := StorageOpts,
        process_step_timeout := TimeoutSec,
        worker_pool_size := PoolSize,
        task_scan_timeout := ScanTimeoutSec}
) ->
    prg_storage:search_tasks(StorageOpts, NsId, TimeoutSec + ScanTimeoutSec, PoolSize).

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

start_rescan_timer(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), rescan_timer).

header() ->
    {timeout, undefined}.
