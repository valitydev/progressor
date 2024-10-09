-module(prg_scheduler).

-include("progressor.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% API
-export([push_task/3]).
-export([pop_task/2]).
-export([count_workers/1]).
-export([capture_worker/2]).
-export([return_worker/3]).
-export([release_worker/3]).
-export([continuation_task/3]).

-record(prg_scheduler_state, {ns_id, ns_opts, ready, free_workers, owners}).

%%%%%%%
%%% API
%%%%%%%

-spec push_task(namespace_id(), task_header(), task()) -> ok.
push_task(NsId, TaskHeader, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:cast(RegName, {push_task, TaskHeader, Task}).

-spec pop_task(namespace_id(), pid()) -> {task_header(), task()} | not_found.
pop_task(NsId, Worker) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {pop_task, Worker}, infinity).

-spec continuation_task(namespace_id(), pid(), task()) -> {task_header(), task()} | ok.
continuation_task(NsId, Worker, Task) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {continuation_task, Worker, Task}).

-spec count_workers(namespace_id()) -> non_neg_integer().
count_workers(NsId) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, count_workers, infinity).

-spec capture_worker(namespace_id(), pid()) -> {ok, pid()} | {error, not_found | nested_capture}.
capture_worker(NsId, Owner) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:call(RegName, {capture_worker, Owner}, infinity).

-spec return_worker(namespace_id(), pid(), pid()) -> ok.
return_worker(NsId, Owner, Worker) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:cast(RegName, {return_worker, Owner, Worker}).

-spec release_worker(namespace_id(), pid(), pid()) -> ok.
release_worker(NsId, Owner, Pid) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:cast(RegName, {release_worker, Owner, Pid}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link({NsId, _NsOpts} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_scheduler"),
    gen_server:start_link({local, RegName}, ?MODULE, NS, []).

init({NsId, Opts}) ->
    _ = start_workers(NsId, Opts),
    State = #prg_scheduler_state{
        ns_id = NsId,
        ns_opts = Opts,
        ready = queue:new(),
        free_workers = queue:new(),
        owners = #{}
    },
    {ok, State}.

handle_call({pop_task, Worker}, _From, State) ->
    case queue:out(State#prg_scheduler_state.ready) of
        {{value, TaskData}, NewReady} ->
            {reply, TaskData, State#prg_scheduler_state{ready = NewReady}};
        {empty, _} ->
            Workers = State#prg_scheduler_state.free_workers,
            {reply, not_found, State#prg_scheduler_state{free_workers = queue:in(Worker, Workers)}}
    end;
handle_call(count_workers, _From, #prg_scheduler_state{free_workers = Workers} = State) ->
    {reply, queue:len(Workers), State};
handle_call(
    {capture_worker, Owner}, _From,
    #prg_scheduler_state{owners = Owners} = State
) when erlang:is_map_key(Owner, Owners) ->
    {reply, {error, nested_capture}, State};
handle_call(
    {capture_worker, Owner}, _From,
    #prg_scheduler_state{owners = Owners} = State
) ->
    case queue:out(State#prg_scheduler_state.free_workers) of
        {{value, Worker}, NewWorkers} ->
            MRef = erlang:monitor(process, Owner),
            NewOwners = Owners#{Owner => {MRef, Worker}},
            {reply, {ok, Worker}, State#prg_scheduler_state{free_workers = NewWorkers, owners = NewOwners}};
        {empty, _} ->
            {reply, {error, not_found}, State}
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
handle_cast(
    {return_worker, Owner, Worker},
    State = #prg_scheduler_state{free_workers = Workers, owners = Owners}
) ->
    case maps:get(Owner, Owners, undefined) of
        undefined ->
            skip;
        {Ref, Worker} ->
            _ = erlang:demonitor(Ref)
    end,
    NewWorkers = queue:in(Worker, Workers),
    NewOwners = maps:without([Owner], Owners),
    {noreply, State#prg_scheduler_state{free_workers = NewWorkers, owners = NewOwners}};
handle_cast(
    {release_worker, Owner, Worker},
    State = #prg_scheduler_state{owners = Owners}
) ->
    NewState = case maps:get(Owner, Owners, undefined) of
        undefined ->
            State;
        {Ref, Worker} ->
            _ = erlang:demonitor(Ref),
            State#prg_scheduler_state{owners = maps:without([Owner], Owners)}
    end,
    {noreply, NewState};
handle_cast(_Request, State = #prg_scheduler_state{}) ->
    {noreply, State}.

handle_info(
    {'DOWN', _Ref, process, Pid, _Info},
    State = #prg_scheduler_state{owners = Owners}
) when erlang:is_map_key(Pid, Owners) ->
    {noreply, State#prg_scheduler_state{owners = maps:without([Pid], Owners)}};
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

header() ->
    header(<<"timeout">>).

header(Type) ->
    {erlang:binary_to_atom(Type), undefined}.
