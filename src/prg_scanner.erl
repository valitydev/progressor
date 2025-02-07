-module(prg_scanner).

-behaviour(gen_server).

-export([start_link/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(prg_scanner_state, {ns_id, ns_opts, rescan_timeout, step_timeout}).

-define(CALLS_SCAN_KEY, progressor_calls_scanning_duration_ms).
-define(TIMERS_SCAN_KEY, progressor_timers_scanning_duration_ms).
-define(ZOMBIE_COLLECTION_KEY, progressor_zombie_collection_duration_ms).

-dialyzer({nowarn_function, search_timers/3}).
-dialyzer({nowarn_function, search_calls/3}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link({NsId, _NsOpts} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_scanner"),
    gen_server:start_link({local, RegName}, ?MODULE, NS, []).

init(
    {NsId, #{task_scan_timeout := RescanTimeoutSec, process_step_timeout := StepTimeoutSec} = Opts}
) ->
    RescanTimeoutMs = RescanTimeoutSec * 1000,
    StepTimeoutMs = StepTimeoutSec * 1000,
    State = #prg_scanner_state{
        ns_id = NsId,
        ns_opts = Opts,
        rescan_timeout = RescanTimeoutMs,
        step_timeout = StepTimeoutMs
    },
    _ = start_rescan_timers(RescanTimeoutMs),
    _ = start_rescan_calls((RescanTimeoutMs div 3) + 100),
    _ = start_zombie_collector(StepTimeoutMs),
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, #prg_scanner_state{} = State) ->
    {noreply, State}.

handle_info(
    {timeout, _TimerRef, rescan_timers},
    #prg_scanner_state{ns_id = NsId, ns_opts = NsOpts, rescan_timeout = RescanTimeout} = State
) ->
    case prg_scheduler:count_workers(NsId) of
        0 ->
            %% all workers is busy
            skip;
        N ->
            Calls = search_calls(N, NsId, NsOpts),
            Timers = search_timers(N - erlang:length(Calls), NsId, NsOpts),
            Tasks = Calls ++ Timers,
            lists:foreach(
                fun(#{task_type := Type} = Task) ->
                    ok = prg_scheduler:push_task(NsId, header(Type), Task)
                end,
                Tasks
            )
    end,
    _ = start_rescan_timers(RescanTimeout),
    {noreply, State};
handle_info(
    {timeout, _TimerRef, rescan_calls},
    #prg_scanner_state{ns_id = NsId, ns_opts = NsOpts, rescan_timeout = RescanTimeout} = State
) ->
    case prg_scheduler:count_workers(NsId) of
        0 ->
            %% all workers is busy
            skip;
        N ->
            Calls = search_calls(N, NsId, NsOpts),
            lists:foreach(
                fun(#{task_type := Type} = Task) ->
                    ok = prg_scheduler:push_task(NsId, header(Type), Task)
                end,
                Calls
            )
    end,
    _ = start_rescan_calls((RescanTimeout div 3) + 1),
    {noreply, State};
handle_info(
    {timeout, _TimerRef, collect_zombie},
    #prg_scanner_state{ns_id = NsId, ns_opts = NsOpts, step_timeout = StepTimeout} = State
) ->
    ok = collect_zombie(NsId, NsOpts),
    _ = start_zombie_collector(StepTimeout),
    {noreply, State};
handle_info(_Info, #prg_scanner_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #prg_scanner_state{} = _State) ->
    ok.

code_change(_OldVsn, #prg_scanner_state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_rescan_timers(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), rescan_timers).

start_rescan_calls(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), rescan_calls).

start_zombie_collector(RescanTimeoutMs) ->
    RandomDelta = rand:uniform(RescanTimeoutMs div 5),
    erlang:start_timer(RescanTimeoutMs + RandomDelta, self(), collect_zombie).

search_timers(
    FreeWorkersCount,
    NsId,
    #{
        storage := StorageOpts,
        process_step_timeout := TimeoutSec,
        task_scan_timeout := ScanTimeoutSec
    }
) ->
    Fun = fun() ->
        try
            prg_storage:search_timers(
                StorageOpts, NsId, TimeoutSec + ScanTimeoutSec, FreeWorkersCount
            )
        of
            Result when is_list(Result) ->
                Result;
            Unexpected ->
                logger:error("search timers error: ~p", [Unexpected]),
                []
        catch
            Class:Reason:Trace ->
                logger:error("search timers exception: ~p", [[Class, Reason, Trace]]),
                []
        end
    end,
    prg_utils:with_observe(Fun, ?TIMERS_SCAN_KEY, [erlang:atom_to_binary(NsId, utf8)]).

search_calls(
    FreeWorkersCount,
    NsId,
    #{storage := StorageOpts}
) ->
    Fun = fun() ->
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
        end
    end,
    prg_utils:with_observe(Fun, ?CALLS_SCAN_KEY, [erlang:atom_to_binary(NsId, utf8)]).

collect_zombie(
    NsId,
    #{
        storage := StorageOpts,
        process_step_timeout := TimeoutSec,
        task_scan_timeout := ScanTimeoutSec
    }
) ->
    Fun = fun() ->
        try
            prg_storage:collect_zombies(StorageOpts, NsId, TimeoutSec + ScanTimeoutSec)
        catch
            Class:Reason:Trace ->
                logger:error("zombie collection exception: ~p", [[Class, Reason, Trace]])
        end,
        ok
    end,
    prg_utils:with_observe(Fun, ?ZOMBIE_COLLECTION_KEY, [erlang:atom_to_binary(NsId, utf8)]).

header(Type) ->
    {erlang:binary_to_atom(Type), undefined}.
