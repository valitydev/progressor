-module(base_bench).

-behaviour(gen_server).

-export([start/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(NS, default).

-record(base_bench_state, {ids, duration}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start(ProcCount, DurationSec) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ProcCount, DurationSec], []).

init([ProcCount, DurationSec]) ->
    IDs = start_processes(ProcCount, DurationSec),
    io:format(user, "Started: ~p~n", [calendar:system_time_to_rfc3339(erlang:system_time(second))]),
    erlang:start_timer(DurationSec * 1000, self(), finish),
    {ok, #base_bench_state{ids = IDs, duration = DurationSec}}.

handle_call(_Request, _From, State = #base_bench_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #base_bench_state{}) ->
    {noreply, State}.

handle_info({timeout, _TimerRef, finish}, State = #base_bench_state{ids = IDs, duration = Duration}) ->
    io:format(user, "Stopping: ~p~n", [calendar:system_time_to_rfc3339(erlang:system_time(second))]),
    {EvCountsList, ErrCount} = stop_processes(IDs),
    io:format(user, "Finish: ~p~n", [calendar:system_time_to_rfc3339(erlang:system_time(second))]),
    Max = lists:max(EvCountsList),
    Min = lists:min(EvCountsList),
    Avg = lists:sum(EvCountsList) / erlang:length(EvCountsList),
    Rate = lists:sum(EvCountsList) / Duration,
    io:format(user, "Max: ~p~nMin: ~p~nAvg: ~p~nRate: ~p~nErrors: ~p~n", [Max, Min, Avg, Rate, ErrCount]),
    {noreply, State};
handle_info(_Info, State = #base_bench_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #base_bench_state{}) ->
    ok.

code_change(_OldVsn, State = #base_bench_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_processes(N, Duration) ->
    lists:foldl(fun(_N, Acc) -> [start_process(Duration) | Acc] end, [], lists:seq(1, N)).

start_process(Duration) ->
    Id = gen_id(),
    _ = spawn(progressor, init, [#{ns => ?NS, id => Id, args => term_to_binary(Duration)}]),
    Id.
%%

stop_processes(IDs) ->
    lists:foldl(fun(Id, Acc) ->
        do_call(#{ns => ?NS, id => Id, args => <<>>}, Acc)
    end, {[], 0}, IDs).
%%

do_call(Req, {Evs, Errs}) ->
    try progressor:call(Req) of
        {ok, EventsCount} ->
            {[EventsCount | Evs], Errs};
        {error, _} ->
            {Evs, Errs + 1}
    catch
        _Ex:_Er ->
            {Evs, Errs + 1}
    end.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
