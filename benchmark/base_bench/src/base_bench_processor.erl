-module(base_bench_processor).

-export([process/3]).

process({init, Args, _Process}, _Opts, _Ctx) ->
    Fin = erlang:system_time(second) + binary_to_term(Args),
    Result = #{
        metadata => #{finish => Fin},
        events => [event(1)],
        action => timeout
    },
    {ok, Result};
%%
process({timeout, _Args, #{history := History, metadata := Meta} = _Process}, _Opts, _Ctx) ->
    #{finish := FinishTime} = Meta,
    Action =
        case FinishTime > erlang:system_time(second) of
            true -> timeout;
            false -> suspend
        end,
    NextId = erlang:length(History) + 1,
    Result = #{
        events => [event(NextId)],
        action => Action
    },
    {ok, Result};
%%
process({call, _Args, #{history := History} = _Process}, _Opts, _Ctx) ->
    Result = #{
        response => erlang:length(History),
        events => [],
        action => suspend
    },
    {ok, Result}.
%%

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(64)})
    }.
