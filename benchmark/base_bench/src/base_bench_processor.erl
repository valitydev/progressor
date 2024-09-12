-module(base_bench_processor).

-export([process/3]).

process({init, _Args, _Process}, _Opts, _Ctx) ->
    Result = #{
        events => [event(1)],
        action => #{set_timer => erlang:system_time(second)}
    },
    {ok, Result};
%%
process({timeout, _Args, #{history := History} = _Process}, _Opts, _Ctx) ->
    %% Random = rand:uniform(40),
    %% timer:sleep(60 + Random),
    NextId = erlang:length(History) + 1,
    Result = #{
        events => [event(NextId)],
        action => #{set_timer => erlang:system_time(second)}
    },
    {ok, Result};
%%
process({call, _Args, #{history := History} = _Process}, _Opts, _Ctx) ->
    Result = #{
        response => erlang:length(History),
        events => [],
        action => unset_timer
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
