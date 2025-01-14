-module(prg_echo_processor).

-export([process/3]).

-spec process(_Req, _Opts, _Ctx) -> _.
process({_, _, #{history := History} = _Process}, _Opts, _Ctx) ->
    case erlang:length(History) of
        10 = Count ->
            Result = #{
                events => [event(Count + 1)]
            },
            {ok, Result};
        Count ->
            Result = #{
                events => [event(Count + 1)],
                action => #{set_timer => erlang:system_time(second)}
            },
            {ok, Result}
    end.

%%

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.
