%%%-------------------------------------------------------------------
%% @doc base_bench public API
%% @end
%%%-------------------------------------------------------------------

-module(base_bench_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = prg_manager:create_namespace('default', #{
        processor => #{
            client => base_bench_processor,
            options => #{}
        }
    }),
    base_bench_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
