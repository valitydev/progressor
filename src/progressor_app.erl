%%%-------------------------------------------------------------------
%% @doc progressor public API
%% @end
%%%-------------------------------------------------------------------

-module(progressor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    progressor_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
