-module(progressor_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok,
        {
            #{
                strategy => one_for_all,
                intensity => 0,
                period => 1
            },
            [
                prg_manager:child_spec(manager),
                prg_namespaces_root_sup:child_spec(namespaces)
            ]
        }}.
