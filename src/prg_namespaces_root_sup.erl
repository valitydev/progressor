-module(prg_namespaces_root_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([child_spec/1]).
-export([create_namespace/2]).
-export([destroy_namespace/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

child_spec(ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, []},
        type => supervisor
    }.

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

create_namespace(ID, NsOpts) ->
    NS = {ID, prg_utils:make_ns_opts(ID, NsOpts)},
    {ok, _NsPid} = supervisor:start_child(?SERVER, [NS]).

destroy_namespace(NsPid) ->
    case supervisor:terminate_child(?SERVER, NsPid) of
        {error, not_found} ->
            ok;
        ok ->
            ok
    end.

init([]) ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs = [
        #{
            id => namespace,
            start => {prg_namespace_sup, start_link, []},
            restart => permanent,
            type => supervisor
        }
    ],
    {ok, {Flags, ChildsSpecs}}.
