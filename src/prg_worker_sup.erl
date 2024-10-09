-module(prg_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link(term()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({NsId, _} = NS) ->
    RegName = prg_utils:registered_name(NsId, "_worker_sup"),
    supervisor:start_link({local, RegName}, ?MODULE, NS).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init({NsId, NsOpts}) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = #{strategy => simple_one_for_one,
        intensity => MaxRestarts,
        period => MaxSecondsBetweenRestarts},
    ChildSpecs = [
        #{
            id => prg_utils:registered_name(NsId, "_prg_worker"),
            start => {prg_worker, start_link, [NsId, NsOpts]}
        }
    ],

    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
