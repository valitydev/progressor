-module(prg_namespace_sup).

-include("progressor.hrl").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link({namespace_id(), namespace_opts()}) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link({NsId, #{storage := StorageOpts}} = NS) ->
    ok = prg_storage:db_init(StorageOpts, NsId),
    RegName = prg_utils:registered_name(NsId, "_namespace_sup"),
    supervisor:start_link({local, RegName}, ?MODULE, NS).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init({NsId, _NsOpts} = NS) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = #{strategy => one_for_all,
        intensity => MaxRestarts,
        period => MaxSecondsBetweenRestarts},
    SchedulerSpec = #{
        id => prg_utils:registered_name(NsId, "_scheduler"),
        start => {prg_scheduler, start_link, [NS]}
    },
    WorkerSupSpec = #{
        id => prg_utils:registered_name(NsId, "_worker_sup"),
        start => {prg_worker_sup, start_link, [NS]},
        type => supervisor
    },
    Specs = [
        WorkerSupSpec,
        SchedulerSpec
    ],

    {ok, {SupFlags, Specs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
