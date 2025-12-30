%%%-------------------------------------------------------------------
%% @doc progressor public API
%% @end
%%%-------------------------------------------------------------------

-module(progressor_app).

-include("progressor.hrl").

-behaviour(application).

-export([start/2, stop/1]).

-export([start_namespace/2]).

start(_StartType, _StartArgs) ->
    _ = create_metrics(),
    {ok, _} = Result = progressor_sup:start_link(),
    %%%
    %%% The following code is for backward compatibility with static namespace launching (from sys.config)
    %% namespaces starting
    _ = maps:fold(
        fun(NsID, NsOpts, _Acc) ->
            FullOpts = prg_utils:make_ns_opts(NsID, NsOpts),
            {ok, _} = start_namespace(NsID, FullOpts)
        end,
        #{},
        application:get_env(progressor, namespaces, #{})
    ),
    %% post hooks execution
    lists:foreach(
        fun({Module, Function, Args}) -> ok = erlang:apply(Module, Function, Args) end,
        application:get_env(progressor, post_init_hooks, [])
    ),
    %%%
    Result.

stop(_State) ->
    ok.

-spec start_namespace(namespace_id(), namespace_opts()) -> supervisor:startchild_ret().
start_namespace(NsID, NsOpts) ->
    NS = {NsID, NsOpts},
    ChildSpec = #{
        id => NsID,
        start => {prg_namespace_sup, start_link, [NS]},
        type => supervisor
    },
    supervisor:start_child(progressor_sup, ChildSpec).

%% internal functions

create_metrics() ->
    _ = prometheus_histogram:declare([
        {name, progressor_calls_scanning_duration_ms},
        {help, "Calls (call, repair) scanning durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_timers_scanning_duration_ms},
        {help, "Timers (timeout, remove) scanning durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_zombie_collection_duration_ms},
        {help, "Zombie tasks collecting durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_request_preparing_duration_ms},
        {help, "Preparing request (init, call, repair) durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace, task_type]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_task_processing_duration_ms},
        {help, "Task processing durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace, task_type]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_task_completion_duration_ms},
        {help, "Task completion durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace, completion_type]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_process_removing_duration_ms},
        {help, "Task completion durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [prg_namespace]}
    ]),

    _ = prometheus_histogram:declare([
        {name, progressor_notification_duration_ms},
        {help, "Notification durations in millisecond"},
        {buckets, [10, 50, 150, 300, 500, 1000]},
        {labels, [prg_namespace, notification_type]}
    ]).
