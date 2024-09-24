%%%-------------------------------------------------------------------
%% @doc progressor public API
%% @end
%%%-------------------------------------------------------------------

-module(progressor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    create_metrics(),
    progressor_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

create_metrics() ->
%    _ = prometheus_counter:new([
%        {name, progressor_requests_total},
%        {help, "Progressor request (init, call, repair, get) count"},
%        {labels, [service, namespace, task_type]}
%    ]),
%    _ = prometheus_histogram:new([
%        {name, progressor_requests_duration_ms},
%        {help, "Progressor request (init, call, repair, get) durations in millisecond"},
%        {buckets, [50, 150, 300, 500, 750, 1000]},
%        {labels, [service, namespace, task_type]}
%    ]),

    _ = prometheus_histogram:new([
        {name, progressor_request_preparing_duration_ms},
        {help, "Preparing request (init, call, repair, get) durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [service, namespace, task_type]}
    ]),

    _ = prometheus_histogram:new([
        {name, progressor_task_processing_duration_ms},
        {help, "Task processing durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [service, namespace, task_type]}
    ]),

    _ = prometheus_histogram:new([
        {name, progressor_task_completion_duration_ms},
        {help, "Task completion durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [service, namespace, completion_type]}
    ]),

    _ = prometheus_histogram:new([
        {name, progressor_process_removing_duration_ms},
        {help, "Task completion durations in millisecond"},
        {buckets, [50, 150, 300, 500, 750, 1000]},
        {labels, [service, namespace]}
    ]),

    _ = prometheus_histogram:new([
        {name, progressor_notification_duration_ms},
        {help, "Notification durations in millisecond"},
        {buckets, [10, 50, 150, 300, 500, 1000]},
        {labels, [service, namespace, notification_type]}
    ]).
