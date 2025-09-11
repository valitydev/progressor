-module(prg_pg_utils).

-include_lib("progressor/include/progressor.hrl").

-export([tables/1]).
-export([marshal_process/1]).
-export([marshal_process_state/1]).
-export([marshal_task/1]).
-export([convert/2]).

-spec tables(namespace_id()) -> map().
tables(NsId) ->
    #{
        processes => construct_table_name(NsId, "_processes"),
        tasks => construct_table_name(NsId, "_tasks"),
        schedule => construct_table_name(NsId, "_schedule"),
        running => construct_table_name(NsId, "_running"),
        generations => construct_table_name(NsId, "_generations")
    }.

-spec marshal_process(map()) -> process().
marshal_process(Process) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"status">>, Status, Acc) -> Acc#{status => Status};
            (<<"detail">>, Detail, Acc) -> Acc#{detail => Detail};
            (<<"aux_state">>, AuxState, Acc) -> Acc#{aux_state => AuxState};
            (<<"metadata">>, Meta, Acc) -> Acc#{metadata => Meta};
            (<<"current_generation">>, Gen, Acc) -> Acc#{current_generation => Gen};
            (<<"corrupted_by">>, CorruptedBy, Acc) -> Acc#{corrupted_by => CorruptedBy};
            (_, _, Acc) -> Acc
        end,
        #{},
        Process
    ).

-spec marshal_process_state(map()) -> process_state().
marshal_process_state(ProcessState) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"generation">>, Gen, Acc) -> Acc#{generation => Gen};
            (<<"timestamp">>, Ts, Acc) -> Acc#{timestamp => Ts};
            (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
            (<<"payload">>, Payload, Acc) -> Acc#{payload => Payload};
            (_, _, Acc) -> Acc
        end,
        #{},
        ProcessState
    ).

-spec marshal_task(map()) -> task().
marshal_task(Task) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"task_type">>, TaskType, Acc) -> Acc#{task_type => TaskType};
            (<<"status">>, Status, Acc) -> Acc#{status => Status};
            (<<"scheduled_time">>, Ts, Acc) -> Acc#{scheduled_time => Ts};
            (<<"running_time">>, Ts, Acc) -> Acc#{running_time => Ts};
            (<<"args">>, Args, Acc) -> Acc#{args => Args};
            (<<"generation">>, Gen, Acc) -> Acc#{generation => Gen};
            (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
            (<<"idempotency_key">>, IdempotencyKey, Acc) -> Acc#{idempotency_key => IdempotencyKey};
            (<<"response">>, Response, Acc) -> Acc#{response => Response};
            (<<"blocked_task">>, BlockedTaskId, Acc) -> Acc#{blocked_task => BlockedTaskId};
            (<<"last_retry_interval">>, LastRetryInterval, Acc) -> Acc#{last_retry_interval => LastRetryInterval};
            (<<"attempts_count">>, AttemptsCount, Acc) -> Acc#{attempts_count => AttemptsCount};
            (<<"context">>, Context, Acc) -> Acc#{context => Context};
            (_, _, Acc) -> Acc
        end,
        #{},
        Task
    ).

-spec convert(_, _) -> _.
convert(_Type, null) ->
    null;
convert(timestamp, Value) ->
    daytime_to_unixtime(Value);
convert(timestamptz, Value) ->
    daytime_to_unixtime(Value);
convert(jsonb, Value) ->
    jsx:decode(Value, [return_maps]);
convert(json, Value) ->
    jsx:decode(Value, [return_maps]);
convert(_Type, Value) ->
    Value.

%% Internal functions

daytime_to_unixtime({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    daytime_to_unixtime({Date, {Hour, Minute, trunc(Second)}});
daytime_to_unixtime(Daytime) ->
    to_unixtime(calendar:datetime_to_gregorian_seconds(Daytime)).

to_unixtime(Time) when is_integer(Time) ->
    Time - ?EPOCH_DIFF.

construct_table_name(NsId, Postfix) ->
    "\"" ++ erlang:atom_to_list(NsId) ++ Postfix ++ "\"".
