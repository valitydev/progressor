-module(prg_pg_utils).

-include_lib("progressor/include/progressor.hrl").

-export([tables/1]).
-export([marshal_process/1]).
-export([marshal_event/1]).
-export([convert/2]).

-spec tables(namespace_id()) -> map().
tables(NsId) ->
    #{
        processes => construct_table_name(NsId, "_processes"),
        tasks => construct_table_name(NsId, "_tasks"),
        schedule => construct_table_name(NsId, "_schedule"),
        running => construct_table_name(NsId, "_running"),
        events => construct_table_name(NsId, "_events")
    }.

-spec marshal_process(map()) -> map().
marshal_process(Process) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"status">>, Status, Acc) -> Acc#{status => Status};
            (<<"detail">>, Detail, Acc) -> Acc#{detail => Detail};
            (<<"aux_state">>, AuxState, Acc) -> Acc#{aux_state => AuxState};
            (<<"metadata">>, Meta, Acc) -> Acc#{metadata => Meta};
            (<<"corrupted_by">>, CorruptedBy, Acc) -> Acc#{corrupted_by => CorruptedBy};
            (_, _, Acc) -> Acc
        end,
        #{},
        Process
    ).

-spec marshal_event(map()) -> map().
marshal_event(Event) ->
    maps:fold(
        fun
            (_, null, Acc) -> Acc;
            (<<"process_id">>, ProcessId, Acc) -> Acc#{process_id => ProcessId};
            (<<"task_id">>, TaskId, Acc) -> Acc#{task_id => TaskId};
            (<<"event_id">>, EventId, Acc) -> Acc#{event_id => EventId};
            (<<"timestamp">>, Ts, Acc) -> Acc#{timestamp => Ts};
            (<<"metadata">>, MetaData, Acc) -> Acc#{metadata => MetaData};
            (<<"payload">>, Payload, Acc) -> Acc#{payload => Payload};
            (_, _, Acc) -> Acc
        end,
        #{},
        Event
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
