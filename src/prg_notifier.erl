-module(prg_notifier).

-include("progressor.hrl").
-include_lib("mg_proto/include/mg_proto_lifecycle_sink_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_event_sink_thrift.hrl").

-export([event_sink/3]).
-export([lifecycle_sink/3]).

%% internals
-export([serialize_content/1]).

-spec event_sink(namespace_opts(), id(), [event()]) -> ok | no_return().
event_sink(_NsOpts, _ID, []) ->
    ok;
event_sink(
    #{
        namespace := NS,
        notifier := #{client := Client, options := #{topic := Topic}}
    },
    ID,
    Events
) ->
    Batch = encode(fun serialize_eventsink/3, NS, ID, Events),
    ok = produce(Client, Topic, event_key(NS, ID), Batch),
    ok;
event_sink(_NsOpts, _ID, _Events) ->
    ok.

-spec lifecycle_sink(namespace_opts(), task_t() | {error, _Reason}, id()) -> ok | no_return().
lifecycle_sink(
    #{
        namespace := NS,
        notifier := #{client := Client, options := #{lifecycle_topic := Topic}}
    },
    TaskType,
    ID
) when
    TaskType =:= init;
    TaskType =:= repair;
    TaskType =:= remove;
    erlang:is_tuple(TaskType)
->
    Batch = encode(fun serialize_lifecycle/3, NS, ID, [lifecycle_event(TaskType)]),
    ok = produce(Client, Topic, event_key(NS, ID), Batch),
    ok;
lifecycle_sink(_NsOpts, _TaskType, _ID) ->
    ok.

%% Internal functions

encode(Encoder, NS, ID, Events) ->
    [
        #{
            key => event_key(NS, ID),
            value => Encoder(NS, ID, Event)
        } || Event <- Events
    ].

produce(Client, Topic, PartitionKey, Batch) ->
    case brod:get_partitions_count(Client, Topic) of
        {ok, PartitionsCount} ->
            Partition = partition(PartitionsCount, PartitionKey),
            case brod:produce_sync_offset(Client, Topic, Partition, PartitionKey, Batch) of
                {ok, _Offset} ->
                    ok;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.

event_key(NS, ID) ->
    <<NS/binary, " ", ID/binary>>.

%% eventsink serialization

serialize_eventsink(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    #{
        event_id := EventID,
        timestamp := Timestamp,
        payload := Payload
    } = Event,
    Content = erlang:binary_to_term(Payload),
    Metadata = maps:get(metadata, Event, #{}),
    Data =
        {event, #mg_evsink_MachineEvent{
            source_ns = SourceNS,
            source_id = SourceID,
            event_id = EventID,
            created_at = serialize_timestamp(Timestamp),
            format_version = maps:get(format_version, Metadata, undefined),
            data = Content
        }},
    Type = {struct, union, {mg_proto_event_sink_thrift, 'SinkEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

serialize_content(null) ->
    {nl, #mg_msgpack_Nil{}};
serialize_content(Boolean) when is_boolean(Boolean) ->
    {b, Boolean};
serialize_content(Integer) when is_integer(Integer) ->
    {i, Integer};
serialize_content(Float) when is_float(Float) ->
    {flt, Float};
serialize_content({string, String}) ->
    {str, unicode:characters_to_binary(String, unicode)};
serialize_content(Binary) when is_binary(Binary) ->
    {bin, Binary};
serialize_content(Object) when is_map(Object) ->
    {obj,
        maps:fold(
            fun(K, V, Acc) -> maps:put(serialize_content(K), serialize_content(V), Acc) end,
            #{},
            Object
        )};
serialize_content(Array) when is_list(Array) ->
    {arr, lists:map(fun serialize_content/1, Array)};
serialize_content(Arg) ->
    erlang:error(badarg, [Arg]).

serialize_timestamp(TimestampSec) ->
    Str = calendar:system_time_to_rfc3339(TimestampSec, [{unit, second}, {offset, "Z"}]),
    erlang:list_to_binary(Str).

%% lifecycle serialization

lifecycle_event(init) ->
    {machine_lifecycle_created, #{occurred_at => erlang:system_time(second)}};
lifecycle_event(repair) ->
    {machine_lifecycle_repaired, #{occurred_at => erlang:system_time(second)}};
lifecycle_event(remove) ->
    {machine_lifecycle_removed, #{occurred_at => erlang:system_time(second)}};
lifecycle_event({error, Reason}) ->
    {machine_lifecycle_failed, #{occurred_at => erlang:system_time(second), reason => Reason}}.

serialize_lifecycle(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    Data = serialize_lifecycle_event(SourceNS, SourceID, Event),
    Type = {struct, struct, {mg_proto_lifecycle_sink_thrift, 'LifecycleEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

serialize_lifecycle_event(SourceNS, SourceID, {_, #{occurred_at := Timestamp}} = Event) ->
    #mg_lifesink_LifecycleEvent{
        machine_ns = SourceNS,
        machine_id = SourceID,
        created_at = serialize_timestamp(Timestamp),
        data = serialize_lifecycle_data(Event)
    }.

serialize_lifecycle_data({machine_lifecycle_created, _}) ->
    {machine, {created, #mg_lifesink_MachineLifecycleCreatedEvent{}}};
serialize_lifecycle_data({machine_lifecycle_failed, #{reason := Reason}}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status =
                {failed, #mg_stateproc_MachineStatusFailed{
                    reason = Reason
                }}
        }}};
serialize_lifecycle_data({machine_lifecycle_repaired, _}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status = {working, #mg_stateproc_MachineStatusWorking{}}
        }}};
serialize_lifecycle_data({machine_lifecycle_removed, _}) ->
    {machine, {removed, #mg_lifesink_MachineLifecycleRemovedEvent{}}}.
