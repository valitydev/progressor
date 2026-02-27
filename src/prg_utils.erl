-module(prg_utils).

-include("progressor.hrl").

%% API
-export([registered_name/2]).
-export([pipe/2]).
-export([format/1]).
-export([make_ns_opts/2]).
-export([unixtime_to_datetime/1]).
-export([with_observe/4]).
-export([with_observe/5]).
-export([with_span/2]).

-spec registered_name(atom(), string()) -> atom().
registered_name(BaseAtom, PostfixStr) ->
    erlang:list_to_atom(erlang:atom_to_list(BaseAtom) ++ PostfixStr).

-spec pipe([function()], term()) -> term().
pipe([], Result) -> Result;
pipe(_Funs, {error, _} = Error) -> Error;
pipe(_Funs, {break, Result}) -> Result;
pipe([F | Rest], Acc) -> pipe(Rest, F(Acc)).

-spec format(term()) -> binary().
format(Term) when is_binary(Term) ->
    Term;
format(Term) ->
    unicode:characters_to_binary(io_lib:format("~64000p", [Term])).

-spec make_ns_opts(atom(), namespace_opts()) -> namespace_opts().
make_ns_opts(NsId, NsOpts) ->
    PresetDefaults = #{
        namespace => erlang:atom_to_binary(NsId),
        retry_policy => ?DEFAULT_RETRY_POLICY,
        worker_pool_size => ?DEFAULT_WORKER_POOL_SIZE,
        process_step_timeout => ?DEFAULT_STEP_TIMEOUT_SEC,
        task_scan_timeout => (?DEFAULT_STEP_TIMEOUT_SEC div 2) + 1,
        call_scan_timeout => ?DEFAULT_CALL_SCAN_TIMEOUT_SEC,
        last_timer_repair => false
    },
    ConfigDefaults = application:get_env(progressor, defaults, #{}),
    Defaults = maps:merge(PresetDefaults, ConfigDefaults),
    maps:merge(Defaults, NsOpts).

-spec unixtime_to_datetime(timestamp_sec()) -> calendar:datetime().
unixtime_to_datetime(TimestampSec) ->
    calendar:gregorian_seconds_to_datetime(TimestampSec + ?EPOCH_DIFF).

-type span_params() :: #{
    name := binary(),
    attributes => opentelemetry:attributes_map(),
    kind => internal | server | client | producer | consumer
}.

-spec with_observe(_Fun, atom(), [list() | binary()], span_params() | undefined) -> any().
with_observe(Fun, MetricKey, Labels, SpanParams) ->
    with_observe(Fun, histogram, MetricKey, Labels, SpanParams).

-spec with_observe(fun(() -> T), atom(), atom(), [list() | binary()], span_params() | undefined) -> T when T :: any().
with_observe(Fun, MetricType, MetricKey, Labels, SpanParams) ->
    {DurationMicro, Result} = with_span(SpanParams, fun(_SpanCtx) -> timer:tc(Fun) end),
    DurationMs = DurationMicro div 1000,
    logger:debug("metric: ~p, labels: ~p, value: ~p", [MetricKey, Labels, DurationMs]),
    ok = collect(MetricType, MetricKey, Labels, DurationMs),
    Result.

-spec with_span(span_params() | undefined, fun((opentelemetry:span_ctx() | undefined) -> T)) -> T when T :: any().
with_span(undefined, Fun) ->
    Fun(undefined);
with_span(#{name := SpanName} = SpanParams, Fun) ->
    OtelCtx = otel_ctx:get_current(),
    Tracer = opentelemetry:get_application_tracer(progressor),
    SpanOpts = #{
        kind => maps:get(kind, SpanParams, internal),
        attributes => maps:get(attributes, SpanParams, #{})
    },
    SpannedFun = fun(SpanCtx) ->
        try
            Fun(SpanCtx)
        catch
            Class:Reason:Stacktrace ->
                _ = otel_span:record_exception(SpanCtx, Class, Reason, Stacktrace, #{}),
                erlang:raise(Class, Reason, Stacktrace)
        end
    end,
    otel_tracer:with_span(OtelCtx, Tracer, SpanName, SpanOpts, SpannedFun).

collect(histogram, MetricKey, Labels, Value) ->
    prometheus_histogram:observe(MetricKey, Labels, Value).
%%collect(_, _MetricKey, _Labels, _Value) ->
%%    %% TODO implement it
%%    ok.
