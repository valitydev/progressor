-ifndef(__progressor_otel__).
-define(__progressor_otel__, ok).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(current_otel_ctx, otel_ctx:get_current()).

-define(current_span_ctx, otel_tracer:current_span_ctx(?current_otel_ctx)).

-define(span_exception(Class, Error, Stacktrace),
    otel_span:record_exception(?current_span_ctx, Class, Error, Stacktrace, #{})
).
-define(span_exception(Class, Error, Message, Stacktrace),
    otel_span:record_exception(?current_span_ctx, Class, Error, Message, Stacktrace, #{})
).

-define(span_event(EventName), otel_span:add_event(?current_span_ctx, EventName, #{})).

-define(span_attributes(Attributes), otel_span:set_attributes(?current_span_ctx, Attributes)).

-define(tracer, opentelemetry:get_application_tracer(?MODULE)).

%% TODO Maybe add macros for distinct span kinds, like SPAN_KIND_PRODUCER or SPAN_KIND_CONSUMER
-define(with_span(OtelCtx, SpanName, Fun),
    otel_tracer:with_span(OtelCtx, ?tracer, SpanName, #{kind => ?SPAN_KIND_SERVER}, fun(_SpanCtx) -> Fun() end)
).
-define(with_span(SpanName, Fun), ?with_span(?current_otel_ctx, SpanName, Fun)).

-endif.
