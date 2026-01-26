-ifndef(__progressor_otel__).
-define(__progressor_otel__, ok).

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

%% NOTE See `otel_tracer_default:with_span/5`
-define(with_span(Ctx, SpanName, Fun), begin
    SpanCtx = otel_tracer:start_span(Ctx, ?tracer, SpanName, #{kind => internal}),
    Ctx1 = otel_tracer:set_current_span(Ctx, SpanCtx),
    Token = otel_ctx:attach(Ctx1),
    try
        Fun()
    after
        _ = otel_span_ets:end_span(SpanCtx),
        otel_ctx:detach(Token)
    end
end).
-define(with_span(SpanName, Fun), ?with_span(?current_otel_ctx, SpanName, Fun)).

-endif.
