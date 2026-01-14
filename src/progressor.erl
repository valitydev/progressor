-module(progressor).

-include("progressor.hrl").

-define(TASK_REPEAT_REQUEST_TIMEOUT, 1000).
-define(PREPARING_KEY, progressor_request_preparing_duration_ms).

%% Public API
-export([init/1]).
-export([call/1]).
-export([repair/1]).
-export([simple_repair/1]).
-export([get/1]).
-export([put/1]).
-export([trace/1]).
-export([health_check/1]).
%% TODO
%% -export([remove/1]).

%% Internal API
-export([reply/2]).

%-ifdef(TEST).
-export([cleanup/1]).
%-endif.

-type request() :: #{
    ns := namespace_id(),
    id := id(),
    args => term(),
    idempotency_key => binary(),
    otel_ctx => otel_ctx:t(),
    context => binary(),
    range => history_range(),
    options => map(),
    _ => _
}.

%% see receive blocks bellow in this module
-spec reply(pid(), term()) -> term().
reply(Pid, Msg) ->
    Pid ! Msg.

%% API
-spec init(request()) -> {ok, _Result} | {error, _Reason}.
init(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun check_idempotency/1,
            fun add_task/1,
            fun(Data) -> prepare(fun prg_storage:prepare_init/4, Data) end,
            fun process_call/1
        ],
        Req#{type => init}
    ).

-spec call(request()) -> {ok, _Result} | {error, _Reason}.
call(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun check_idempotency/1,
            fun(Data) -> check_process_status(Data, <<"running">>) end,
            fun add_task/1,
            fun(Data) -> prepare(fun prg_storage:prepare_call/4, Data) end,
            fun process_call/1
        ],
        Req#{type => call}
    ).

-spec repair(request()) -> {ok, _Result} | {error, _Reason}.
repair(Req) ->
    prg_utils:pipe(
        [
            fun add_ns_opts/1,
            fun check_idempotency/1,
            fun(Data) -> check_process_status(Data, <<"error">>) end,
            fun add_task/1,
            fun(Data) -> prepare(fun prg_storage:prepare_repair/4, Data) end,
            fun process_call/1
        ],
        Req#{type => repair}
    ).

-spec simple_repair(request()) -> {ok, _Result} | {error, _Reason}.
simple_repair(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun check_idempotency/1,
            fun check_process_continuation/1,
            fun(Data) -> prepare_postponed(fun prg_storage:prepare_call/4, Data) end,
            fun do_simple_repair/1
        ],
        Req
    ).

-spec get(request()) -> {ok, _Result} | {error, _Reason}.
get(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun do_get/1
        ],
        Req
    ).

-spec put(request()) -> {ok, _Result} | {error, _Reason}.
put(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun do_put/1
        ],
        Req
    ).

-spec trace(request()) -> {ok, _Result :: term()} | {error, _Reason :: term()}.
trace(Req) ->
    prg_utils:pipe(
        [
            fun maybe_add_otel_ctx/1,
            fun add_ns_opts/1,
            fun do_trace/1
        ],
        Req
    ).

%% Details term must be json compatible for jsx encode/decode
-spec health_check([namespace_id()]) -> {Status :: passing | critical, Details :: term()}.
health_check(Namespaces) ->
    health_check(Namespaces, {passing, []}).

health_check([], Result) ->
    Result;
health_check([NsId | Tail], {passing, _}) ->
    health_check(Tail, health_check_namespace(NsId));
health_check(_Namespaces, Result) ->
    Result.

health_check_namespace(NsId) ->
    prg_utils:pipe(
        [
            fun add_ns_opts/1,
            fun do_health_check/1
        ],
        #{ns => NsId}
    ).

%-ifdef(TEST).

-spec cleanup(_) -> _.
cleanup(Opts) ->
    prg_utils:pipe(
        [
            fun add_ns_opts/1,
            fun cleanup_storage/1
        ],
        Opts
    ).

cleanup_storage(#{ns := NsId, ns_opts := #{storage := StorageOpts}}) ->
    ok = prg_storage:cleanup(StorageOpts, NsId).

%-endif.

%% Internal functions

maybe_add_otel_ctx(#{otel_ctx := _} = Opts) ->
    Opts;
maybe_add_otel_ctx(Opts) ->
    Opts#{otel_ctx => otel_ctx:get_current()}.

add_ns_opts(#{ns := NsId} = Opts) ->
    NSs = application:get_env(progressor, namespaces, #{}),
    case maps:get(NsId, NSs, undefined) of
        undefined ->
            {error, <<"namespace not found">>};
        NsOpts ->
            Opts#{ns_opts => prg_utils:make_ns_opts(NsId, NsOpts)}
    end.

check_idempotency(#{idempotency_key := _IdempotencyKey} = Req) ->
    case get_task_result(Req) of
        not_found -> Req;
        Result -> {break, Result}
    end;
check_idempotency(Req) ->
    Req.

add_task(#{id := Id, type := Type} = Opts) ->
    Context = maps:get(context, Opts, <<>>),
    Args = maps:get(args, Opts, <<>>),
    TaskData = #{
        process_id => Id,
        args => Args,
        task_type => convert_task_type(Type),
        context => Context,
        metadata => #{range => maps:get(range, Opts, #{})}
    },
    Task = make_task(maybe_add_idempotency(TaskData, maps:get(idempotency_key, Opts, undefined))),
    Opts#{task => Task}.

check_process_status(
    #{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId} = Opts, ExpectedStatus
) ->
    case prg_storage:get_process_status(StorageOpts, NsId, Id) of
        {ok, ExpectedStatus} -> Opts;
        {ok, OtherStatus} -> {error, <<"process is ", OtherStatus/binary>>};
        {error, _} = Error -> Error
    end.

check_process_continuation(
    #{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId} = Opts
) ->
    case prg_storage:get_process(external, StorageOpts, NsId, Id, #{limit => 0}) of
        {ok, #{status := <<"running">>}} ->
            {error, <<"process is running">>};
        {ok, #{status := <<"error">>, corrupted_by := TaskId}} when is_integer(TaskId) ->
            case prg_storage:get_task(external, StorageOpts, NsId, TaskId) of
                {ok, #{task_type := Type}} when Type =:= <<"timeout">>; Type =:= <<"remove">> ->
                    add_task(Opts#{type => Type});
                {ok, _} ->
                    Opts
            end;
        {ok, #{status := <<"error">>}} ->
            Opts;
        {error, _} = Error ->
            Error
    end.

do_simple_repair(#{task := _}) ->
    %% process will repaired via timeout task
    {ok, ok};
do_simple_repair(#{ns_opts := #{storage := StorageOpts} = NsOpts, id := Id, ns := NsId}) ->
    ok = prg_storage:repair_process(StorageOpts, NsId, Id),
    ok = prg_notifier:lifecycle_sink(NsOpts, repair, Id),
    {ok, ok}.

prepare(
    Fun,
    #{ns_opts := #{storage := StorageOpts} = NsOpts, ns := NsId, id := ProcessId, task := Task} =
        Req
) ->
    Worker = capture_worker(NsId),
    TaskStatus = check_for_run(Worker),
    TaskType = maps:get(task_type, Task),
    PrepareResult = prg_utils:with_observe(
        fun() -> Fun(StorageOpts, NsId, ProcessId, Task#{status => TaskStatus}) end,
        ?PREPARING_KEY,
        [erlang:atom_to_binary(NsId, utf8), TaskType]
    ),
    case PrepareResult of
        {ok, {continue, TaskId}} ->
            Req#{task => Task#{task_id => TaskId}, worker => Worker};
        {ok, {postpone, TaskId}} ->
            ok = return_worker(NsId, Worker),
            TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
            Timeout = TimeoutSec * 1000,
            case await_task_result(StorageOpts, NsId, {task_id, TaskId}, Timeout, 0) of
                {ok, _Result} = OK ->
                    {break, OK};
                {error, _Reason} = ERR ->
                    ERR
            end;
        {error, _} = Error ->
            ok = return_worker(NsId, Worker),
            Error
    end.

prepare_postponed(
    Fun,
    #{ns_opts := #{storage := StorageOpts}, ns := NsId, id := ProcessId, task := Task} = Req
) ->
    TaskType = maps:get(task_type, Task),
    PrepareResult = prg_utils:with_observe(
        fun() -> Fun(StorageOpts, NsId, ProcessId, Task#{status => <<"waiting">>}) end,
        ?PREPARING_KEY,
        [erlang:atom_to_binary(NsId, utf8), TaskType]
    ),
    case PrepareResult of
        {ok, {postpone, TaskId}} ->
            Req#{task => Task#{task_id => TaskId}};
        {error, _} = Error ->
            Error
    end;
prepare_postponed(_Fun, Req) ->
    %% Req without task, skip this step
    Req.

get_task_result(#{
    ns_opts := #{storage := StorageOpts} = NsOpts, ns := NsId, idempotency_key := IdempotencyKey
}) ->
    case prg_storage:get_task_result(StorageOpts, NsId, {idempotency_key, IdempotencyKey}) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            not_found;
        {error, in_progress} ->
            StepTimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
            StepTimeout = StepTimeoutSec * 1000,
            await_task_result(StorageOpts, NsId, {idempotency_key, IdempotencyKey}, StepTimeout, 0)
    end.

await_task_result(_StorageOpts, _NsId, _KeyOrId, StepTimeout, Duration) when Duration > StepTimeout ->
    {error, <<"timeout">>};
await_task_result(StorageOpts, NsId, KeyOrId, StepTimeout, Duration) ->
    case prg_storage:get_task_result(StorageOpts, NsId, KeyOrId) of
        {ok, Result} ->
            Result;
        {error, _} ->
            RepeatTimeout = application:get_env(progressor, task_repeat_request_timeout, ?TASK_REPEAT_REQUEST_TIMEOUT),
            timer:sleep(RepeatTimeout),
            await_task_result(
                StorageOpts, NsId, KeyOrId, StepTimeout, Duration + RepeatTimeout
            )
    end.

do_get(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId, range := HistoryRange} = Req) ->
    case prg_storage:get_process_with_initialization(StorageOpts, NsId, Id, HistoryRange) of
        {ok, #{initialization := _TaskId}} ->
            %% init task not finished, await and retry
            Timeout = application:get_env(progressor, task_repeat_request_timeout, ?TASK_REPEAT_REQUEST_TIMEOUT),
            timer:sleep(Timeout),
            do_get(Req);
        Result ->
            Result
    end;
do_get(Req) ->
    do_get(Req#{range => #{}}).

do_trace(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId}) ->
    prg_storage:process_trace(StorageOpts, NsId, Id).

do_put(
    #{
        ns_opts := #{storage := StorageOpts},
        id := Id,
        ns := NsId,
        args := #{process := Process} = Args
    } = Opts
) ->
    #{
        process_id := ProcessId
    } = Process,
    Action = maps:get(action, Args, undefined),
    Context = maps:get(context, Opts, <<>>),
    Now = erlang:system_time(second),
    InitTask = #{
        process_id => ProcessId,
        task_type => <<"init">>,
        status => <<"finished">>,
        args => <<>>,
        context => Context,
        response => term_to_binary({ok, ok}),
        scheduled_time => Now,
        running_time => Now,
        finished_time => Now,
        last_retry_interval => 0,
        attempts_count => 0
    },
    ActiveTask = action_to_task(Action, ProcessId, Context),
    ProcessData0 = #{process => Process, init_task => InitTask},
    ProcessData = maybe_add_key(ActiveTask, active_task, ProcessData0),
    prg_storage:put_process_data(StorageOpts, NsId, Id, ProcessData).

do_health_check(#{ns := NsId, ns_opts := #{storage := StorageOpts}}) ->
    try prg_storage:health_check(StorageOpts) of
        ok ->
            {passing, []};
        {error, Reason} ->
            Detail = unicode:characters_to_binary(io_lib:format("~64000p", [Reason])),
            {critical, #{progressor_namespace => NsId, error => Detail}}
    catch
        Error:Reason:Stacktrace ->
            Detail = unicode:characters_to_binary(io_lib:format("~64000p", [{Error, Reason, Stacktrace}])),
            {critical, #{progressor_namespace => NsId, error => Detail}}
    end.

process_call(#{ns_opts := NsOpts, ns := NsId, type := Type, task := Task, worker := Worker, otel_ctx := OtelCtx}) ->
    otel_tracer:with_span(
        OtelCtx, opentelemetry:get_application_tracer(?MODULE), <<"call">>, #{kind => internal}, fun(SpanCtx) ->
            TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
            Timeout = TimeoutSec * 1000,
            Ref = make_ref(),
            TaskHeader = make_task_header(Type, Ref),
            ok = prg_worker:process_task(Worker, TaskHeader, Task, otel_ctx:get_current()),
            ok = prg_scheduler:release_worker(NsId, self(), Worker),
            %% TODO Maybe refactor to span inside scheduler gen_server
            _ = otel_span:add_event(SpanCtx, <<"release worker">>, #{}),
            %% see fun reply/2
            receive
                {Ref, Result} ->
                    Result
            after Timeout ->
                _ = otel_span:record_exception(SpanCtx, throw, <<"timeout">>, [], #{}),
                {error, <<"timeout">>}
            end
        end
    ).

make_task_header(init, Ref) ->
    {init, {self(), Ref}};
make_task_header(call, Ref) ->
    {call, {self(), Ref}};
make_task_header(repair, Ref) ->
    {repair, {self(), Ref}};
make_task_header(timeout, _Ref) ->
    {timeout, undefined};
make_task_header(notify, _Ref) ->
    {notify, undefined}.

convert_task_type(init) ->
    <<"init">>;
convert_task_type(call) ->
    <<"call">>;
convert_task_type(notify) ->
    <<"notify">>;
convert_task_type(timeout) ->
    <<"timeout">>;
convert_task_type(repair) ->
    <<"repair">>;
convert_task_type(Type) when is_binary(Type) ->
    Type.

maybe_add_idempotency(Task, undefined) ->
    Task;
maybe_add_idempotency(Task, IdempotencyKey) ->
    Task#{idempotency_key => IdempotencyKey}.

make_task(#{task_type := TaskType} = TaskData) when
    TaskType =:= <<"init">>;
    TaskType =:= <<"call">>;
    TaskType =:= <<"repair">>
->
    Now = erlang:system_time(second),
    Defaults = #{
        status => <<"running">>,
        scheduled_time => Now,
        running_time => Now,
        last_retry_interval => 0,
        attempts_count => 0
    },
    maps:merge(Defaults, TaskData);
make_task(#{task_type := <<"timeout">>} = TaskData) ->
    Now = erlang:system_time(second),
    Defaults = #{
        %% TODO
        metadata => #{<<"kind">> => <<"simple_repair">>},
        status => <<"waiting">>,
        scheduled_time => Now,
        last_retry_interval => 0,
        attempts_count => 0
    },
    maps:merge(Defaults, TaskData);
make_task(#{task_type := <<"notify">>} = TaskData) ->
    Now = erlang:system_time(second),
    Defaults = #{
        status => <<"running">>,
        scheduled_time => Now,
        running_time => Now,
        response => term_to_binary({ok, ok}),
        last_retry_interval => 0,
        attempts_count => 0
    },
    maps:merge(Defaults, TaskData).

capture_worker(NsId) ->
    case prg_scheduler:capture_worker(NsId, self()) of
        {ok, Worker} -> Worker;
        {error, _} -> undefined
    end.

return_worker(_NsId, undefined) ->
    ok;
return_worker(NsId, Worker) ->
    prg_scheduler:return_worker(NsId, self(), Worker).

check_for_run(undefined) ->
    <<"waiting">>;
check_for_run(Pid) when is_pid(Pid) ->
    <<"running">>.

action_to_task(undefined, _ProcessId, _Ctx) ->
    undefined;
action_to_task(unset_timer, _ProcessId, _Ctx) ->
    undefined;
action_to_task(#{set_timer := Timestamp} = Action, ProcessId, Context) ->
    TaskType =
        case maps:get(remove, Action, false) of
            true -> <<"remove">>;
            false -> <<"timeout">>
        end,
    #{
        process_id => ProcessId,
        task_type => TaskType,
        status => <<"waiting">>,
        args => <<>>,
        context => Context,
        scheduled_time => Timestamp,
        last_retry_interval => 0,
        attempts_count => 0
    }.

maybe_add_key(undefined, _Key, Map) ->
    Map;
maybe_add_key(Value, Key, Map) ->
    Map#{Key => Value}.

%options(#{options := Opts}) ->
%    Opts;
%options(_) ->
%    #{}.

%recipient(#{cache := ignore}) ->
%    internal;
%recipient(_Req) ->
%    external.
