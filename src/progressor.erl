-module(progressor).

-include("progressor.hrl").

-define(TASK_REPEAT_REQUEST_TIMEOUT, 1000).
-define(PREPARING_KEY, progressor_request_preparing_duration_ms).

%% Public API
-export([init/1]).
-export([call/1]).
-export([repair/1]).
-export([get/1]).
-export([put/1]).
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
    context => binary()
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

-spec get(request()) -> {ok, _Result} | {error, _Reason}.
get(Req) ->
    prg_utils:pipe(
        [
            fun add_ns_opts/1,
            fun do_get/1
        ],
        Req
    ).

-spec put(request()) -> {ok, _Result} | {error, _Reason}.
put(Req) ->
    prg_utils:pipe(
        [
            fun add_ns_opts/1,
            fun do_put/1
        ],
        Req
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

add_ns_opts(#{ns := NsId} = Opts) ->
    case prg_manager:get_namespace(NsId) of
        {error, notfound} ->
            {error, <<"namespace not found">>};
        {ok, NsOpts} ->
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
        context => Context
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
            Error
    end.

get_task_result(#{
    ns_opts := #{storage := StorageOpts} = NsOpts, ns := NsId, idempotency_key := IdempotencyKey
}) ->
    case prg_storage:get_task_result(StorageOpts, NsId, {idempotency_key, IdempotencyKey}) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            not_found;
        {error, in_progress} ->
            TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
            Timeout = TimeoutSec * 1000,
            await_task_result(StorageOpts, NsId, {idempotency_key, IdempotencyKey}, Timeout, 0)
    end.

await_task_result(_StorageOpts, _NsId, _KeyOrId, Timeout, Duration) when Duration > Timeout ->
    {error, <<"timeout">>};
await_task_result(StorageOpts, NsId, KeyOrId, Timeout, Duration) ->
    case prg_storage:get_task_result(StorageOpts, NsId, KeyOrId) of
        {ok, Result} ->
            Result;
        {error, _} ->
            timer:sleep(?TASK_REPEAT_REQUEST_TIMEOUT),
            await_task_result(
                StorageOpts, NsId, KeyOrId, Timeout, Duration + ?TASK_REPEAT_REQUEST_TIMEOUT
            )
    end.

do_get(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId, args := HistoryRange}) ->
    prg_storage:get_process(external, StorageOpts, NsId, Id, HistoryRange);
do_get(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId}) ->
    prg_storage:get_process(external, StorageOpts, NsId, Id, #{}).

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

process_call(#{ns_opts := NsOpts, ns := NsId, type := Type, task := Task, worker := Worker}) ->
    TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
    Timeout = TimeoutSec * 1000,
    Ref = make_ref(),
    TaskHeader = make_task_header(Type, Ref),
    ok = prg_worker:process_task(Worker, TaskHeader, Task),
    ok = prg_scheduler:release_worker(NsId, self(), Worker),
    %% see fun reply/2
    receive
        {Ref, Result} ->
            Result
    after Timeout ->
        {error, <<"timeout">>}
    end.

make_task_header(init, Ref) ->
    {init, {self(), Ref}};
make_task_header(call, Ref) ->
    {call, {self(), Ref}};
make_task_header(repair, Ref) ->
    {repair, {self(), Ref}};
make_task_header(notify, _Ref) ->
    {notify, undefined}.

convert_task_type(init) ->
    <<"init">>;
convert_task_type(call) ->
    <<"call">>;
convert_task_type(notify) ->
    <<"notify">>;
convert_task_type(repair) ->
    <<"repair">>.

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
%%

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
