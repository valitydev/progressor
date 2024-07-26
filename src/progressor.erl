-module(progressor).

-include("progressor.hrl").

-define(TASK_REPEAT_REQUEST_TIMEOUT, 1000).

%% Public API
-export([init/1]).
-export([call/1]).
-export([notify/1]).
-export([repair/1]).
-export([get/1]).
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
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun check_idempotency/1,
        fun add_task/1,
        fun prepare_init/1,
        fun process_call/1
    ], Req#{type => init}).

-spec call(request()) -> {ok, _Result} | {error, _Reason}.
call(Req) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun check_idempotency/1,
        fun(Opts) -> check_process_status(Opts, <<"running">>) end,
        fun add_task/1,
        fun prepare_call/1,
        fun process_call/1
    ], Req#{type => call}).

-spec notify(request()) -> {ok, _Result} | {error, _Reason}.
notify(Req) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun check_idempotency/1,
        fun(Opts) -> check_process_status(Opts, <<"running">>) end,
        fun add_task/1,
        fun prepare_call/1,
        fun process_notify/1
    ], Req#{type => notify}).

-spec repair(request()) -> {ok, _Result} | {error, _Reason}.
repair(Req) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun check_idempotency/1,
        fun(Opts) -> check_process_status(Opts, <<"error">>) end,
        fun add_task/1,
        fun prepare_repair/1,
        fun process_call/1
    ], Req#{type => repair}).

-spec get(request()) -> {ok, _Result} | {error, _Reason}.
get(Req) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun do_get_request/1
    ], Req).

%-ifdef(TEST).

-spec cleanup(_) -> _.
cleanup(Opts) ->
    prg_utils:pipe([
        fun add_ns_opts/1,
        fun cleanup_storage/1
    ], Opts).

cleanup_storage(#{ns := NsId, ns_opts := #{storage := StorageOpts}}) ->
    ok = prg_storage:cleanup(StorageOpts, NsId).

%-endif.

%% Internal functions

add_ns_opts(#{ns := NsId} = Opts) ->
    NSs = application:get_env(progressor, namespaces, #{}),
    case maps:get(NsId, NSs, undefined) of
        undefined ->
            {error, <<"namespace not found">>};
        NsOpts ->
            Opts#{ns_opts => prg_utils:make_ns_opts(NsOpts)}
    end.

check_idempotency(#{idempotency_key := _IdempotencyKey} = Req) ->
    case get_task_result(Req) of
        not_found -> Req;
        Result -> {break, Result}
    end;
check_idempotency(Req) ->
    Req.

add_task(#{id := Id, args := Args, type := Type} = Opts) ->
    Context = maps:get(context, Opts, <<>>),
    TaskData = #{
        process_id => Id,
        args => Args,
        task_type => convert_task_type(Type),
        context => Context
    },
    Task = make_task(maybe_add_idempotency(TaskData, maps:get(idempotency_key, Opts, undefined))),
    Opts#{task => Task}.

prepare_init(#{ns_opts := #{storage := StorageOpts}, ns := NsId, task := #{process_id := ProcessId} = InitTask} = Req) ->
    case prg_storage:prepare_init(StorageOpts, NsId, new_process(ProcessId), InitTask) of
        {ok, TaskId} ->
            Req#{task => InitTask#{task_id => TaskId}};
        {error, _Reason} = Error ->
            Error
    end.

check_process_status(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId} = Opts, ExpectedStatus) ->
    case prg_storage:get_process_status(StorageOpts, NsId, Id) of
        {ok, ExpectedStatus} -> Opts;
        {ok, OtherStatus} -> {error, <<"process is ", OtherStatus/binary>>};
        {error, _} = Error -> Error
    end.

prepare_call(#{ns_opts := #{storage := StorageOpts} = NsOpts, ns := NsId, id := ProcessId, task := Task} = Req) ->
    case prg_storage:prepare_call(StorageOpts, NsId, ProcessId, Task) of
        {ok, {continue, TaskId}} ->
            Req#{task => Task#{task_id => TaskId}};
        {ok, {postpone, TaskId}} ->
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

prepare_repair(#{ns_opts := #{storage := StorageOpts}, ns := NsId, id := ProcessId, task := Task} = Opts) ->
    {ok, TaskId} = prg_storage:prepare_repair(StorageOpts, NsId, ProcessId, Task),
    Opts#{task => Task#{task_id => TaskId}}.

get_task_result(#{ns_opts := #{storage := StorageOpts} = NsOpts, ns := NsId, idempotency_key := IdempotencyKey}) ->
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
        {error, in_progress} ->
            timer:sleep(?TASK_REPEAT_REQUEST_TIMEOUT),
            await_task_result(StorageOpts, NsId, KeyOrId, Timeout, Duration + ?TASK_REPEAT_REQUEST_TIMEOUT)
    end.

do_get_request(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId, args := HistoryRange}) ->
    prg_storage:get_process(StorageOpts, NsId, Id, HistoryRange);
do_get_request(#{ns_opts := #{storage := StorageOpts}, id := Id, ns := NsId}) ->
    prg_storage:get_process(StorageOpts, NsId, Id, #{}).

process_call(#{ns := NsId, ns_opts := NsOpts, type := Type, task := Task}) ->
    TimeoutSec = maps:get(process_step_timeout, NsOpts, ?DEFAULT_STEP_TIMEOUT_SEC),
    Timeout = TimeoutSec * 1000,
    Ref = make_ref(),
    TaskHeader = make_task_header(Type, Ref),
    ok = prg_scheduler:push_task(NsId, TaskHeader, Task),
    %% see fun reply/2
    receive
        {Ref, {error, {exception, Class, Term}}} ->
            erlang:raise(Class, {woody_error, {external, result_unexpected, prg_utils:format(Term)}}, []);
        {Ref, Result} ->
            Result
    after
        Timeout ->
            {error, <<"timeout">>}
    end.

process_notify(#{ns := NsId, type := Type, task := Task}) ->
    TaskHeader = make_task_header(Type, undef),
    ok = prg_scheduler:push_task(NsId, TaskHeader, Task).

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

new_process(ProcessId) ->
    #{
        process_id => ProcessId,
        status => <<"running">>
    }.
