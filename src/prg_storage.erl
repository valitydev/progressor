-module(prg_storage).

-include("progressor.hrl").

%% Task management
-export([get_task_result/3]).
-export([search_tasks/4]).
-export([save_task/3]).
-export([search_postponed_calls/3]).

%% Process management
-export([get_process_status/3]).
-export([get_process/3]).
-export([get_process/4]).

%% Complex operations
-export([prepare_init/4]).
-export([prepare_call/4]).
%% TODO must be retryable
-export([complete_and_continue/6]).
-export([complete_and_suspend/5]).
-export([complete_and_unlock/5]).

%% Init operations
-export([db_init/2]).

%-ifdef(TEST).
-export([cleanup/2]).
%-endif.

%% Task management
-spec get_task_result(storage_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, binary()} | {error, _Reason}.
get_task_result(#{client := prg_pg_backend, options := PgOpts}, NsId, KeyOrId) ->
    prg_pg_backend:get_task_result(PgOpts, NsId, KeyOrId).

-spec save_task(storage_opts(), namespace_id(), task()) -> {ok, task_id()}.
save_task(#{client := prg_pg_backend, options := PgOpts}, NsId, Task) ->
    prg_pg_backend:save_task(PgOpts, NsId, Task).

-spec search_postponed_calls(storage_opts(), namespace_id(), id()) -> {ok, task()} | {error, not_found}.
search_postponed_calls(#{client := prg_pg_backend, options := PgOpts}, NsId, Id) ->
    prg_pg_backend:search_postponed_calls(PgOpts, NsId, Id).

%% Process management
-spec get_process_status(storage_opts(), namespace_id(), id()) -> {ok, _Result} | {error, _Reason}.
get_process_status(#{client := prg_pg_backend, options := PgOpts}, NsId, Id) ->
    prg_pg_backend:get_process_status(PgOpts, NsId, Id).

-spec get_process(storage_opts(), namespace_id(), id()) -> {ok, process()} | {error, _Reason}.
get_process(StorageOpts, NsId, ProcessId) ->
    get_process(StorageOpts, NsId, ProcessId, #{}).

-spec get_process(storage_opts(), namespace_id(), id(), history_range()) -> {ok, process()} | {error, _Reason}.
get_process(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, HistoryRange) ->
    prg_pg_backend:get_process(PgOpts, NsId, ProcessId, HistoryRange).

%% Complex operations
-spec search_tasks(storage_opts(), namespace_id(), timeout_sec(), pos_integer()) -> [task()].
search_tasks(#{client := prg_pg_backend, options := PgOpts}, NsId, Timeout, Limit) ->
    prg_pg_backend:search_tasks(PgOpts, NsId, Timeout, Limit).

-spec prepare_init(storage_opts(), namespace_id(), process(), task()) -> {ok, task_id()} | {error, _Reason}.
prepare_init(#{client := prg_pg_backend, options := PgOpts}, NsId, Process, InitTask) ->
    prg_pg_backend:prepare_init(PgOpts, NsId, Process, InitTask).

-spec prepare_call(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Error}.
prepare_call(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, Task) ->
    prg_pg_backend:prepare_call(PgOpts, NsId, ProcessId, Task).

-spec complete_and_continue(storage_opts(), namespace_id(), task_result(), process(), [event()], task()) ->
    {ok, task_id()}.
complete_and_continue(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events, NextTask) ->
    prg_pg_backend:complete_and_continue(PgOpts, NsId, TaskResult, Process, Events, NextTask).

-spec complete_and_suspend(storage_opts(), namespace_id(), task_result(), process(), [event()]) -> ok.
complete_and_suspend(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events) ->
    prg_pg_backend:complete_and_suspend(PgOpts, NsId, TaskResult, Process, Events).

-spec complete_and_unlock(storage_opts(), namespace_id(), task_result(), process(), [event()]) -> ok.
complete_and_unlock(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events) ->
    prg_pg_backend:complete_and_unlock(PgOpts, NsId, TaskResult, Process, Events).

%% Init operations
-spec db_init(storage_opts(), namespace_id()) -> ok.
db_init(#{client := prg_pg_backend, options := PgOpts}, NsId) ->
    prg_pg_backend:db_init(PgOpts, NsId).

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(#{client := prg_pg_backend, options := PgOpts}, NsId) ->
    prg_pg_backend:cleanup(PgOpts, NsId).

%-endif.
