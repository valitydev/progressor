-module(prg_storage).

-include("progressor.hrl").

%% api handler functions
-export([get_task_result/3]).
-export([get_process_status/3]).
-export([prepare_init/4]).
-export([prepare_call/4]).
-export([prepare_repair/4]).

%% scan functions
-export([search_timers/4]).
-export([search_calls/3]).

%% worker functions
-export([complete_and_continue/6]).
-export([complete_and_suspend/5]).
-export([complete_and_unlock/5]).
-export([complete_and_error/4]).
-export([remove_process/3]).

%% shared functions
-export([save_task/3]).
-export([save_task/4]).
-export([get_task/3]).
-export([get_task/4]).
-export([get_process/3]).
-export([get_process/4]).
-export([get_process/5]).

%% Init operations
-export([db_init/2]).

%-ifdef(TEST).
-export([cleanup/2]).
%-endif.

%%%%%%%%%%%%%%%%%%%%%%%%
%% API handler functions
%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_task_result(storage_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, term()} | {error, _Reason}.
get_task_result(#{client := prg_pg_backend, options := PgOpts}, NsId, KeyOrId) ->
    prg_pg_backend:get_task_result(PgOpts, NsId, KeyOrId).

-spec get_process_status(storage_opts(), namespace_id(), id()) -> {ok, _Result} | {error, _Reason}.
get_process_status(#{client := prg_pg_backend, options := PgOpts}, NsId, Id) ->
    prg_pg_backend:get_process_status(PgOpts, NsId, Id).

-spec prepare_init(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_init(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, InitTask) ->
    prg_pg_backend:prepare_init(PgOpts, NsId, ?NEW_PROCESS(ProcessId), InitTask).

-spec prepare_call(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Error}.
prepare_call(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, Task) ->
    prg_pg_backend:prepare_call(PgOpts, NsId, ProcessId, Task).

-spec prepare_repair(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_repair(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, RepairTask) ->
    prg_pg_backend:prepare_repair(PgOpts, NsId, ProcessId, RepairTask).

%%%%%%%%%%%%%%%%%
%% Scan functions
%%%%%%%%%%%%%%%%%

-spec search_timers(storage_opts(), namespace_id(), timeout_sec(), pos_integer()) ->
    [task()] | {error, _Reason}.
search_timers(#{client := prg_pg_backend, options := PgOpts}, NsId, Timeout, Limit) ->
    prg_pg_backend:search_timers(PgOpts, NsId, Timeout, Limit).

-spec search_calls(storage_opts(), namespace_id(), pos_integer()) -> [task()] | {error, _Reason}.
search_calls(#{client := prg_pg_backend, options := PgOpts}, NsId, Limit) ->
    prg_pg_backend:search_calls(PgOpts, NsId, Limit).

%%%%%%%%%%%%%%%%%%%
%% Worker functions
%%%%%%%%%%%%%%%%%%%

-spec complete_and_continue(storage_opts(), namespace_id(), task_result(), process(), [event()], task()) ->
    {ok, [task()]}.
complete_and_continue(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events, NextTask) ->
    prg_pg_backend:complete_and_continue(PgOpts, NsId, TaskResult, Process, Events, NextTask).

-spec complete_and_suspend(storage_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_suspend(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events) ->
    prg_pg_backend:complete_and_suspend(PgOpts, NsId, TaskResult, Process, Events).

-spec complete_and_error(storage_opts(), namespace_id(), task_result(), process()) -> ok.
complete_and_error(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process) ->
    prg_pg_backend:complete_and_error(PgOpts, NsId, TaskResult, Process).

-spec complete_and_unlock(storage_opts(), namespace_id(), task_result(), process(), [event()]) ->
    {ok, [task()]}.
complete_and_unlock(#{client := prg_pg_backend, options := PgOpts}, NsId, TaskResult, Process, Events) ->
    prg_pg_backend:complete_and_unlock(PgOpts, NsId, TaskResult, Process, Events).

-spec remove_process(storage_opts(), namespace_id(), id()) -> ok | no_return().
remove_process(#{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId) ->
    prg_pg_backend:remove_process(PgOpts, NsId, ProcessId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Shared functions (recipient required)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_task(storage_opts(), namespace_id(), task_id()) -> {ok, task()} | {error, _Reason}.
get_task(StorageOpts, NsId, TaskId) ->
    get_task(internal, StorageOpts, NsId, TaskId).

-spec get_task(recipient(), storage_opts(), namespace_id(), task_id()) -> {ok, task()} | {error, _Reason}.
get_task(Recipient, #{client := prg_pg_backend, options := PgOpts}, NsId, TaskId) ->
    prg_pg_backend:get_task(Recipient, PgOpts, NsId, TaskId).


-spec save_task(storage_opts(), namespace_id(), task()) -> {ok, task_id()}.
save_task(StorageOpts, NsId, Task) ->
    save_task(internal, StorageOpts, NsId, Task).

-spec save_task(recipient(), storage_opts(), namespace_id(), task()) -> {ok, task_id()}.
save_task(Recipient, #{client := prg_pg_backend, options := PgOpts}, NsId, Task) ->
    prg_pg_backend:save_task(Recipient, PgOpts, NsId, Task).


-spec get_process(storage_opts(), namespace_id(), id()) -> {ok, process()} | {error, _Reason}.
get_process(StorageOpts, NsId, ProcessId) ->
    get_process(internal, StorageOpts, NsId, ProcessId, #{}).

-spec get_process(
    storage_opts() | recipient(),
    namespace_id() | storage_opts(),
    id() | namespace_id(),
    history_range() | id()
) -> {ok, process()} | {error, _Reason}.
get_process(StorageOpts, NsId, ProcessId, HistoryRange) when is_map(StorageOpts) ->
    get_process(internal, StorageOpts, NsId, ProcessId, HistoryRange);
get_process(Recipient, StorageOpts, NsId, ProcessId) when is_atom(Recipient) ->
    get_process(Recipient, StorageOpts, NsId, ProcessId, #{}).

-spec get_process(recipient(), storage_opts(), namespace_id(), id(), history_range()) ->
    {ok, process()} | {error, _Reason}.
get_process(Recipient, #{client := prg_pg_backend, options := PgOpts}, NsId, ProcessId, HistoryRange) ->
    prg_pg_backend:get_process(Recipient, PgOpts, NsId, ProcessId, HistoryRange).

%% Init operations
-spec db_init(storage_opts(), namespace_id()) -> ok.
db_init(#{client := prg_pg_backend, options := PgOpts}, NsId) ->
    prg_pg_backend:db_init(PgOpts, NsId).

%-ifdef(TEST).

-spec cleanup(_, _) -> _.
cleanup(#{client := prg_pg_backend, options := PgOpts}, NsId) ->
    prg_pg_backend:cleanup(PgOpts, NsId).

%-endif.
