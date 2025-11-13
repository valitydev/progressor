-module(prg_storage).

-include("progressor.hrl").

%% api handler functions
-export([health_check/1]).
-export([get_task_result/3]).
-export([get_process_status/3]).
-export([prepare_init/4]).
-export([prepare_call/4]).
-export([prepare_repair/4]).
-export([put_process_data/4]).
-export([process_trace/3]).
-export([get_process_with_initialization/4]).

%% scan functions
-export([search_timers/4]).
-export([search_calls/3]).
-export([collect_zombies/3]).

%% worker functions
-export([complete_and_continue/6]).
-export([complete_and_suspend/5]).
-export([complete_and_unlock/5]).
-export([complete_and_error/4]).
-export([remove_process/3]).

%% shared functions
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

-spec health_check(storage_opts()) -> ok | {error, Reason :: term()} | no_return().
health_check(#{client := Handler, options := HandlerOpts}) ->
    Handler:health_check(HandlerOpts).

-spec get_task_result(storage_opts(), namespace_id(), {task_id | idempotency_key, binary()}) ->
    {ok, term()} | {error, _Reason}.
get_task_result(#{client := Handler, options := HandlerOpts}, NsId, KeyOrId) ->
    Handler:get_task_result(HandlerOpts, NsId, KeyOrId).

-spec get_process_status(storage_opts(), namespace_id(), id()) -> {ok, _Result} | {error, _Reason}.
get_process_status(#{client := Handler, options := HandlerOpts}, NsId, Id) ->
    Handler:get_process_status(HandlerOpts, NsId, Id).

-spec put_process_data(
    storage_opts(),
    namespace_id(),
    id(),
    #{process := process(), init_task => task(), active_task => task()}
) ->
    {ok, _Result} | {error, _Reason}.
put_process_data(#{client := Handler, options := HandlerOpts}, NsId, Id, ProcessData) ->
    Handler:put_process_data(HandlerOpts, NsId, Id, ProcessData).

-spec process_trace(storage_opts(), namespace_id(), id()) -> {ok, _Result} | {error, _Reason}.
process_trace(#{client := Handler, options := HandlerOpts}, NsId, Id) ->
    Handler:process_trace(HandlerOpts, NsId, Id).

-spec get_process_with_initialization(storage_opts(), namespace_id(), id(), history_range()) ->
    {ok, process()} | {error, _Reason}.
get_process_with_initialization(#{client := Handler, options := HandlerOpts}, NsId, ProcessId, HistoryRange) ->
    Handler:get_process_with_initialization(HandlerOpts, NsId, ProcessId, HistoryRange).

-spec prepare_init(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_init(#{client := Handler, options := HandlerOpts}, NsId, ProcessId, InitTask) ->
    Handler:prepare_init(HandlerOpts, NsId, ProcessId, InitTask).

-spec prepare_call(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Error}.
prepare_call(#{client := Handler, options := HandlerOpts}, NsId, ProcessId, Task) ->
    Handler:prepare_call(HandlerOpts, NsId, ProcessId, Task).

-spec prepare_repair(storage_opts(), namespace_id(), id(), task()) ->
    {ok, {postpone, task_id()} | {continue, task_id()}} | {error, _Reason}.
prepare_repair(#{client := Handler, options := HandlerOpts}, NsId, ProcessId, RepairTask) ->
    Handler:prepare_repair(HandlerOpts, NsId, ProcessId, RepairTask).

%%%%%%%%%%%%%%%%%
%% Scan functions
%%%%%%%%%%%%%%%%%

-spec search_timers(storage_opts(), namespace_id(), timeout_sec(), pos_integer()) ->
    [task()] | {error, _Reason}.
search_timers(#{client := Handler, options := HandlerOpts}, NsId, Timeout, Limit) ->
    Handler:search_timers(HandlerOpts, NsId, Timeout, Limit).

-spec search_calls(storage_opts(), namespace_id(), pos_integer()) -> [task()] | {error, _Reason}.
search_calls(#{client := Handler, options := HandlerOpts}, NsId, Limit) ->
    Handler:search_calls(HandlerOpts, NsId, Limit).

-spec collect_zombies(storage_opts(), namespace_id(), timeout_sec()) -> ok.
collect_zombies(#{client := Handler, options := HandlerOpts}, NsId, Timeout) ->
    Handler:collect_zombies(HandlerOpts, NsId, Timeout).

%%%%%%%%%%%%%%%%%%%
%% Worker functions
%%%%%%%%%%%%%%%%%%%

-spec complete_and_continue(
    storage_opts(), namespace_id(), task_result(), process_updates(), [event()], task()
) ->
    {ok, [task()]}.
complete_and_continue(
    #{client := Handler, options := HandlerOpts}, NsId, TaskResult, ProcessUpdates, Events, NextTask
) ->
    Handler:complete_and_continue(HandlerOpts, NsId, TaskResult, ProcessUpdates, Events, NextTask).

-spec complete_and_suspend(storage_opts(), namespace_id(), task_result(), process_updates(), [event()]) ->
    {ok, [task()]}.
complete_and_suspend(
    #{client := Handler, options := HandlerOpts}, NsId, TaskResult, ProcessUpdates, Events
) ->
    Handler:complete_and_suspend(HandlerOpts, NsId, TaskResult, ProcessUpdates, Events).

-spec complete_and_error(storage_opts(), namespace_id(), task_result(), process_updates()) -> ok.
complete_and_error(#{client := Handler, options := HandlerOpts}, NsId, TaskResult, ProcessUpdates) ->
    Handler:complete_and_error(HandlerOpts, NsId, TaskResult, ProcessUpdates).

-spec complete_and_unlock(storage_opts(), namespace_id(), task_result(), process_updates(), [event()]) ->
    {ok, [task()]}.
complete_and_unlock(
    #{client := Handler, options := HandlerOpts}, NsId, TaskResult, ProcessUpdates, Events
) ->
    Handler:complete_and_unlock(HandlerOpts, NsId, TaskResult, ProcessUpdates, Events).

-spec remove_process(storage_opts(), namespace_id(), id()) -> ok | no_return().
remove_process(#{client := Handler, options := HandlerOpts}, NsId, ProcessId) ->
    Handler:remove_process(HandlerOpts, NsId, ProcessId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Shared functions (recipient required)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_task(storage_opts(), namespace_id(), task_id()) -> {ok, task()} | {error, _Reason}.
get_task(StorageOpts, NsId, TaskId) ->
    get_task(internal, StorageOpts, NsId, TaskId).

-spec get_task(recipient(), storage_opts(), namespace_id(), task_id()) ->
    {ok, task()} | {error, _Reason}.
get_task(Recipient, #{client := Handler, options := HandlerOpts}, NsId, TaskId) ->
    Handler:get_task(Recipient, HandlerOpts, NsId, TaskId).

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
get_process(Recipient, #{client := Handler, options := HandlerOpts}, NsId, ProcessId, HistoryRange) ->
    Handler:get_process(Recipient, HandlerOpts, NsId, ProcessId, HistoryRange).

%%%

-spec db_init(storage_opts(), namespace_id()) -> ok.
db_init(#{client := Handler, options := HandlerOpts}, NsId) ->
    Handler:db_init(HandlerOpts, NsId).

-spec cleanup(_, _) -> _.
cleanup(#{client := Handler, options := HandlerOpts}, NsId) ->
    Handler:cleanup(HandlerOpts, NsId).
