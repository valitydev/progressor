-module(prg_pg_cache).

-include_lib("progressor/include/progressor.hrl").

-behaviour(gen_server).

%% cache API
-export([start/1]).
-export([get/3]).
-export([cache_ref/1]).

%% gen_server callbacks
-export([start_link/4]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% wal_reader callbacks
-export([handle_replication_data/2]).
-export([handle_replication_stop/2]).

-type replication_spec() :: #{
    DbReference :: atom() := {[namespace_id()], AppName :: string()}
}.

-export_type([replication_spec/0]).

-define(DEFAULT_RECONNECT_TIMEOUT, 5000).
%% 5 min
-define(DEFAULT_CLEANUP_TIMEOUT, 300000).

-spec handle_replication_data(_, _) -> ok.
handle_replication_data(Pid, ReplData) ->
    gen_server:cast(Pid, {handle_replication_data, ReplData}).

-spec handle_replication_stop(_, _) -> ok.
handle_replication_stop(Pid, ReplStop) ->
    gen_server:cast(Pid, {handle_replication_stop, ReplStop}).

-spec cache_ref(DbRef :: atom()) -> CacheRef :: atom().
cache_ref(DbRef) ->
    list_to_atom("pg_cache_" ++ atom_to_list(DbRef)).

-spec start(replication_spec()) -> ok.
start(Replications) ->
    {ok, DBs} = application:get_env(epg_connector, databases),
    maps:fold(
        fun(DbRef, {NsIDs, AppName}, _Acc) ->
            DbOpts = maps:get(DbRef, DBs),
            ReplSlot = generate_slot_id(AppName),
            ReplSpec = create_repl_spec(DbRef, DbOpts, ReplSlot, NsIDs),
            {ok, _} = supervisor:start_child(progressor_sup, ReplSpec),
            ok
        end,
        ok,
        Replications
    ).

-spec get(namespace_id(), id(), history_range()) -> {ok, _Result} | undefined.
get(NsID, ProcessID, HistoryRange) ->
    do_get(NsID, ProcessID, HistoryRange).

-spec start_link(
    CacheRef :: atom(),
    DbOpts :: map(),
    ReplSlot :: string(),
    NsIDs :: [string()]
) -> gen_server:start_ret().
start_link(CacheRef, DbOpts, ReplSlot, NsIDs) ->
    gen_server:start_link({local, CacheRef}, ?MODULE, [DbOpts, ReplSlot, NsIDs], []).

-spec init(_Args) -> {ok, _State}.
init([DbOpts, ReplSlot, NsIDs]) ->
    {ok, Connection} = epgsql:connect(DbOpts),
    Publications = lists:foldl(
        fun(NsID, Acc) ->
            {ok, PubName} = create_publication_if_not_exists(Connection, NsID),
            [PubName | Acc]
        end,
        [],
        NsIDs
    ),
    ok = epgsql:close(Connection),
    {ok, Tables} = create_tables(NsIDs),
    {ok, Reader} = epg_wal_reader:subscription_create({?MODULE, self()}, DbOpts, ReplSlot, Publications),
    {ok, #{
        db_opts => DbOpts,
        repl_slot => ReplSlot,
        publications => Publications,
        tables => Tables,
        wal_reader => Reader,
        timers => #{}
    }}.

-spec handle_call(_Request, _From, _State) -> {reply, _Reply, _State}.
handle_call(get_reader, _From, #{wal_reader := Reader} = State) ->
    {reply, Reader, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

-spec handle_cast(_Request, _State) -> {noreply, _State}.
handle_cast({handle_replication_data, ReplData}, State) ->
    NewState = lists:foldl(
        fun(ReplUnit, Acc) -> process_operation(ReplUnit, Acc) end,
        State,
        ReplData
    ),
    {noreply, NewState};
handle_cast({handle_replication_stop, _ReplStop}, State) ->
    ok = cleanup_tables(State),
    ReconnectTimeout = application:get_env(progressor, cache_reconnect_timeout, ?DEFAULT_RECONNECT_TIMEOUT),
    erlang:start_timer(ReconnectTimeout, self(), restart_replication),
    {noreply, maps:without([wal_reader], State)};
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(_Info, _State) -> {noreply, _State}.
handle_info({timeout, _TRef, restart_replication}, State) ->
    #{
        db_opts := DbOpts,
        repl_slot := ReplSlot,
        publications := Publications
    } = State,
    maybe
        {ok, Reader} ?= epg_wal_reader:subscription_create({?MODULE, self()}, DbOpts, ReplSlot, Publications),
        {noreply, State#{wal_reader => Reader}}
    else
        Error ->
            logger:error("Can`t restart replication with error: ~p", [Error]),
            ReconnectTimeout = application:get_env(progressor, cache_reconnect_timeout, 5000),
            erlang:start_timer(ReconnectTimeout, self(), restart_replication),
            {noreply, State}
    end;
handle_info({timeout, _TRef, {cleanup_process, ProcessID}}, #{timers := Timers, tables := Tables} = State) ->
    lists:foreach(
        fun(T) -> ets:delete(T, ProcessID) end,
        Tables
    ),
    NewState = State#{timers => maps:without([ProcessID], Timers)},
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_Reason, _State) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_OldVsn, _State, _Extra) -> {ok, _State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

generate_slot_id(AppName) ->
    Postfix = binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(8))),
    AppName ++ "_" ++ Postfix.

create_repl_spec(DbRef, DbOpts, ReplSlot, NsIDs) ->
    CacheID = "cache_" ++ ReplSlot,
    CacheRef = cache_ref(DbRef),
    #{
        id => CacheID,
        start => {?MODULE, start_link, [CacheRef, DbOpts, ReplSlot, NsIDs]}
    }.

create_publication_if_not_exists(Connection, NsID) ->
    PubName = erlang:atom_to_list(NsID),
    PubNameEscaped = "\"" ++ PubName ++ "\"",
    #{
        processes := ProcessesTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsID),
    %% TODO must be transaction (race condition)
    {ok, _, [{IsPublicationExists}]} = epgsql:equery(
        Connection,
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        [PubName]
    ),
    case IsPublicationExists of
        true ->
            {ok, PubName};
        false ->
            {ok, _, _} = epgsql:equery(
                Connection,
                "CREATE PUBLICATION " ++ PubNameEscaped ++
                    " FOR TABLE " ++ ProcessesTable ++ " , " ++ EventsTable
            ),
            {ok, PubName}
    end.

create_tables(NsIDs) ->
    Tables = lists:foldl(
        fun(NsID, Acc) ->
            #{
                processes := ProcessesTable,
                events := EventsTable
            } = tables(NsID),
            ProcessesETS = ets:new(ProcessesTable, [named_table]),
            EventsETS = ets:new(EventsTable, [named_table, bag]),
            [ProcessesETS, EventsETS | Acc]
        end,
        [],
        NsIDs
    ),
    {ok, Tables}.

cleanup_tables(#{tables := Tables}) ->
    lists:foreach(
        fun(T) -> ets:delete_all_objects(T) end,
        Tables
    );
cleanup_tables(_) ->
    ok.

do_get(NsID, ProcessID, HistoryRange) ->
    #{
        processes := ProcessesTable,
        events := EventsTable
    } = tables(NsID),
    ProcessResult = ets:lookup(ProcessesTable, ProcessID),
    case ProcessResult of
        [] ->
            undefined;
        [{_, ProcessRaw}] ->
            Process = prg_pg_utils:marshal_process(ProcessRaw),
            {EventsRaw, LastEventID} = process_by_range(ets:lookup(EventsTable, ProcessID), HistoryRange),
            Events = lists:map(
                fun(Ev) -> prg_pg_utils:marshal_event(convert_event(Ev)) end,
                EventsRaw
            ),
            {ok, Process#{history => Events, last_event_id => LastEventID, range => HistoryRange}}
    end.

process_operation({Table, insert, #{<<"process_id">> := ProcessID} = Row}, State) ->
    TableETS = binary_to_atom(Table),
    true = ets:insert(TableETS, {ProcessID, Row}),
    reset_timer(ProcessID, State);
process_operation({Table, update, #{<<"process_id">> := ProcessID} = Row}, State) ->
    TableETS = binary_to_atom(Table),
    case ets:lookup(TableETS, ProcessID) of
        [{_, OldRow}] ->
            true = ets:insert(TableETS, {ProcessID, maps:merge(OldRow, Row)});
        [] ->
            ets:insert(TableETS, {ProcessID, Row})
    end,
    reset_timer(ProcessID, State);
process_operation({Table, delete, #{<<"process_id">> := ProcessID}}, State) ->
    TableETS = binary_to_atom(Table),
    true = ets:delete(TableETS, ProcessID),
    State;
process_operation(ReplData, State) ->
    logger:warning("Unsupperted cache operation: ~p", [ReplData]),
    State.

reset_timer(ProcessID, #{timers := Timers} = State) when is_map_key(ProcessID, Timers) ->
    TRef = maps:get(ProcessID, Timers),
    _ = erlang:cancel_timer(TRef),
    CleanupTimeout = application:get_env(progressor, cache_cleanup_timeout, ?DEFAULT_CLEANUP_TIMEOUT),
    NewTRef = erlang:start_timer(CleanupTimeout, self(), {cleanup_process, ProcessID}),
    State#{timers => Timers#{ProcessID => NewTRef}};
reset_timer(ProcessID, #{timers := Timers} = State) ->
    CleanupTimeout = application:get_env(progressor, cache_cleanup_timeout, ?DEFAULT_CLEANUP_TIMEOUT),
    TRef = erlang:start_timer(CleanupTimeout, self(), {cleanup_process, ProcessID}),
    State#{timers => Timers#{ProcessID => TRef}}.

tables(NsID) ->
    NsStr = atom_to_list(NsID),
    #{
        processes => list_to_atom(NsStr ++ "_processes"),
        events => list_to_atom(NsStr ++ "_events")
    }.

process_by_range([], _) ->
    {[], 0};
process_by_range(Events, #{direction := backward, offset := After} = Range) ->
    [{_, #{<<"event_id">> := LastEventID}} | _] =
        Reversed = lists:sort(
            fun({_, #{<<"event_id">> := EventID1}}, {_, #{<<"event_id">> := EventID2}}) -> EventID1 > EventID2 end,
            Events
        ),
    Limit = maps:get(limit, Range, erlang:length(Events)),
    Filtered = lists:filtermap(
        fun
            ({_, #{<<"event_id">> := EvID} = Ev}) when EvID < After -> {true, Ev};
            (_) -> false
        end,
        Reversed
    ),
    {lists:sublist(Filtered, Limit), LastEventID};
process_by_range(Events, #{direction := backward} = Range) ->
    [{_, #{<<"event_id">> := LastEventID}} | _] =
        Reversed = lists:sort(
            fun({_, #{<<"event_id">> := EventID1}}, {_, #{<<"event_id">> := EventID2}}) -> EventID1 > EventID2 end,
            Events
        ),
    Limit = maps:get(limit, Range, erlang:length(Events)),
    History = lists:map(
        fun({_, Ev}) -> Ev end,
        lists:sublist(Reversed, Limit)
    ),
    {History, LastEventID};
process_by_range(Events, #{offset := After} = Range) ->
    [{_, #{<<"event_id">> := LastEventID}} | _] =
        Sorted = lists:sort(
            fun({_, #{<<"event_id">> := EventID1}}, {_, #{<<"event_id">> := EventID2}}) -> EventID1 < EventID2 end,
            Events
        ),
    Limit = maps:get(limit, Range, erlang:length(Events)),
    Filtered = lists:filtermap(
        fun
            ({_, #{<<"event_id">> := EvID} = Ev}) when EvID > After -> {true, Ev};
            (_) -> false
        end,
        Sorted
    ),
    {lists:sublist(Filtered, Limit), LastEventID};
process_by_range(Events, _Range) ->
    [{_, #{<<"event_id">> := LastEventID}} | _] =
        Sorted = lists:sort(
            fun({_, #{<<"event_id">> := EventID1}}, {_, #{<<"event_id">> := EventID2}}) -> EventID1 < EventID2 end,
            Events
        ),
    History = lists:map(
        fun({_, Ev}) -> Ev end,
        Sorted
    ),
    {History, LastEventID}.

convert_event(#{<<"timestamp">> := null} = Event) ->
    Event;
convert_event(#{<<"timestamp">> := DateTime} = Event) ->
    Event#{<<"timestamp">> => prg_pg_utils:convert(timestamp, DateTime)};
convert_event(Event) ->
    Event.
