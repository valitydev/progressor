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

-spec get(namespace_id(), id(), generation()) -> {ok, _Result} | undefined.
get(NsID, ProcessID, Generation) ->
    do_get(NsID, ProcessID, Generation).

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
        %% NOTE transaction as stack (first operation -> last element)
        %% need check
        lists:reverse(ReplData)
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
            ReconnectTimeout = application:get_env(progressor, cache_reconnect_timeout, ?DEFAULT_RECONNECT_TIMEOUT),
            erlang:start_timer(ReconnectTimeout, self(), restart_replication),
            {noreply, State}
    end;
handle_info({timeout, _TRef, {cleanup_process, ProcessID, Tables}}, #{timers := Timers} = State) ->
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
        generations := GensTable
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
                    " FOR TABLE " ++ ProcessesTable ++ " , " ++ GensTable
            ),
            {ok, PubName}
    end.

create_tables(NsIDs) ->
    Tables = lists:foldl(
        fun(NsID, Acc) ->
            #{
                processes := ProcessesTable,
                generations := GensTable
            } = tables(NsID),
            ProcessesETS = ets:new(ProcessesTable, [named_table]),
            GensETS = ets:new(GensTable, [named_table, bag]),
            [ProcessesETS, GensETS | Acc]
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

do_get(NsID, ProcessID, Generation) ->
    #{
        processes := ProcessesTable,
        generations := GensTable
    } = tables(NsID),
    case ets:lookup(ProcessesTable, ProcessID) of
        [] ->
            undefined;
        [{_, ProcessRaw}] ->
            case get_process_state(ets:lookup(GensTable, ProcessID), Generation) of
                undefined ->
                    undefined;
                ProcessStateRaw ->
                    Process = prg_pg_utils:marshal_process(ProcessRaw),
                    ProcessState = prg_pg_utils:marshal_process_state(convert_state(ProcessStateRaw)),
                    {ok, Process#{state => ProcessState}}
            end
    end.

process_operation({Table, _, _} = ReplData, State) ->
    [NsBin, Object] = string:split(Table, <<"_">>, trailing),
    process_operation(Object, binary_to_atom(NsBin), ReplData, State).

process_operation(<<"processes">>, NsID, {_Table, insert, #{<<"process_id">> := ProcessID} = Row}, State) ->
    %% process created
    #{
        processes := ProcessesTable,
        generations := GensTable
    } = tables(NsID),
    true = ets:insert(ProcessesTable, {ProcessID, Row}),
    reset_timer([ProcessesTable, GensTable], ProcessID, State);
process_operation(<<"generations">>, NsID, {_Table, insert, #{<<"process_id">> := ProcessID} = Row}, State) ->
    #{
        processes := ProcessesTable,
        generations := GensTable
    } = tables(NsID),
    case ets:lookup(ProcessesTable, ProcessID) of
        [_Process] ->
            %% known cached process, save next generation state
            true = ets:insert(GensTable, {ProcessID, Row}),
            reset_timer([ProcessesTable, GensTable], ProcessID, State);
        [] ->
            %% old process, not cached, ignore
            State
    end;
%% update operation is not applicable for generations table (append only)
process_operation(<<"processes">>, NsID, {_Table, update, #{<<"process_id">> := ProcessID} = Row}, State) ->
    #{
        processes := ProcessesTable,
        generations := GensTable
    } = tables(NsID),
    case ets:lookup(ProcessesTable, ProcessID) of
        [{_, OldRow}] ->
            true = ets:insert(ProcessesTable, {ProcessID, maps:merge(OldRow, Row)}),
            reset_timer([ProcessesTable, GensTable], ProcessID, State);
        [] ->
            State
    end;
process_operation(_Object, _NsID, {Table, delete, #{<<"process_id">> := ProcessID}}, State) ->
    TableETS = binary_to_atom(Table),
    true = ets:delete(TableETS, ProcessID),
    State;
process_operation(_Object, _NsID, ReplData, State) ->
    logger:warning("Unsupported cache operation: ~p", [ReplData]),
    State.

reset_timer(Tables, ProcessID, #{timers := Timers} = State) when is_map_key(ProcessID, Timers) ->
    TRef = maps:get(ProcessID, Timers),
    _ = erlang:cancel_timer(TRef),
    CleanupTimeout = application:get_env(progressor, cache_cleanup_timeout, ?DEFAULT_CLEANUP_TIMEOUT),
    Msg = {cleanup_process, ProcessID, Tables},
    NewTRef = erlang:start_timer(CleanupTimeout, self(), Msg),
    State#{timers => Timers#{ProcessID => NewTRef}};
reset_timer(Tables, ProcessID, #{timers := Timers} = State) ->
    CleanupTimeout = application:get_env(progressor, cache_cleanup_timeout, ?DEFAULT_CLEANUP_TIMEOUT),
    Msg = {cleanup_process, ProcessID, Tables},
    TRef = erlang:start_timer(CleanupTimeout, self(), Msg),
    State#{timers => Timers#{ProcessID => TRef}}.

tables(NsID) ->
    NsStr = atom_to_list(NsID),
    #{
        processes => list_to_atom(NsStr ++ "_processes"),
        generations => list_to_atom(NsStr ++ "_generations")
    }.

get_process_state([], _Generation) ->
    undefined;
get_process_state(Generations, latest) ->
    [{_, ProcessState} | _] = lists:sort(
        fun({_, #{<<"generation">> := GenA}}, {_, #{<<"generation">> := GenB}}) -> GenA > GenB end,
        Generations
    ),
    ProcessState;
get_process_state(Generations, Gen) ->
    Result = lists:search(fun({_, #{<<"generation">> := G}}) -> G =:= Gen end, Generations),
    case Result of
        {value, {_, ProcessState}} ->
            ProcessState;
        false ->
            undefined
    end.

convert_state(#{<<"timestamp">> := null} = State) ->
    State;
convert_state(#{<<"timestamp">> := DateTime} = State) ->
    State#{<<"timestamp">> => prg_pg_utils:convert(timestamp, DateTime)};
convert_state(State) ->
    State.
