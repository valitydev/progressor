-module(prg_lookup_SUITE).

-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    lookups_test/1,
    updates_test/1,
    invalid_intents_test/1,
    invalid_init_test/1,
    constraints_test/1,
    init_without_events_test/1,
    arguments_test/1,
    metrics_test/1,
    cache_test/1
]).

-define(NS, 'default/default').
-define(CACHED_NS, 'cached/namespace').
-define(INVALID_FIELDS, {error, {exception, error, {invalid_processor_fields, invalid_lookup_fields}}}).
%% NUL byte, invalid UTF-8 ({error, ...}) and truncated UTF-8 sequence ({incomplete, ...})
-define(INVALID_TEXT_VALUES, [<<"a", 0, "b">>, <<0>>, <<"a", 255>>, <<"a", 16#D0>>]).

all() ->
    [
        lookups_test,
        updates_test,
        invalid_intents_test,
        invalid_init_test,
        constraints_test,
        init_without_events_test,
        arguments_test,
        metrics_test,
        cache_test
    ].

init_per_suite(Config) ->
    _ = prg_ct_hook:start_applications(),
    %% the processor applies lookup fields passed in args and always emits the next event
    ok = meck:new(prg_ct_processor, [no_link]),
    ok = meck:expect(prg_ct_processor, process, fun({_Type, Args, #{history := History}}, _Opts, _Ctx) ->
        Fields = binary_to_term(Args),
        {ok, Fields#{events => [event(length(History) + 1)]}}
    end),
    Config.

end_per_suite(_Config) ->
    ok = meck:unload(prg_ct_processor),
    prg_ct_hook:stop_applications().

-spec lookups_test(_) -> _.
lookups_test(_C) ->
    Fields = #{subnamespace => <<"Owner">>, sub_id => <<"same">>, aliases => [<<"shared">>, <<"shared">>, <<"%_">>]},
    A = create(?NS, Fields),
    B = create(?NS, Fields),
    _ = create(?NS, Fields#{subnamespace => <<"owner">>, aliases => []}),
    {ok, ok} = update(?NS, A, #{metadata => #{<<"step">> => 2}}),
    {ok, Matches} = progressor:get_by_sub_id(?NS, <<"Owner">>, <<"same">>),
    ?assertEqual(lists:sort([A, B]), ids(Matches)),
    {ok, AliasMatches} = progressor:get_by_alias(?NS, <<"shared">>),
    ?assertEqual(ids(Matches), ids(AliasMatches)),
    ProcessA = find(A, Matches),
    ProcessB = find(B, Matches),
    %% attributes and full history
    ?assertMatch(
        #{
            status := <<"running">>,
            subnamespace := <<"Owner">>,
            sub_id := <<"same">>,
            aliases := [<<"%_">>, <<"shared">>],
            metadata := #{<<"step">> := 2},
            last_event_id := 2,
            history := [#{event_id := 1, payload := _, timestamp := _}, #{event_id := 2}]
        },
        ProcessA
    ),
    ?assertMatch(#{last_event_id := 1, history := [#{event_id := 1}]}, ProcessB),
    %% same content as direct reading
    {ok, DirectA} = get(?NS, A),
    ?assertEqual(DirectA, ProcessA),
    %% exact matching: no wildcards, case sensitive
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"%">>)),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"SHARED">>)),
    ?assertMatch({ok, [_, _]}, progressor:get_by_alias(?NS, <<"%_">>)),
    ?assertEqual({ok, []}, progressor:get_by_sub_id(?NS, <<"Owner">>, <<"missing">>)),
    %% namespaces are isolated
    ?assertEqual({ok, []}, progressor:get_by_alias(?CACHED_NS, <<"shared">>)).

-spec updates_test(_) -> _.
updates_test(_C) ->
    Id = create(?NS, #{subnamespace => <<"updates">>, sub_id => <<"id">>, aliases => [<<"old">>]}),
    %% absent fields are kept
    {ok, ok} = update(?NS, Id, #{}),
    ?assertMatch({ok, [#{sub_id := <<"id">>}]}, progressor:get_by_alias(?NS, <<"old">>)),
    %% undefined clears scalar, list replaces aliases
    {ok, ok} = update(?NS, Id, #{sub_id => undefined, aliases => [<<"new">>, <<"new">>]}),
    {ok, [P]} = progressor:get_by_alias(?NS, <<"new">>),
    ?assertNot(maps:is_key(sub_id, P)),
    ?assertEqual(<<"updates">>, maps:get(subnamespace, P)),
    ?assertEqual([<<"new">>], maps:get(aliases, P)),
    ?assertEqual({ok, []}, progressor:get_by_alias(?NS, <<"old">>)),
    %% [] clears aliases
    {ok, ok} = update(?NS, Id, #{subnamespace => undefined, aliases => []}),
    {ok, Cleared} = get(?NS, Id),
    ?assertNot(maps:is_key(subnamespace, Cleared)),
    ?assertEqual([], maps:get(aliases, Cleared)),
    {ok, ok} = update(?NS, Id, #{subnamespace => <<"again">>, sub_id => <<"id">>}),
    ?assertMatch({ok, [#{process_id := Id}]}, progressor:get_by_sub_id(?NS, <<"again">>, <<"id">>)),
    {ok, ok} = update(?NS, Id, #{subnamespace => undefined, sub_id => undefined}),
    ?assertEqual({ok, []}, progressor:get_by_sub_id(?NS, <<"again">>, <<"id">>)),
    ?assertMatch({ok, #{last_event_id := 6}}, get(?NS, Id)).

-spec invalid_intents_test(_) -> _.
invalid_intents_test(_C) ->
    Invalid =
        [
            %% resulting combination is checked: sub_id remains without subnamespace
            #{subnamespace => undefined},
            #{subnamespace => <<>>},
            #{sub_id => <<>>},
            #{sub_id => null},
            #{subnamespace => 1},
            #{subnamespace => "text"},
            #{aliases => undefined},
            #{aliases => <<"x">>},
            #{aliases => [null]},
            #{aliases => [<<>>]},
            #{aliases => [[1]]}
        ] ++
            lists:flatmap(
                fun(Value) ->
                    [#{subnamespace => Value}, #{sub_id => Value}, #{aliases => [<<"valid">>, Value]}]
                end,
                ?INVALID_TEXT_VALUES
            ),
    lists:foreach(
        fun(Fields) ->
            Id = create(?NS, #{subnamespace => <<"valid">>, sub_id => <<"id">>, aliases => [<<"valid">>]}),
            ?assertEqual(?INVALID_FIELDS, update(?NS, Id, Fields), Fields),
            %% nothing is saved, process is broken
            {ok, P} = get(?NS, Id),
            ?assertMatch(
                #{
                    status := <<"error">>,
                    subnamespace := <<"valid">>,
                    sub_id := <<"id">>,
                    aliases := [<<"valid">>],
                    last_event_id := 1
                },
                P,
                Fields
            )
        end,
        Invalid
    ).

-spec invalid_init_test(_) -> _.
invalid_init_test(_C) ->
    lists:foreach(
        fun(Fields) ->
            Id = gen_id(),
            ?assertEqual(?INVALID_FIELDS, init(?NS, Id, Fields)),
            ?assertMatch({ok, #{status := <<"error">>, aliases := [], history := []}}, get(?NS, Id))
        end,
        [#{sub_id => <<"orphan">>}, #{aliases => [<<"bad", 0>>]}, #{subnamespace => <<255>>}]
    ).

-spec constraints_test(_) -> _.
constraints_test(_C) ->
    Id = create(?NS, #{}),
    #{processes := Table} = prg_pg_utils:tables(?NS),
    lists:foreach(
        fun(Set) ->
            ?assertMatch(
                {error, _},
                epg_pool:query(default_pool, "UPDATE " ++ Table ++ " SET " ++ Set ++ " WHERE process_id = $1", [Id]),
                Set
            )
        end,
        [
            "subnamespace = ''",
            "sub_id = ''",
            "sub_id = 'orphan'",
            "aliases = NULL",
            "aliases = ARRAY['']::text[]",
            "aliases = ARRAY[NULL]::text[]"
        ]
    ).

-spec init_without_events_test(_) -> _.
init_without_events_test(_C) ->
    Id = gen_id(),
    #{processes := Table} = prg_pg_utils:tables(?NS),
    {ok, 1} = epg_pool:query(
        default_pool,
        "INSERT INTO " ++ Table ++
            " (process_id, status, previous_status, status_changed_at, subnamespace, sub_id, aliases)"
            " VALUES ($1, 'init', 'init', now(), 'pending', $2, ARRAY['pending'])",
        [Id, Id]
    ),
    {ok, [P]} = progressor:get_by_alias(?NS, <<"pending">>),
    ?assertMatch(#{process_id := Id, status := <<"init">>, history := [], last_event_id := 0}, P),
    ?assertEqual({ok, [P]}, progressor:get_by_sub_id(?NS, <<"pending">>, Id)).

-spec arguments_test(_) -> _.
arguments_test(_C) ->
    lists:foreach(
        fun(Value) ->
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_alias(?NS, Value)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(?NS, Value, <<"id">>)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(?NS, <<"ns">>, Value))
        end,
        [<<>>, undefined, null, "text", 1 | ?INVALID_TEXT_VALUES]
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_alias(Ns, <<"a">>)),
            ?assertEqual({error, invalid_lookup_arguments}, progressor:get_by_sub_id(Ns, <<"ns">>, <<"id">>))
        end,
        [<<"default/default">>, "default/default", 1, #{}, []]
    ),
    ?assertEqual({error, <<"namespace not found">>}, progressor:get_by_alias(unknown_namespace, <<"a">>)),
    ?assertEqual(
        {error, <<"namespace not found">>},
        progressor:get_by_sub_id(unknown_namespace, <<"ns">>, <<"id">>)
    ).

-spec metrics_test(_) -> _.
metrics_test(_C) ->
    Labels = [atom_to_binary(?NS)],
    SubIdBefore = observations(progressor_get_by_sub_id_duration_ms, Labels),
    AliasBefore = observations(progressor_get_by_alias_duration_ms, Labels),
    {ok, _} = progressor:get_by_sub_id(?NS, <<"metrics">>, <<"id">>),
    {ok, _} = progressor:get_by_alias(?NS, <<"metrics">>),
    {ok, _} = progressor:get_by_alias(?NS, <<"metrics">>),
    %% rejected arguments do not reach storage and are not observed
    {error, invalid_lookup_arguments} = progressor:get_by_alias(?NS, <<>>),
    ?assertEqual(SubIdBefore + 1, observations(progressor_get_by_sub_id_duration_ms, Labels)),
    ?assertEqual(AliasBefore + 2, observations(progressor_get_by_alias_duration_ms, Labels)).

-spec cache_test(_) -> _.
cache_test(_C) ->
    Aliases = [
        <<"cached">>,
        <<"comma,value">>,
        <<"quote\"value">>,
        <<"back\\slash">>,
        <<"NULL">>,
        <<"NULLfoo">>,
        <<"{braces}">>,
        <<" space ">>,
        <<"юникод"/utf8>>
    ],
    Id = create(?CACHED_NS, #{subnamespace => <<"cache">>, sub_id => <<"id">>, aliases => Aliases}),
    %% process with aliases is cached, aliases are excluded from replicated row and from result
    Cached = await_cache(Id, 1),
    ?assertNot(maps:is_key(aliases, Cached)),
    ?assertMatch(#{subnamespace := <<"cache">>, sub_id := <<"id">>, history := [#{event_id := 1}]}, Cached),
    ?assertNot(maps:is_key(<<"aliases">>, cached_row(Id))),
    ?assertEqual({ok, Cached}, progressor:get(#{ns => ?CACHED_NS, id => Id})),
    %% real aliases are returned from PostgreSQL
    {ok, Direct} = get(?CACHED_NS, Id),
    ?assertEqual(lists:sort(Aliases), maps:get(aliases, Direct)),
    ?assertMatch(#{subnamespace := <<"cache">>, sub_id := <<"id">>, last_event_id := 1}, Direct),
    {ok, [Found]} = progressor:get_by_alias(?CACHED_NS, <<"NULLfoo">>),
    ?assertEqual(lists:sort(Aliases), maps:get(aliases, Found)),
    ?assertMatch({ok, [#{process_id := Id}]}, progressor:get_by_alias(?CACHED_NS, <<"юникод"/utf8>>)),
    %% updates of other fields are replicated, aliases are still excluded
    {ok, ok} = update(?CACHED_NS, Id, #{subnamespace => <<"cache2">>, aliases => [<<"NULLfoo">>]}),
    Updated = await_cache(Id, 2),
    ?assertMatch(#{subnamespace := <<"cache2">>, sub_id := <<"id">>}, Updated),
    ?assertNot(maps:is_key(aliases, Updated)),
    ?assertNot(maps:is_key(<<"aliases">>, cached_row(Id))),
    ?assertMatch({ok, #{aliases := [<<"NULLfoo">>]}}, get(?CACHED_NS, Id)),
    %% clearing fields
    {ok, ok} = update(?CACHED_NS, Id, #{subnamespace => undefined, sub_id => undefined, aliases => []}),
    Cleared = await_cache(Id, 3),
    ?assertNot(maps:is_key(subnamespace, Cleared)),
    ?assertNot(maps:is_key(sub_id, Cleared)),
    ?assertNot(maps:is_key(aliases, Cleared)),
    ?assertMatch({ok, #{aliases := []}}, get(?CACHED_NS, Id)).

%% Internal functions

await_cache(Id, EventsCount) ->
    await_cache(Id, EventsCount, 100).

%% waits by history length: cached last_event_id is not reliable for forward reading
await_cache(_Id, _EventsCount, 0) ->
    ct:fail(cache_not_updated);
await_cache(Id, EventsCount, Attempts) ->
    case prg_pg_cache:get(?CACHED_NS, Id, #{}) of
        {ok, #{history := History} = P} when length(History) =:= EventsCount ->
            P;
        _ ->
            timer:sleep(50),
            await_cache(Id, EventsCount, Attempts - 1)
    end.

cached_row(Id) ->
    [{Id, Row}] = ets:lookup('cached/namespace_processes', Id),
    Row.

observations(Metric, Labels) ->
    case prometheus_histogram:value(Metric, Labels) of
        undefined -> 0;
        {Buckets, _Sum} -> lists:sum(Buckets)
    end.

create(Ns, Fields) ->
    Id = gen_id(),
    {ok, ok} = init(Ns, Id, Fields),
    Id.

init(Ns, Id, Fields) ->
    progressor:init(#{ns => Ns, id => Id, args => term_to_binary(Fields)}).

update(Ns, Id, Fields) ->
    progressor:call(#{ns => Ns, id => Id, args => term_to_binary(Fields)}).

get(Ns, Id) ->
    progressor:get(#{ns => Ns, id => Id, options => #{use_cache => false}}).

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(microsecond),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.

gen_id() ->
    binary:encode_hex(crypto:strong_rand_bytes(16)).

find(Id, Processes) ->
    [Process] = [P || #{process_id := ProcessId} = P <- Processes, ProcessId =:= Id],
    Process.

ids(Processes) ->
    lists:sort([Id || #{process_id := Id} <- Processes]).
