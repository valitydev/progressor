-module(prg_base_SUITE).

-include_lib("stdlib/include/assert.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    all/0,
    groups/0
]).

%% Tests
-export([simple_timers_test/1]).
-export([simple_call_test/1]).
-export([reschedule_after_call_test/1]).
-export([simple_repair_after_call_error_test/1]).
-export([simple_call_with_generation_test/1]).
-export([missing_generation_test/1]).
-export([call_replace_timer_test/1]).
-export([call_unset_timer_test/1]).
-export([postponed_call_test/1]).
-export([postponed_call_to_suspended_process_test/1]).
-export([multiple_calls_test/1]).
-export([simple_repair_after_non_retriable_error_test/1]).
-export([repair_after_non_retriable_error_test/1]).
-export([error_after_max_retries_test/1]).
-export([repair_after_call_error_test/1]).
-export([remove_by_timer_test/1]).
-export([remove_without_timer_test/1]).

-define(TIMEOUT_IN(Sec), {schedule, #{at => erlang:system_time(microsecond) + (Sec) * 1000000, action => timeout}}).
-define(REMOVE_IN(Sec), {schedule, #{at => erlang:system_time(microsecond) + (Sec) * 1000000, action => remove}}).

-define(NS(C), proplists:get_value(ns_id, C, 'default/default')).
-define(AWAIT_TIMEOUT(C), proplists:get_value(repl_timeout, C, 0)).
-define(PROCESS_EXPECTED(Id, StateGeneration, CurrentGeneration, Status), #{
    status := Status,
    state := #{
        timestamp := _,
        metadata := #{<<"format_version">> := 1},
        process_id := Id,
        task_id := _,
        generation := StateGeneration,
        payload := _
    },
    process_id := Id,
    current_generation := CurrentGeneration
}).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cache, C) ->
    _ = prg_ct_hook:start_applications(),
    [{ns_id, 'cached/namespace'}, {repl_timeout, 50} | C];
init_per_group(tasks_injection, C) ->
    PrgConfig = prg_ct_hook:app_env(progressor),
    UpdPrgConfig = lists:foldl(
        fun
            ({defaults, Defaults}, Acc) -> [{defaults, Defaults#{task_scan_timeout => 1}} | Acc];
            (Env, Acc) -> [Env | Acc]
        end,
        [],
        PrgConfig
    ),
    Applications = [
        {epg_connector, prg_ct_hook:app_env(epg_connector)},
        {progressor, UpdPrgConfig}
    ],
    _ = prg_ct_hook:start_applications(Applications),
    C;
init_per_group(_, C) ->
    _ = prg_ct_hook:start_applications(),
    C.

end_per_group(_, _) ->
    _ = prg_ct_hook:stop_applications(),
    ok.

all() ->
    [
        {group, base},
        {group, tasks_injection},
        {group, cache}
    ].

groups() ->
    [
        {base, [], [
            simple_timers_test,
            simple_call_test,
            reschedule_after_call_test,
            simple_call_with_generation_test,
            missing_generation_test,
            call_replace_timer_test,
            call_unset_timer_test,
            postponed_call_test,
            postponed_call_to_suspended_process_test,
            multiple_calls_test,
            repair_after_non_retriable_error_test,
            error_after_max_retries_test,
            repair_after_call_error_test,
            remove_by_timer_test,
            remove_without_timer_test
        ]},
        {tasks_injection, [], [simple_repair_after_non_retriable_error_test, simple_repair_after_call_error_test]},
        {cache, [], [
            {group, base}
        ]}
    ].

-spec simple_timers_test(_) -> _.
simple_timers_test(C) ->
    %% steps:
    %%    step       aux_state   state    action
    %% 1. init ->    aux_state1, state1, timer 2s
    %% 2. timeout -> aux_state2, state2, timer 0s
    %% 3. timeout -> undefined,  state3, undefined
    _ = mock_processor(simple_timers_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    3 = expect_steps_counter(3),
    ExpectedAux = erlang:term_to_binary(<<"aux_state2">>),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    ?assertEqual(ExpectedAux, maps:get(aux_state, Process)),
    unmock_processor(),
    ok.
%%
-spec simple_call_test(_) -> _.
simple_call_test(C) ->
    %% steps:
    %% 1. init ->    state1, timer 2s
    %% 2. call ->    state2, undefined (duration 3s)
    %% 3. timeout -> state3, undefined
    _ = mock_processor(simple_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec simple_call_with_generation_test(_) -> _.
simple_call_with_generation_test(C) ->
    %% steps:
    %% 1. init              -> state1, timer 2s
    %% 2. call generation 1 -> state2, timer 0s
    %% 2. call generation 1 -> state3, timer 0s
    %% 3. timeout           -> state4, undefined
    _ = mock_processor(simple_call_with_generation_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{
        ns => ?NS(C),
        id => Id,
        args => <<"call_args">>,
        generation => 1
    }),
    {ok, <<"response">>} = progressor:call(#{
        ns => ?NS(C),
        id => Id,
        args => <<"call_args_back">>,
        generation => 1
    }),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 4, 4, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ?PROCESS_EXPECTED(Id, 2, 4, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id, generation => 2}),
    unmock_processor(),
    ok.
%%
-spec missing_generation_test(_) -> _.
missing_generation_test(C) ->
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            {ok, #{state => state()}};
        ({call, <<"call_args">>, #{state := #{generation := 1}}}, _Opts, _Ctx) ->
            {ok, #{state => state(), response => <<"response">>}}
    end,
    _ = mock_processor(missing_generation_test, MockProcessor),
    try
        Id = gen_id(),
        Req = #{ns => ?NS(C), id => Id},
        {ok, ok} = progressor:init(Req#{args => <<"init_args">>}),
        timer:sleep(?AWAIT_TIMEOUT(C)),
        {ok, ?PROCESS_EXPECTED(Id, 1, 1, <<"running">>)} = progressor:get(Req),
        {ok, InitialTrace} = progressor:trace(Req),
        ?assertMatch([#{task_type := <<"init">>}], InitialTrace),
        ?assertEqual({error, {generation_not_found, 2}}, progressor:get(Req#{generation => 2})),
        Call = Req#{args => <<"call_args">>, idempotency_key => gen_id()},
        ?assertEqual({error, {generation_not_found, 2}}, progressor:call(Call#{generation => 2})),
        ?assertEqual({ok, InitialTrace}, progressor:trace(Req)),
        ?assertEqual(1, meck:num_calls(prg_ct_processor, process, '_')),
        {ok, ?PROCESS_EXPECTED(Id, 1, 1, <<"running">>)} = progressor:get(Req),
        %% A rejected request must not consume its idempotency key.
        {ok, <<"response">>} = progressor:call(Call#{generation => 1}),
        timer:sleep(?AWAIT_TIMEOUT(C)),
        {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>)} = progressor:get(Req),
        {ok, [#{task_type := <<"init">>}, #{task_type := <<"call">>, generation := 1}]} = progressor:trace(Req),
        ?assertEqual(2, meck:num_calls(prg_ct_processor, process, '_'))
    after
        unmock_processor()
    end,
    ok.

-spec call_replace_timer_test(_) -> _.
call_replace_timer_test(C) ->
    %% steps:
    %% 1. init ->    state1, timer 2s + remove
    %% 2. call ->    state2,       timer 0s (new timer cancel remove)
    %% 3. timeout -> state3, undefined
    _ = mock_processor(call_replace_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    %% wait task_scan_timeout, maybe remove works
    timer:sleep(4000),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, [
        #{
            args := <<"init_args">>,
            running := _,
            finished := _,
            state_generation := 1,
            state_timestamp := _,
            state_metadata := #{<<"format_version">> := 1},
            state_payload := _,
            task_id := _,
            task_type := <<"init">>,
            task_status := <<"finished">>,
            scheduled := _,
            retry_interval := 0,
            retry_attempts := 0
        },
        #{
            args := <<"call_args">>,
            running := _,
            finished := _,
            generation := 1,
            state_generation := 2,
            task_id := _,
            task_type := <<"call">>,
            task_status := <<"finished">>,
            scheduled := _,
            retry_interval := 0,
            retry_attempts := 0
        },
        #{
            running := _,
            finished := _,
            generation := 2,
            state_generation := 3,
            task_id := _,
            task_type := <<"timeout">>,
            task_status := <<"finished">>,
            scheduled := _,
            retry_interval := 0,
            retry_attempts := 0
        },
        #{
            task_id := _,
            task_type := <<"remove">>,
            task_status := <<"cancelled">>,
            scheduled := _,
            retry_interval := 0,
            retry_attempts := 0
        }
    ]} = progressor:trace(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec call_unset_timer_test(_) -> _.
call_unset_timer_test(C) ->
    %% steps:
    %% 1. init ->    state1,   timer 2s
    %% 2. call ->    [],       unset_timer
    _ = mock_processor(call_unset_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    %% wait 3 steps but got 2 - good!
    2 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec postponed_call_test(_) -> _.
postponed_call_test(C) ->
    %% call between 0 sec timers
    %% steps:
    %% 1. init ->    state1,    timer 0s
    %% 2. timeout -> state2,    timer 0s (process duration 3000)
    %% 3. call ->    state3,    undefined
    %% 4. timeout -> state4,    undefined
    _ = mock_processor(postponed_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 4, 4, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec postponed_call_to_suspended_process_test(_) -> _.
postponed_call_to_suspended_process_test(C) ->
    %% steps:
    %% 1. init ->    state1, timer 0s
    %% 2. timeout -> state2, undefined (process duration 3000)
    %% 3. call ->    state3, undefined
    _ = mock_processor(postponed_call_to_suspended_process_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec multiple_calls_test(_) -> _.
multiple_calls_test(C) ->
    %% steps:
    %% 1.  init ->    state1,  undefined
    %% 2.  call ->    state2,  undefined
    %% ...
    %% 11. call ->    state11, undefined
    _ = mock_processor(multiple_calls_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    lists:foreach(
        fun(N) ->
            spawn(progressor, call, [#{ns => ?NS(C), id => Id, args => <<N>>}])
        end,
        lists:seq(1, 10)
    ),
    11 = expect_steps_counter(33),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 11, 11, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.

-spec simple_repair_after_non_retriable_error_test(_) -> _.
simple_repair_after_non_retriable_error_test(C) ->
    %% steps:
    %% 1. init                            -> state1, timer 0s
    %% 2. timeout                         -> {error, do_not_retry}
    %% 3. timeout(via simple repair call) -> state2, undefined
    %% 4. timeout                         -> state3, undefined
    _ = mock_processor(simple_repair_after_non_retriable_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        status := <<"error">>,
        state := #{generation := 1},
        process_id := Id,
        current_generation := 1,
        detail := <<"do_not_retry">>,
        corrupted_by := _,
        corrupted_type := <<"timeout">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:simple_repair(#{ns => ?NS(C), id => Id, context => <<"simple_repair_ctx">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
    false = erlang:is_map_key(corrupted_type, Process),
    unmock_processor(),
    ok.

-spec repair_after_non_retriable_error_test(_) -> _.
repair_after_non_retriable_error_test(C) ->
    %% steps:
    %% 1. init ->    state1, timer 0s
    %% 2. timeout -> {error, do_not_retry}
    %% 3. repair ->  state2, undefined
    _ = mock_processor(repair_after_non_retriable_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        status := <<"error">>,
        state := #{generation := 1},
        process_id := Id,
        current_generation := 1,
        detail := <<"do_not_retry">>,
        corrupted_by := _,
        corrupted_type := <<"timeout">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    3 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
    false = erlang:is_map_key(corrupted_type, Process),
    unmock_processor(),
    ok.
%%
-spec error_after_max_retries_test(_) -> _.
error_after_max_retries_test(C) ->
    %% steps:
    %% 1. init ->    state1, timer 0s
    %% 2. timeout -> {error, retry_this}
    %% 3. timeout -> {error, retry_this}
    %% 4. timeout -> {error, retry_this}
    _ = mock_processor(error_after_max_retries_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        status := <<"error">>,
        state := #{generation := 1},
        process_id := Id,
        current_generation := 1,
        detail := <<"retry_this">>,
        corrupted_by := _,
        corrupted_type := <<"timeout">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec repair_after_call_error_test(_) -> _.
repair_after_call_error_test(C) ->
    %% steps:
    %% 1. init ->    [],       undefined
    %% 2. call ->    {error, retry_this}
    %% 3. repair ->  {error, repair_error}
    %% 4. repair ->  [event1], undefined
    _ = mock_processor(repair_after_call_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {error, retry_this} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        status := <<"error">>,
        state := #{generation := 1},
        process_id := Id,
        current_generation := 1,
        detail := <<"retry_this">>,
        corrupted_by := CorruptionTask,
        corrupted_type := <<"call">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {error, <<"repair_error">>} = progressor:repair(#{
        ns => ?NS(C), id => Id, args => <<"bad_repair_args">>
    }),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    %% shoul not rewrite detail
    {ok, #{
        status := <<"error">>,
        state := #{generation := 1},
        process_id := Id,
        current_generation := 1,
        detail := <<"retry_this">>,
        corrupted_by := CorruptionTask,
        corrupted_type := <<"call">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
    false = erlang:is_map_key(corrupted_type, Process),
    ?assertEqual(#{<<"k2">> => <<"v2">>}, maps:get(metadata, Process)),
    unmock_processor(),
    ok.
%%
-spec remove_by_timer_test(_) -> _.
remove_by_timer_test(C) ->
    %% steps:
    %% 1. init -> state1, timer 2s + remove
    _ = mock_processor(remove_by_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 1, 1, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    %% wait tsk_scan_timeout
    timer:sleep(4000),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec remove_without_timer_test(_) -> _.
remove_without_timer_test(C) ->
    %% steps:
    %% 1. init -> state1, timer 2s
    %% 2. timeout -> state2,    remove
    _ = mock_processor(remove_without_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 1, 1, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.

-spec reschedule_after_call_test(_) -> _.
reschedule_after_call_test(C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s
    %% 2. call ->    [event2], undefined (duration 500 ms)
    %% 3. timeout -> [event3], undefined
    _ = mock_processor(reschedule_after_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>)} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%

-spec simple_repair_after_call_error_test(_) -> _.
simple_repair_after_call_error_test(C) ->
    %% steps:
    %% 1. init ->    [],       undefined
    %% 2. call ->    {error, retry_this}
    %% 3. simple_repair
    _ = mock_processor(simple_repair_after_call_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {error, retry_this} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        detail := <<"retry_this">>,
        corrupted_by := CorruptionTask,
        corrupted_type := <<"call">>,
        metadata := #{<<"k">> := <<"v">>},
        state := #{generation := 1},
        process_id := Id,
        status := <<"error">>,
        previous_status := <<"running">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, BeforeTrace} = progressor:trace(#{ns => ?NS(C), id => Id}),
    ?assertMatch([_, #{task_id := CorruptionTask, task_type := <<"call">>}], BeforeTrace),
    {ok, ok} = progressor:simple_repair(#{ns => ?NS(C), id => Id}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok,
        #{
            metadata := #{<<"k">> := <<"v">>},
            state := #{generation := 1},
            process_id := Id,
            status := <<"running">>,
            previous_status := <<"error">>
        } = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    ?assertNot(maps:is_key(corrupted_by, Process)),
    ?assertNot(maps:is_key(corrupted_type, Process)),
    ?assertNot(maps:is_key(detail, Process)),
    ?assertEqual({ok, BeforeTrace}, progressor:trace(#{ns => ?NS(C), id => Id})),
    unmock_processor(),
    ok.

%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%

mock_processor(reschedule_after_call_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => ?TIMEOUT_IN(2)
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process scheduled (wait timeout)
            timer:sleep(500),
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{state := #{generation := Generation}} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing (without scanner, performed by internal timer)
            ?assertEqual(2, Generation),
            Result = #{
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_repair_after_call_error_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                metadata => #{<<"k">> => <<"v">>},
                state => state()
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{state := #{generation := 1}} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            %% retriable error for call must be ignore and process set error status
            {error, retry_this}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_timers_test = TestCase) ->
    Self = self(),
    MockProcessor = fun({_Type, _Args, Process}, _Opts, _Ctx) ->
        case Process of
            #{current_generation := 2, state := #{generation := 2}} ->
                Result = #{
                    state => state()
                },
                Self ! 3,
                {ok, Result};
            #{current_generation := 1, state := #{generation := 1}} ->
                Result = #{
                    state => state(),
                    %% continuation timer
                    action => timeout,
                    aux_state => erlang:term_to_binary(<<"aux_state2">>)
                },
                Self ! 2,
                {ok, Result};
            _ ->
                Result = #{
                    state => state(),
                    metadata => #{<<"k">> => <<"v">>},
                    %% postponed timer
                    action => ?TIMEOUT_IN(2),
                    aux_state => erlang:term_to_binary(<<"aux_state1">>)
                },
                Self ! 1,
                {ok, Result}
        end
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_call_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => ?TIMEOUT_IN(2)
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (deferred timer while call processed)
            timer:sleep(3000),
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing
            Result = #{
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_call_with_generation_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{current_generation := 1, state := #{generation := 1}} = _Process}, _Opts, _Ctx) ->
            %% call with generation=1 when current_generation=1
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! 2,
            {ok, Result};
        (
            {
                call,
                <<"call_args_back">>,
                #{current_generation := 2, state := #{generation := 1}} = _Process
            },
            _Opts,
            _Ctx
        ) ->
            %% call with generation=1 when current_generation=2
            Result = #{
                response => <<"response">>,
                state => state(),
                action => timeout
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 3, state := #{generation := 3}} = _Process}, _Opts, _Ctx) ->
            %% timeout task executes on last generation
            Result = #{
                state => state()
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(call_replace_timer_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => ?REMOVE_IN(2)
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended
            Result = #{
                response => <<"response">>,
                state => state(),
                action => timeout
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing (remove action was cancelled by call action)
            Result = #{
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(call_unset_timer_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => ?TIMEOUT_IN(2)
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (wait timeout)
            Result = #{
                response => <<"response">>,
                state => state(),
                action => suspend
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(postponed_call_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            timer:sleep(3000),
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 2,
            {ok, Result};
        ({call, <<"call_args">>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 3} = _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(postponed_call_to_suspended_process_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            timer:sleep(3000),
            Result = #{
                state => state()
            },
            Self ! 2,
            {ok, Result};
        ({call, <<"call_args">>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(multiple_calls_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 1,
            {ok, Result};
        ({call, <<_N>>, _Process}, _Opts, _Ctx) ->
            timer:sleep(100),
            Result = #{
                response => <<"response">>,
                state => state()
            },
            Self ! iterate,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_repair_after_non_retriable_error_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, <<>>) ->
            Self ! 2,
            {error, do_not_retry};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, <<"simple_repair_ctx">>) ->
            %% timeout via simple repair
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 2} = _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(repair_after_non_retriable_error_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            {error, do_not_retry};
        ({repair, <<"repair_args">>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state()
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(error_after_max_retries_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => timeout
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            %% must be 3 attempts
            Self ! iterate,
            {error, retry_this}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(repair_after_call_error_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                metadata => #{<<"k">> => <<"v">>},
                state => state()
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            %% retriable error for call must be ignore and process set error status
            {error, retry_this};
        ({repair, <<"bad_repair_args">>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            %% repair error should not rewrite process detail
            Self ! 3,
            {error, <<"repair_error">>};
        ({repair, <<"repair_args">>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            Result = #{
                metadata => #{<<"k2">> => <<"v2">>},
                state => state()
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(remove_by_timer_test = TestCase) ->
    MockProcessor = fun({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
        Result = #{
            state => state(),
            action => ?REMOVE_IN(2)
        },
        {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(remove_without_timer_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => ?TIMEOUT_IN(2)
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => remove
            },
            Self ! 2,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor).

mock_processor(_TestCase, MockFun) ->
    meck:new(prg_ct_processor),
    meck:expect(prg_ct_processor, process, MockFun).

unmock_processor() ->
    meck:unload(prg_ct_processor).

expect_steps_counter(ExpectedSteps) ->
    expect_steps_counter(ExpectedSteps, 0).

expect_steps_counter(ExpectedSteps, CurrentStep) ->
    receive
        iterate when CurrentStep + 1 =:= ExpectedSteps ->
            %% wait storage
            timer:sleep(50),
            ExpectedSteps;
        iterate ->
            expect_steps_counter(ExpectedSteps, CurrentStep + 1);
        Counter when Counter =:= ExpectedSteps ->
            %% wait storage
            timer:sleep(50),
            Counter;
        Counter ->
            expect_steps_counter(ExpectedSteps, Counter)
    after 5000 ->
        %% after process_step_timeout/2
        CurrentStep
    end.

state() ->
    #{
        timestamp => erlang:system_time(microsecond),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
