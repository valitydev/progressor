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
-export([simple_call_with_generation_test/1]).
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
    [{ns_id, 'cached/namespace'}, {repl_timeout, 50} | C];
init_per_group(_, C) ->
    C.

end_per_group(_, _) ->
    ok.

all() ->
    [
        {group, base},
        {group, cache}
    ].

groups() ->
    [
        {base, [], [
            simple_timers_test,
            simple_call_test,
            simple_call_with_generation_test,
            call_replace_timer_test,
            call_unset_timer_test,
            postponed_call_test,
            postponed_call_to_suspended_process_test,
            multiple_calls_test,
            simple_repair_after_non_retriable_error_test,
            repair_after_non_retriable_error_test,
            error_after_max_retries_test,
            repair_after_call_error_test,
            remove_by_timer_test,
            remove_without_timer_test
        ]},
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 3,
            payload := _
        },
        metadata := #{<<"k">> := <<"v">>},
        process_id := Id,
        aux_state := ExpectedAux,
        current_generation := 3
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 3,
            payload := _
        },
        process_id := Id,
        current_generation := 3
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 4,
            payload := _
        },
        process_id := Id,
        current_generation := 4
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 2,
            payload := _
        },
        process_id := Id,
        current_generation := 4
    }} = progressor:get(#{ns => ?NS(C), id => Id, generation => 2}),
    unmock_processor(),
    ok.
%%
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 3,
            payload := _
        },
        process_id := Id,
        current_generation := 3
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 2,
            payload := _
        },
        process_id := Id,
        current_generation := 2
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
    {ok, #{
        status := <<"running">>,
        state := #{
            timestamp := _,
            metadata := #{<<"format_version">> := 1},
            process_id := Id,
            task_id := _,
            generation := 4,
            payload := _
        },
        process_id := Id,
        current_generation := 4
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
        corrupted_by := _
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:simple_repair(#{ns => ?NS(C), id => Id, context => <<"simple_repair_ctx">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 3, 3, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
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
        corrupted_by := _
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    3 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
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
        corrupted_by := _
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
        corrupted_by := CorruptionTask
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
        corrupted_by := CorruptionTask
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, ?PROCESS_EXPECTED(Id, 2, 2, <<"running">>) = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    false = erlang:is_map_key(corrupted_by, Process),
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

%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%

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
                    action => #{set_timer => erlang:system_time(second)},
                    aux_state => erlang:term_to_binary(<<"aux_state2">>)
                },
                Self ! 2,
                {ok, Result};
            _ ->
                Result = #{
                    state => state(),
                    metadata => #{<<"k">> => <<"v">>},
                    %% postponed timer
                    action => #{set_timer => erlang:system_time(second) + 2},
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
                action => #{set_timer => erlang:system_time(second) + 2}
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
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second) + 2, remove => true}
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended
            Result = #{
                response => <<"response">>,
                state => state(),
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second) + 2}
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (wait timeout)
            Result = #{
                response => <<"response">>,
                state => state(),
                action => unset_timer
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
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{current_generation := 1} = _Process}, _Opts, _Ctx) ->
            timer:sleep(3000),
            Result = #{
                state => state(),
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second)}
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
                action => #{set_timer => erlang:system_time(second)}
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
            action => #{set_timer => erlang:system_time(second) + 2, remove => true}
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
                action => #{set_timer => erlang:system_time(second) + 2}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, _Process}, _Opts, _Ctx) ->
            Result = #{
                state => state(),
                action => #{remove => true}
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
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
