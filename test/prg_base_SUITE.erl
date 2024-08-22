-module(prg_base_SUITE).

-include_lib("stdlib/include/assert.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    all/0
]).

%% Tests
-export([simple_timers_test/1]).
-export([simple_call_test/1]).
-export([call_replace_timer_test/1]).
-export([call_unset_timer_test/1]).
-export([postponed_call_test/1]).
-export([repair_after_non_retriable_error_test/1]).
-export([error_after_max_retries_test/1]).
-export([repair_after_call_error_test/1]).
-export([remove_by_timer_test/1]).
-export([remove_without_timer_test/1]).

-define(NS, 'default/default').

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

all() -> [
    simple_timers_test,
    simple_call_test,
    call_replace_timer_test,
    call_unset_timer_test,
    postponed_call_test,
    repair_after_non_retriable_error_test,
    error_after_max_retries_test,
    repair_after_call_error_test,
    remove_by_timer_test,
    remove_without_timer_test
].

-spec simple_timers_test(_) -> _.
simple_timers_test(_C) ->
    %% steps:
    %%    step       aux_state   events    action
    %% 1. init ->    aux_state1, [event1], timer 2s
    %% 2. timeout -> aux_state2, [event2], timer 0s
    %% 3. timeout -> undefined,  [],       undefined
    _ = mock_processor(simple_timers_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    3 = expect_steps_counter(3),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        aux_state := _AuxState,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    unmock_processor(),
    ok.
%%
-spec simple_call_test(_) -> _.
simple_call_test(_C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s
    %% 2. call ->    [event2], undefined
    %% 3. timeout -> [event3], undefined
    _ = mock_processor(simple_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS, id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            },
            #{
                event_id := 3,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl3,
                timestamp := _Ts3
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec call_replace_timer_test(_) -> _.
call_replace_timer_test(_C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s + remove
    %% 2. call ->    [],       timer 0s (new timer cancel remove)
    %% 3. timeout -> [event2], undefined
    _ = mock_processor(call_replace_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS, id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    %% wait task_scan_timeout, maybe remove works
    timer:sleep(4000),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec call_unset_timer_test(_) -> _.
call_unset_timer_test(_C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s
    %% 2. call ->    [],       unset_timer
    _ = mock_processor(call_unset_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS, id => Id, args => <<"call_args">>}),
    %% wait 3 steps but got 2 - good!
    2 = expect_steps_counter(3),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec postponed_call_test(_) -> _.
postponed_call_test(_C) ->
    %% call between 0 sec timers
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> [event1], timer 0s (process duration 3000)
    %% 3. call ->    [event2], undefined
    %% 4. timeout -> [event3], undefined
    _ = mock_processor(postponed_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS, id => Id, args => <<"call_args">>}),
    4 = expect_steps_counter(4),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            },
            #{
                event_id := 3,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl3,
                timestamp := _Ts3
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec repair_after_non_retriable_error_test(_) -> _.
repair_after_non_retriable_error_test(_C) ->
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> {error, do_not_retry}
    %% 3. repair ->  [event1], undefined
    %% 4. timeout -> [event2], undefined
    _ = mock_processor(repair_after_non_retriable_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    2 = expect_steps_counter(2),
    {ok, #{
        detail := <<"do_not_retry">>,
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS, id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS, id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec error_after_max_retries_test(_) -> _.
error_after_max_retries_test(_C) ->
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> {error, retry_this}
    %% 3. timeout -> {error, retry_this}
    %% 4. timeout -> {error, retry_this}
    _ = mock_processor(error_after_max_retries_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    4 = expect_steps_counter(4),
    {ok, #{
        detail := <<"retry_this">>,
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec repair_after_call_error_test(_) -> _.
repair_after_call_error_test(_C) ->
    %% steps:
    %% 1. init ->    [],       undefined
    %% 2. call ->    {error, retry_this}
    %% 3. repair ->  {error, repair_error}
    %% 4. repair ->  [event1], undefined
    _ = mock_processor(repair_after_call_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    %% FIXME BUG: incorrect Reason returning
    {error, _} = progressor:call(#{ns => ?NS, id => Id, args => <<"call_args">>}),
    2 = expect_steps_counter(2),
    {ok, #{
        detail := <<"retry_this">>,
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS, id => Id}),
    %% FIXME BUG: incorrect Reason returning
    {error, _} = progressor:repair(#{ns => ?NS, id => Id, args => <<"bad_repair_args">>}),
    3 = expect_steps_counter(3),
    %% shoul not rewrite detail
    {ok, #{
        detail := <<"retry_this">>,
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS, id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS, id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec remove_by_timer_test(_) -> _.
remove_by_timer_test(_C) ->
    %% steps:
    %% 1. init -> [event1, event2], timer 2s + remove
    _ = mock_processor(remove_by_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            },
            #{
                event_id := 2,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl2,
                timestamp := _Ts2
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    %% wait tsk_scan_timeout
    timer:sleep(4000),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS, id => Id}),
    ok.
%%
-spec remove_without_timer_test(_) -> _.
remove_without_timer_test(_C) ->
    %% steps:
    %% 1. init -> [event1], timer 2s
    %% 2. timeout -> [],            remove
    _ = mock_processor(remove_without_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS, id => Id, args => <<"init_args">>}),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            }
        ]
    }} = progressor:get(#{ns => ?NS, id => Id}),
    2 = expect_steps_counter(2),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS, id => Id}),
    ok.

%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%

mock_processor(simple_timers_test = TestCase) ->
    Self = self(),
    MockProcessor = fun({_Type, _Args, #{history := History} = _Process}, _Opts, _Ctx) ->
        case erlang:length(History) of
            0 ->
                Result = #{
                    events => [event(1)],
                    %% postponed timer
                    action => #{set_timer => erlang:system_time(second) + 2},
                    aux_state => erlang:term_to_binary(<<"aux_state1">>)
                },
                Self ! 1,
                {ok, Result};
            1 ->
                Result = #{
                    events => [event(2)],
                    %% continuation timer
                    action => #{set_timer => erlang:system_time(second)},
                    aux_state => erlang:term_to_binary(<<"aux_state2">>)
                },
                Self ! 2,
                {ok, Result};
            _ ->
                Result = #{
                    events => []
                },
                Self ! 3,
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
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second) + 2}
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (wait timeout)
            timer:sleep(3000),
            Result = #{
                response => <<"response">>,
                events => [event(2)]
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing
            ?assertEqual(2, erlang:length(History)),
            Result = #{
                events => [event(3)]
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(call_replace_timer_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second) + 2, remove => true}
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (wait timeout)
            Result = #{
                response => <<"response">>,
                events => [],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing (remove action was cancelled by call action)
            ?assertEqual(1, erlang:length(History)),
            Result = #{
                events => [event(2)]
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
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second) + 2}
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            %% call when process suspended (wait timeout)
            Result = #{
                response => <<"response">>,
                events => [],
                action => unset_timer
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing (should not work!)
            ?assertEqual(2, erlang:length(History)),
            Result = #{
                events => [event(3)]
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
                events => [],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{history := []} = _Process}, _Opts, _Ctx) ->
            timer:sleep(3000),
            Result = #{
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 2,
            {ok, Result};
        ({call, <<"call_args">>, #{history := History} = _Process}, _Opts, _Ctx) ->
            ?assertEqual(1, erlang:length(History)),
            Result = #{
                response => <<"response">>,
                events => [event(2)]
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            ?assertEqual(2, erlang:length(History)),
            Result = #{
                events => [event(3)]
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
                events => [],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            {error, do_not_retry};
        ({repair, <<"repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1)]
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            ?assertEqual(1, erlang:length(History)),
            Result = #{
                events => [event(2)]
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(error_after_max_retries_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{history := []} = _Process}, _Opts, _Ctx) ->
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
                events => []
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            %% retriable error for call must be ignore and process set  error status
            {error, retry_this};
        ({repair, <<"bad_repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            %% repair error should not rewrite process detail
            Self ! 3,
            {error, <<"repair_error">>};
        ({repair, <<"repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1)]
            },
            Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(remove_by_timer_test = TestCase) ->
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1), event(2)],
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
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second) + 2}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [],
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
            timer:sleep(20),
            ExpectedSteps;
        iterate ->
            expect_steps_counter(ExpectedSteps, CurrentStep + 1);
        Counter when Counter =:= ExpectedSteps ->
            %% wait storage
            timer:sleep(20),
            Counter;
        Counter ->
            expect_steps_counter(ExpectedSteps, Counter)
    after 5000 ->
        %% after process_step_timeout/2
        CurrentStep
    end.

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)}) %% msg_pack compatibility for kafka
    }.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
