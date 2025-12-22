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
-export([simple_call_with_range_test/1]).
-export([call_replace_timer_test/1]).
-export([call_unset_timer_test/1]).
-export([postponed_call_test/1]).
-export([postponed_call_to_suspended_process_test/1]).
-export([multiple_calls_test/1]).
-export([simple_repair_after_non_retriable_error_test/1]).
-export([simple_repair_after_call_error_test/1]).
-export([repair_after_non_retriable_error_test/1]).
-export([error_after_max_retries_test/1]).
-export([repair_after_call_error_test/1]).
-export([remove_by_timer_test/1]).
-export([remove_without_timer_test/1]).
-export([put_process_test/1]).
-export([put_process_zombie_test/1]).
-export([put_process_with_timeout_test/1]).
-export([put_process_with_remove_test/1]).
-export([task_race_condition_hack_test/1]).

-define(NS(C), proplists:get_value(ns_id, C, 'default/default')).
-define(AWAIT_TIMEOUT(C), proplists:get_value(repl_timeout, C, 0)).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cache, C) ->
    _ = prg_ct_hook:start_applications(),
    _ = prg_ct_hook:create_kafka_topics(),
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
        {brod, prg_ct_hook:app_env(brod)},
        {progressor, UpdPrgConfig}
    ],
    _ = prg_ct_hook:start_applications(Applications),
    _ = prg_ct_hook:create_kafka_topics(),
    C;
init_per_group(_, C) ->
    _ = prg_ct_hook:start_applications(),
    _ = prg_ct_hook:create_kafka_topics(),
    C.

end_per_group(_, _) ->
    _ = prg_ct_hook:stop_applications(),
    ok.

all() ->
    [
        {group, base}
        %{group, tasks_injection}
        %% while race condition hack using cache not applicable
        %{group, cache}
    ].

groups() ->
    [
        {base, [], [
            simple_timers_test,
            simple_call_test,
            reschedule_after_call_test,
            simple_call_with_range_test,
            call_replace_timer_test,
            call_unset_timer_test,
            postponed_call_test,
            postponed_call_to_suspended_process_test,
            multiple_calls_test,
            repair_after_non_retriable_error_test,
            error_after_max_retries_test,
            repair_after_call_error_test,
            remove_by_timer_test,
            remove_without_timer_test,
            put_process_test,
            task_race_condition_hack_test
        ]},
        {tasks_injection, [], [
            %% tasks performed only by scanning
            simple_repair_after_non_retriable_error_test,
            simple_repair_after_call_error_test,
            put_process_zombie_test,
            put_process_with_timeout_test,
            put_process_with_remove_test
        ]},
        {cache, [], [
            {group, base}
        ]}
    ].

-spec simple_timers_test(_) -> _.
simple_timers_test(C) ->
    %% steps:
    %%    step       aux_state   events    action
    %% 1. init ->    aux_state1, [event1], timer 2s
    %% 2. timeout -> aux_state2, [event2], timer 0s
    %% 3. timeout -> undefined,  [],       undefined
    _ = mock_processor(simple_timers_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    3 = expect_steps_counter(3),
    ExpectedAux = erlang:term_to_binary(<<"aux_state2">>),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        aux_state := ExpectedAux,
        metadata := #{<<"k">> := <<"v">>},
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec simple_call_test(_) -> _.
simple_call_test(C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s
    %% 2. call ->    [event2], undefined (duration 3s)
    %% 3. timeout -> [event3], undefined
    _ = mock_processor(simple_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec simple_call_with_range_test(_) -> _.
simple_call_with_range_test(C) ->
    %% steps:
    %% 1. init ->    [event1, event2, event3, event4], timer 2s
    %% 2. call range limit 2 offset 1 ->    [event5], timer 0s
    %% 2. call range limit 2 offset 5 back -> [event6], timer 0s
    %% 3. timeout -> [event7], undefined
    _ = mock_processor(simple_call_with_range_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{
        ns => ?NS(C),
        id => Id,
        args => <<"call_args">>,
        range => #{offset => 1, limit => 2}
    }),
    {ok, <<"response">>} = progressor:call(#{
        ns => ?NS(C),
        id => Id,
        args => <<"call_args_back">>,
        range => #{offset => 5, limit => 2, direction => backward}
    }),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{event_id := 1},
            #{event_id := 2},
            #{event_id := 3},
            #{event_id := 4},
            #{event_id := 5},
            #{event_id := 6},
            #{event_id := 7}
        ]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        range := #{offset := 6, direction := backward},
        last_event_id := 7,
        history := [
            #{event_id := 5},
            #{event_id := 4},
            #{event_id := 3},
            #{event_id := 2},
            #{event_id := 1}
        ]
    }} = progressor:get(#{ns => ?NS(C), id => Id, range => #{offset => 6, direction => backward}}),
    unmock_processor(),
    ok.
%%

-spec call_replace_timer_test(_) -> _.
call_replace_timer_test(C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s + remove
    %% 2. call ->    [],       timer 0s (new timer cancel remove)
    %% 3. timeout -> [event2], undefined
    _ = mock_processor(call_replace_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, [
        #{
            task_id := _,
            args := <<"init_args">>,
            task_type := <<"init">>,
            task_status := <<"finished">>,
            task_metadata := #{<<"range">> := #{}},
            retry_interval := 0,
            retry_attempts := 0,
            scheduled := _,
            running := _,
            finished := _,
            response := {ok, ok},
            event_id := 1,
            event_timestamp := _,
            event_metadata := #{<<"format_version">> := 1},
            event_payload := _
        },
        #{
            task_id := _,
            task_type := <<"remove">>,
            task_status := <<"cancelled">>,
            scheduled := _,
            retry_interval := 0,
            retry_attempts := 0
        },
        #{
            task_id := _,
            args := <<"call_args">>,
            task_type := <<"call">>,
            task_status := <<"finished">>,
            retry_interval := 0,
            retry_attempts := 0,
            task_metadata := #{<<"range">> := #{}},
            scheduled := _,
            running := _,
            finished := _,
            response := {ok, <<"response">>}
        },
        #{
            task_id := _,
            task_type := <<"timeout">>,
            task_status := <<"finished">>,
            retry_interval := 0,
            retry_attempts := 0,
            scheduled := _,
            %% TODO need fix for running time!!!
            finished := _,
            response := {ok, ok},
            event_id := 2,
            event_timestamp := _,
            event_metadata := #{<<"format_version">> := 1},
            event_payload := _
        }
    ]} = progressor:trace(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec call_unset_timer_test(_) -> _.
call_unset_timer_test(C) ->
    %% steps:
    %% 1. init ->    [event1], timer 2s
    %% 2. call ->    [],       unset_timer
    _ = mock_processor(call_unset_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    %% wait 3 steps but got 2 - good!
    2 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec postponed_call_test(_) -> _.
postponed_call_test(C) ->
    %% call between 0 sec timers
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> [event1], timer 0s (process duration 3000)
    %% 3. call ->    [event2], undefined
    %% 4. timeout -> [event3], undefined
    _ = mock_processor(postponed_call_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec postponed_call_to_suspended_process_test(_) -> _.
postponed_call_to_suspended_process_test(C) ->
    %% call between 0 sec timers
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> [event1], undefined (process duration 3000)
    %% 3. call ->    [event2], undefined
    _ = mock_processor(postponed_call_to_suspended_process_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    {ok, <<"response">>} = progressor:call(#{ns => ?NS(C), id => Id, args => <<"call_args">>}),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec multiple_calls_test(_) -> _.
multiple_calls_test(C) ->
    %% call between 0 sec timers
    %% steps:
    %% 1.  init ->    [],        undefined
    %% 2.  call ->    [event1],  undefined
    %% ...
    %% 11. call ->    [event10], undefined
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
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{event_id := 1},
            #{event_id := 2},
            #{event_id := 3},
            #{event_id := 4},
            #{event_id := 5},
            #{event_id := 6},
            #{event_id := 7},
            #{event_id := 8},
            #{event_id := 9},
            #{event_id := 10}
        ]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.

-spec simple_repair_after_non_retriable_error_test(_) -> _.
simple_repair_after_non_retriable_error_test(C) ->
    %% steps:
    %% 1. init                            -> [],       timer 0s
    %% 2. timeout                         -> {error, do_not_retry}
    %% 3. timeout(via simple repair call) -> [event1], undefined
    %% 4. timeout                         -> [event2], undefined
    _ = mock_processor(simple_repair_after_non_retriable_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        detail := <<"do_not_retry">>,
        history := [],
        process_id := Id,
        status := <<"error">>,
        previous_status := <<"running">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:simple_repair(#{ns => ?NS(C), id => Id, context => <<"simple_repair_ctx">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok,
        #{
            process_id := Id,
            status := <<"running">>,
            previous_status := <<"error">>,
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
        } = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    unmock_processor(),
    ok.

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
        metadata := #{<<"k">> := <<"v">>},
        history := [],
        process_id := Id,
        status := <<"error">>,
        previous_status := <<"running">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:simple_repair(#{ns => ?NS(C), id => Id}),
    {ok, #{
        metadata := #{<<"k">> := <<"v">>},
        history := [],
        process_id := Id,
        status := <<"running">>,
        previous_status := <<"error">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    ok.

-spec repair_after_non_retriable_error_test(_) -> _.
repair_after_non_retriable_error_test(C) ->
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> {error, do_not_retry}
    %% 3. repair ->  [event1], undefined
    %% 4. timeout -> [event2], undefined
    _ = mock_processor(repair_after_non_retriable_error_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        detail := <<"do_not_retry">>,
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok,
        #{
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
        } = Process} = progressor:get(#{ns => ?NS(C), id => Id}),
    false = erlang:is_map_key(detail, Process),
    unmock_processor(),
    ok.
%%
-spec error_after_max_retries_test(_) -> _.
error_after_max_retries_test(C) ->
    %% steps:
    %% 1. init ->    [],       timer 0s
    %% 2. timeout -> woody_retryable_error
    %% 3. timeout -> woody_retryable_error
    %% 4. timeout -> woody_retryable_error
    _ = mock_processor(error_after_max_retries_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        detail := <<"{exception,error,{woody_error,{external,result_unknown,<<\"closed\">>}}}">>,
        history := [],
        process_id := Id,
        status := <<"error">>
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
        detail := <<"retry_this">>,
        metadata := #{<<"k">> := <<"v">>},
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {error, <<"repair_error">>} = progressor:repair(#{
        ns => ?NS(C), id => Id, args => <<"bad_repair_args">>
    }),
    3 = expect_steps_counter(3),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    %% shoul not rewrite detail
    {ok, #{
        detail := <<"retry_this">>,
        metadata := #{<<"k">> := <<"v">>},
        history := [],
        process_id := Id,
        status := <<"error">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    {ok, ok} = progressor:repair(#{ns => ?NS(C), id => Id, args => <<"repair_args">>}),
    4 = expect_steps_counter(4),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        metadata := #{<<"k2">> := <<"v2">>},
        history := [
            #{
                event_id := 1,
                metadata := #{<<"format_version">> := 1},
                payload := _Pl1,
                timestamp := _Ts1
            }
        ]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec remove_by_timer_test(_) -> _.
remove_by_timer_test(C) ->
    %% steps:
    %% 1. init -> [event1, event2], timer 2s + remove
    _ = mock_processor(remove_by_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    %% wait tsk_scan_timeout
    timer:sleep(4000),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec remove_without_timer_test(_) -> _.
remove_without_timer_test(C) ->
    %% steps:
    %% 1. init -> [event1], timer 2s
    %% 2. timeout -> [],    remove
    _ = mock_processor(remove_without_timer_test),
    Id = gen_id(),
    {ok, ok} = progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
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
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    2 = expect_steps_counter(2),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec put_process_test(_) -> _.
put_process_test(C) ->
    Id = gen_id(),
    Args = #{
        process => #{
            process_id => Id,
            status => <<"running">>,
            history => [
                event(1),
                event(2),
                event(3)
            ]
        }
    },
    {ok, ok} = progressor:put(#{ns => ?NS(C), id => Id, args => Args}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [
            #{
                metadata := #{<<"format_version">> := 1},
                process_id := Id,
                event_id := 1,
                timestamp := _Ts1,
                payload := _Pl1
            },
            #{
                timestamp := _Ts2,
                metadata := #{<<"format_version">> := 1},
                process_id := Id,
                event_id := 2,
                payload := _Pl2
            },
            #{
                timestamp := _Ts3,
                metadata := #{<<"format_version">> := 1},
                process_id := Id,
                event_id := 3,
                payload := _Pl3
            }
        ]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),

    {error, <<"process already exists">>} = progressor:put(#{ns => ?NS(C), id => Id, args => Args}),
    ok.
%%
-spec put_process_with_timeout_test(_) -> _.
put_process_with_timeout_test(C) ->
    %% steps:
    %% 1. put     -> [event1], timer 1s
    %% 2. timeout -> [event2], undefined
    _ = mock_processor(put_process_with_timeout_test),
    Id = gen_id(),
    Args = #{
        process => #{
            process_id => Id,
            status => <<"running">>,
            history => [event(1)]
        },
        action => #{set_timer => erlang:system_time(second) + 1}
    },
    {ok, ok} = progressor:put(#{ns => ?NS(C), id => Id, args => Args}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [#{event_id := 1}]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    1 = expect_steps_counter(1),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [#{event_id := 1}, #{event_id := 2}]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    unmock_processor(),
    ok.
%%
-spec put_process_zombie_test(_) -> _.
put_process_zombie_test(C) ->
    %% steps:
    %% 1. put     -> [event1], timer 1s
    %% 2. insert running task from past
    %% 3. zombie collecttion
    Id = gen_id(),
    Args = #{
        process => #{
            process_id => Id,
            status => <<"running">>,
            history => [event(1)]
        }
    },
    {ok, ok} = progressor:put(#{ns => ?NS(C), id => Id, args => Args}),
    Now = erlang:system_time(second),
    ZombieTs = prg_utils:unixtime_to_datetime(Now - 30),
    NS = erlang:atom_to_list(?NS(C)),
    %% TODO: rework it via storage backend
    %% START SQL INJECTION
    {ok, _, _, [{TaskId}]} = epg_pool:query(
        default_pool,
        "INSERT INTO \"" ++ NS ++
            "_tasks\" "
            "  (process_id, task_type, status, scheduled_time, running_time, args, last_retry_interval, attempts_count)"
            "  VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING task_id",
        [
            Id,
            <<"timeout">>,
            <<"running">>,
            ZombieTs,
            ZombieTs,
            <<>>,
            0,
            0
        ]
    ),
    {ok, 1} = epg_pool:query(
        default_pool,
        "INSERT INTO \"" ++ NS ++
            "_running\" "
            "  (task_id, process_id, task_type, status, scheduled_time, running_time, args, "
            "   last_retry_interval, attempts_count)"
            "  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        [
            TaskId,
            Id,
            <<"timeout">>,
            <<"running">>,
            ZombieTs,
            ZombieTs,
            <<>>,
            0,
            0
        ]
    ),
    %% END SQL INJECTION

    %% await zombie collection (process step timeout (10s) + random part (2s))
    timer:sleep(12010),
    {ok, #{
        process_id := Id,
        status := <<"error">>,
        detail := <<"zombie detected">>
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    ok.
%%
-spec put_process_with_remove_test(_) -> _.
put_process_with_remove_test(C) ->
    %% steps:
    %% 1. put     -> [event1], remove 1s
    %% 2. remove
    Id = gen_id(),
    Args = #{
        process => #{
            process_id => Id,
            status => <<"running">>,
            history => [event(1)]
        },
        action => #{set_timer => erlang:system_time(second) + 1, remove => true}
    },
    {ok, ok} = progressor:put(#{ns => ?NS(C), id => Id, args => Args}),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        process_id := Id,
        status := <<"running">>,
        history := [#{event_id := 1}]
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
    timer:sleep(3000),
    {error, <<"process not found">>} = progressor:get(#{ns => ?NS(C), id => Id}),
    ok.
%%
-spec task_race_condition_hack_test(_) -> _.
task_race_condition_hack_test(C) ->
    %% steps:
    %% 1. init (sleep 3s) -> [event1], undefined
    _ = mock_processor(task_race_condition_hack_test),
    Id = gen_id(),
    erlang:spawn(fun() -> progressor:init(#{ns => ?NS(C), id => Id, args => <<"init_args">>}) end),
    1 = expect_steps_counter(1),
    timer:sleep(?AWAIT_TIMEOUT(C)),
    {ok, #{
        status := <<"running">>,
        range := #{},
        history := [#{event_id := 1}],
        process_id := Id,
        aux_state := <<"aux_state">>,
        last_event_id := 1
    }} = progressor:get(#{ns => ?NS(C), id => Id}),
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
                    metadata => #{<<"k">> => <<"v">>},
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
mock_processor(reschedule_after_call_test = TestCase) ->
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
            %% call when process scheduled (wait timeout)
            timer:sleep(500),
            Result = #{
                response => <<"response">>,
                events => [event(2)]
            },
            Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing (without scanner, performed by internal timer)
            ?assertEqual(2, erlang:length(History)),
            Result = #{
                events => [event(3)]
            },
            Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(simple_call_with_range_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, Process}, _Opts, _Ctx) ->
            ?assertEqual(0, maps:get(last_event_id, Process)),
            Result = #{
                events => [event(1), event(2), event(3), event(4)]
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{history := History} = Process}, _Opts, _Ctx) ->
            %% call with range limit=2, offset=1
            ?assertEqual(2, erlang:length(History)),
            ?assertEqual(4, maps:get(last_event_id, Process)),
            [
                #{event_id := 2},
                #{event_id := 3}
            ] = History,
            Result = #{
                response => <<"response">>,
                events => [event(5)]
            },
            Self ! 2,
            {ok, Result};
        ({call, <<"call_args_back">>, #{history := History} = Process}, _Opts, _Ctx) ->
            %% call with range limit=2, offset=5 direction=backward
            ?assertEqual(2, erlang:length(History)),
            ?assertEqual(5, maps:get(last_event_id, Process)),
            [
                #{event_id := 4},
                #{event_id := 3}
            ] = History,
            Result = #{
                response => <<"response">>,
                events => [event(6)],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 3,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = Process}, _Opts, _Ctx) ->
            ?assertEqual(6, erlang:length(History)),
            ?assertEqual(6, maps:get(last_event_id, Process)),
            Result = #{
                events => [event(7)]
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
mock_processor(postponed_call_to_suspended_process_test = TestCase) ->
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
                events => [event(1)]
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
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(multiple_calls_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => []
            },
            Self ! 1,
            {ok, Result};
        ({call, <<N>>, _Process}, _Opts, _Ctx) ->
            timer:sleep(100),
            Result = #{
                response => <<"response">>,
                events => [event(N)]
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
                events => [],
                action => #{set_timer => erlang:system_time(second)}
            },
            Self ! 1,
            {ok, Result};
        ({timeout, <<>>, #{history := []} = _Process}, _Opts, <<>>) ->
            Self ! 2,
            {error, do_not_retry};
        ({timeout, <<>>, #{history := []} = _Process}, _Opts, <<"simple_repair_ctx">>) ->
            %% timeout via simple repair
            Result = #{
                events => [event(1)],
                action => #{set_timer => erlang:system_time(second)}
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
            erlang:raise(error, {woody_error, {external, result_unknown, <<"closed">>}}, [])
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(repair_after_call_error_test = TestCase) ->
    Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                metadata => #{<<"k">> => <<"v">>},
                events => []
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            %% retriable error for call must be ignore and process set error status
            {error, retry_this};
        ({repair, <<"bad_repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            %% repair error should not rewrite process detail
            Self ! 3,
            {error, <<"repair_error">>};
        ({repair, <<"repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Result = #{
                metadata => #{<<"k2">> => <<"v2">>},
                events => [event(1)]
            },
            Self ! 4,
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
                events => []
            },
            Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Self ! 2,
            %% retriable error for call must be ignore and process set error status
            {error, retry_this}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(remove_by_timer_test = TestCase) ->
    MockProcessor = fun({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
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
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(put_process_with_timeout_test = TestCase) ->
    Self = self(),
    MockProcessor = fun({timeout, <<>>, _Process}, _Opts, _Ctx) ->
        Result = #{events => [event(2)]},
        Self ! 1,
        {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
%%
mock_processor(task_race_condition_hack_test = TestCase) ->
    Self = self(),
    MockProcessor = fun({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
        timer:sleep(3000),
        Result = #{
            events => [event(1)],
            aux_state => <<"aux_state">>
        },
        Self ! 1,
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

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        %% msg_pack compatibility for kafka
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
