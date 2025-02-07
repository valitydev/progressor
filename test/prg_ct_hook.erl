-module(prg_ct_hook).

%% API
-export([init/2, terminate/1, pre_init_per_suite/3]).

-define(LIFECYCLE_TOPIC, <<"default_lifecycle_topic">>).
-define(EVENTSINK_TOPIC, <<"default_topic">>).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).

init(_Id, State) ->
    _ = start_applications(),
    _ = create_kafka_topics(),
    State.

pre_init_per_suite(_SuiteName, Config, State) ->
    {Config ++ State, State}.

terminate(_State) -> ok.

%% Internal functions

start_applications() ->
    lists:foreach(fun(App) ->
        _ = application:load(App),
        lists:foreach(fun({K, V}) -> ok = application:set_env(App, K, V) end, app_env(App)),
        {ok, _} = application:ensure_all_started(App)
    end, app_list()).

app_list() ->
    %% in order of launch
    [
        epg_connector,
        brod,
        progressor
    ].

app_env(progressor) ->
    [
        {defaults, #{
            storage => #{
                client => prg_pg_backend,
                options => #{
                    pool => default_pool
                }
            },
            retry_policy => #{
                initial_timeout => 1, %% seconds
                backoff_coefficient => 1.0,
                max_timeout => 180, %% seconds
                max_attempts => 3,
                non_retryable_errors => [
                    do_not_retry,
                    some_reason,
                    any_term,
                    <<"Error message">>,
                    {temporary, unavilable}
                ]
            },
            task_scan_timeout => 1, %% seconds
            worker_pool_size => 10,
            process_step_timeout => 10 %% seconds
        }},

        {namespaces, #{
            'default/default' => #{
                processor => #{
                    client => prg_ct_processor,
                    options => #{}
                },
                notifier => #{
                    client => default_kafka_client,
                    options => #{
                        topic => ?EVENTSINK_TOPIC,
                        lifecycle_topic => ?LIFECYCLE_TOPIC
                    }
                }
            }
        }}
    ];
app_env(epg_connector) ->
    [
        {databases, #{
            progressor_db => #{
                host => "postgres",
                port => 5432,
                database => "progressor_db",
                username => "progressor",
                password => "progressor"
            }
        }},
        {pools, #{
            default_pool => #{
                database => progressor_db,
                size => 10
            }
        }}
    ];
app_env(brod) ->
    [
        {clients, [
            {default_kafka_client, [
                {endpoints, ?BROKERS},
                {auto_start_producers, true},
                {default_producer_config, []}
            ]}
        ]}
    ].

create_kafka_topics() ->
    TopicConfig = [
        #{
            configs => [],
            num_partitions => 1,
            assignments => [],
            replication_factor => 1,
            name => ?EVENTSINK_TOPIC
        },
        #{
            configs => [],
            num_partitions => 1,
            assignments => [],
            replication_factor => 1,
            name => ?LIFECYCLE_TOPIC
        }
    ],
    _ = brod:create_topics(?BROKERS, TopicConfig, #{timeout => 5000}).
