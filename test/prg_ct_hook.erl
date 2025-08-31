-module(prg_ct_hook).

%% API
-export([init/2, terminate/1, pre_init_per_suite/3]).

init(_Id, State) ->
    _ = start_applications(),
    State.

pre_init_per_suite(_SuiteName, Config, State) ->
    {Config ++ State, State}.

terminate(_State) -> ok.

%% Internal functions

start_applications() ->
    lists:foreach(
        fun(App) ->
            _ = application:load(App),
            lists:foreach(fun({K, V}) -> ok = application:set_env(App, K, V) end, app_env(App)),
            {ok, _} = application:ensure_all_started(App)
        end,
        app_list()
    ).

app_list() ->
    %% in order of launch
    [
        epg_connector,
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
                %% seconds
                initial_timeout => 1,
                backoff_coefficient => 1.0,
                %% seconds
                max_timeout => 180,
                max_attempts => 3,
                non_retryable_errors => [
                    do_not_retry,
                    some_reason,
                    any_term,
                    <<"Error message">>,
                    {temporary, unavilable}
                ]
            },
            %% seconds
            task_scan_timeout => 1,
            worker_pool_size => 10,
            %% seconds
            process_step_timeout => 10
        }},

        {namespaces, #{
            'default/default' => #{
                processor => #{
                    client => prg_ct_processor,
                    options => #{}
                }
            },
            'cached/namespace' => #{
                storage => #{
                    client => prg_pg_backend,
                    options => #{
                        pool => default_pool,
                        cache => progressor_db
                    }
                },
                processor => #{
                    client => prg_ct_processor,
                    options => #{}
                }
            }
        }},

        {post_init_hooks, [
            {prg_pg_cache, start, [#{progressor_db => {['cached/namespace'], "progressor"}}]}
        ]}
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
                size => {1, 2}
            }
        }},
        {force_garbage_collect, true}
    ].
