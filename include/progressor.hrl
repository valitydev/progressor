
%%%
%%% Base entities
%%%
-type process() :: #{
    process_id := id(),
    status := process_status(),
    detail => binary(),
    metadata => binary(),
    history => [event()]
    %% timer => timestamp_sec()
}.

-type task() :: #{
    task_id => task_id(),
    process_id := id(),
    task_type := task_type(),
    status := task_status(),
    scheduled_time := timestamp_ms(),
    running_time => timestamp_ms(),
    args => binary(),
    metadata => map(),
    idempotency_key => binary(),
    response => binary(),
    blocked_task => task_id(),
    last_retry_interval => non_neg_integer(),
    attempts_count => non_neg_integer()
}.

-type event() :: #{
    process_id := id(),
    task_id := task_id(),
    event_id := event_id(),
    timestamp := timestamp_sec(),
    metadata => #{format => pos_integer()},
    payload := binary()
}.

%%%
%%% Config options
%%%
-type namespace_opts() :: #{
    storage := storage_opts(),
    processor := processor_opts(),
    retry_policy => retry_policy(),
    worker_pool_size => pos_integer(),
    process_step_timeout => timeout_sec(),
    task_scan_timeout => timeout_sec(),
    last_timer_repair => boolean()
}.

-type processor_opts() :: #{
    client := module(),
    options => term()
}.

-type storage_opts() :: #{
    client := module(),
    options => term()
}.

-type retry_policy() :: #{
    initial_timeout => timeout_sec(),
    backoff_coefficient => float(),
    max_timeout => timeout_sec(),
    max_attempts => pos_integer(),
    non_retryable_errors => [term()]
}.

%%%
%%% Other types
%%%
-type id() :: binary().
-type event_id() :: pos_integer().
-type task_id() :: pos_integer().

-type process_status() :: binary().
% <<"running">> | <<"error">>

-type task_status() :: binary().
% <<"waiting">> | <<"running">> | <<"blocked">> | <<"error">> | <<"finished">> | <<"cancelled">>

-type task_type() :: binary().
% <<"init">> | <<"call">> | <<"notify">> | <<"repair">> | <<"timeout">>

-type task_t() :: init | call | repair | notify | timeout.
-type task_origin() :: {pid(), reference()}.
-type task_header() :: {task_t(), task_origin() | undefined}.

-type namespace_id() :: atom().

-type history_range() :: #{
    offset => non_neg_integer(),
    limit => pos_integer()
}.

-type process_result() ::
    {ok, #{
        events := [event()],
        action => action(),
        response => term(),
        aux_state => binary(),
        metadata => map()
    }}
    | {error, binary()}.

-type action() :: #{set_timer := pos_integer(), remove => true} | unset_timer.

-type task_result() :: #{
    task_id := task_id(),
    status := task_status(),
    finish_time => timestamp_sec(),
    response => binary()
}.

-type timestamp_ms() :: non_neg_integer().
-type timestamp_sec() :: non_neg_integer().
-type timeout_sec() :: non_neg_integer().

%%%
%%% Constants
%%%
-define(DEFAULT_STEP_TIMEOUT_SEC, 60).

-define(DEFAULT_RETRY_POLICY, #{
    initial_timeout => 5, %% second
    backoff_coefficient => 1.0,
    max_attempts => 3
}).

-define(DEFAULT_WORKER_POOL_SIZE, 10).
