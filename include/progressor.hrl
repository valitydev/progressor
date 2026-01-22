%%%
%%% Base entities
%%%

-define(CLEAR_PROCESS_KEYS, [
    process_id,
    status,
    detail,
    aux_state,
    metadata,
    corrupted_by,
    previous_status,
    status_changed_at
]).

-type process() :: #{
    process_id := id(),
    status := process_status(),
    detail => binary(),
    aux_state => binary(),
    metadata => map(),
    history => [event()],
    corrupted_by => task_id(),
    range => history_range(),
    last_event_id => event_id(),
    initialization => task_id(),
    previous_status => process_status(),
    status_changed_at => timestamp_sec()
}.

-type task() :: #{
    task_id => task_id(),
    process_id := id(),
    task_type := task_type(),
    status := task_status(),
    scheduled_time := timestamp_sec(),
    running_time => timestamp_sec(),
    finished_time => timestamp_sec(),
    args => binary(),
    metadata => map(),
    idempotency_key => binary(),
    response => binary(),
    blocked_task => task_id(),
    last_retry_interval => non_neg_integer(),
    attempts_count => non_neg_integer(),
    context => binary()
}.

-type event() :: #{
    process_id := id(),
    task_id := task_id(),
    event_id := event_id(),
    timestamp := timestamp_sec(),
    metadata => #{format => pos_integer()},
    payload := binary()
}.

-type process_trace_unit() :: #{
    task_id := task_id(),
    task_type := task_type(),
    task_status := task_status(),
    scheduled := timestamp_sec(),
    running => timestamp_sec(),
    finished => timestamp_sec(),
    args => binary(),
    metadata => map(),
    idempotency_key => binary(),
    response => term(),
    retry_interval => non_neg_integer(),
    retry_attempts => non_neg_integer(),
    event_id => event_id(),
    event_timestamp => timestamp_sec(),
    event_metadata => #{format => pos_integer()},
    event_payload => binary()
}.

-type process_flat_trace() :: [process_trace_unit()].

%%%
%%% Config options
%%%
-type namespace_opts() :: #{
    namespace := id(),
    storage := storage_opts(),
    processor := processor_opts(),
    notifier => notifier_opts(),
    retry_policy => retry_policy(),
    worker_pool_size => pos_integer(),
    process_step_timeout => timeout_sec(),
    task_scan_timeout => timeout_sec(),
    call_scan_timeout => timeout_sec(),
    last_timer_repair => boolean()
}.

-type notifier_opts() :: #{
    client => atom(),
    options => term()
}.

-type processor_opts() :: #{
    client := module(),
    options => term()
}.

-type storage_handler() :: prg_pg_backend.

-type storage_opts() :: #{
    client := storage_handler(),
    options => storage_handler_opts()
}.

-type storage_handler_opts() :: term().

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
% <<"init">> | <<"running">> | <<"error">>

-type task_status() :: binary().
% <<"waiting">> | <<"running">> | <<"blocked">> | <<"error">> | <<"finished">> | <<"cancelled">>

-type task_type() :: binary().
% <<"init">> | <<"call">> | <<"notify">> | <<"repair">> | <<"timeout">>

-type task_t() :: init | call | repair | notify | timeout | remove.
-type task_origin() :: {pid(), reference()}.
-type task_header() :: {task_t(), task_origin() | undefined}.

-type namespace_id() :: atom().

-type recipient() :: internal | external.

-type history_range() :: #{
    offset => non_neg_integer(),
    limit => non_neg_integer(),
    direction => forward | backward
}.

-type process_updates() :: #{
    process_id := id(),
    _ => _
}.

-type processor_intent() :: #{
    events := [event()],
    action => action(),
    response => term(),
    aux_state => binary(),
    metadata => map()
}.
-type processor_exception(Reason) :: {exception, _Class, Reason}.
-type maybe_transient_error_reason() ::
    processor_exception({woody_error, {_, atom(), _}})
    | any().
-type non_transient_error_reason() ::
    processor_exception({woody_error, {_, result_unexpected, _}})
    | processor_exception(any()).
-type process_result() ::
    {ok, processor_intent()}
    | {error, non_transient_error_reason() | maybe_transient_error_reason()}.
%% NOTE: There is a crutch in MG-compatibility retry-ability check. See
%% `prg_worker:is_retryable/5'.
%%
%% Erroneous results can be treated as transient errors only during
%% 'timeout' tasks, with the following caveats:
%%
%% - A `processor_exception/1' can be treated as transient only if it is not a
%%   'result_unexpected' woody error;
%%
%% - Other possible errors matching `maybe_transient_error_reason/0'
%%   are always treated as transient when the retry policy applies
%%   (i.e., attempts are not exhausted, and the error is not marked as
%%   non-retryable).

-type action() :: #{set_timer := timestamp_sec(), remove => true} | unset_timer.

-type task_result() :: #{
    task_id := task_id(),
    status := task_status(),
    finished_time => timestamp_sec(),
    response => binary()
}.

-type timestamp_ms() :: non_neg_integer().
-type timestamp_sec() :: non_neg_integer().
-type timeout_sec() :: non_neg_integer().
-type timeout_ms() :: non_neg_integer().

%%%
%%% Constants
%%%
-define(DEFAULT_STEP_TIMEOUT_SEC, 60).

-define(DEFAULT_CALL_SCAN_TIMEOUT_SEC, 3).

-define(DEFAULT_RETRY_POLICY, #{
    %% second
    initial_timeout => 5,
    backoff_coefficient => 1.0,
    max_attempts => 3
}).

-define(DEFAULT_WORKER_POOL_SIZE, 10).

-define(EPOCH_DIFF, 62167219200).

-define(NEW_PROCESS(ID), #{
    process_id => ProcessId,
    status => <<"init">>,
    previous_status => <<"init">>,
    created_at => erlang:system_time(second),
    status_changed_at => erlang:system_time(second)
}).
