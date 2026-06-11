-module(progressor_action).

-include("progressor.hrl").

-export([new/0]).
-export([instant/0]).
-export([set_timeout/1]).
-export([set_timeout/2]).
-export([set_deadline/1]).
-export([set_deadline/2]).
-export([set_timer/1]).
-export([set_timer/2]).
-export([unset_timer/0]).
-export([unset_timer/1]).
-export([remove/0]).
-export([remove/1]).
-export([mark_removal/0]).
-export([mark_removal/1]).
-export([marshal_timer/1]).

-type seconds() :: timeout_sec().
-type datetime() :: calendar:datetime() | binary().
-type timer() :: {timeout, seconds()} | {deadline, datetime()}.
-type t() :: undefined | action().

-export_type([t/0, action/0, timer/0, seconds/0]).

-spec new() -> t().
new() ->
    undefined.

-spec instant() -> t().
instant() ->
    set_timeout(0).

-spec set_timeout(seconds()) -> t().
set_timeout(Seconds) ->
    set_timeout(Seconds, new()).

-spec set_timeout(seconds(), t()) -> set_timer_action().
set_timeout(Seconds, _Action) when is_integer(Seconds), Seconds >= 0 ->
    #{set_timer => marshal_timer({timeout, Seconds})}.

-spec set_deadline(datetime()) -> t().
set_deadline(Deadline) ->
    set_deadline(Deadline, new()).

-spec set_deadline(datetime(), t()) -> set_timer_action().
set_deadline(Deadline, _Action) ->
    #{set_timer => marshal_timer({deadline, Deadline})}.

-spec set_timer(timer()) -> t().
set_timer(Timer) ->
    set_timer(Timer, new()).

-spec set_timer(timer(), t()) -> set_timer_action().
set_timer(Timer, _Action) ->
    #{set_timer => marshal_timer(Timer)}.

-spec unset_timer() -> unset_timer.
unset_timer() ->
    'unset_timer'.

-spec unset_timer(t()) -> unset_timer.
unset_timer(_Action) ->
    'unset_timer'.

-spec remove() -> remove_action().
remove() ->
    #{remove => true}.

-spec remove(t()) -> remove_action().
remove(_Action) ->
    #{remove => true}.

-spec mark_removal() -> remove_action().
mark_removal() ->
    remove().

-spec mark_removal(t()) -> remove_action().
mark_removal(Action) ->
    remove(Action).

-spec marshal_timer(timer()) -> timestamp_sec().
marshal_timer({timeout, Seconds}) when is_integer(Seconds), Seconds >= 0 ->
    erlang:system_time(second) + Seconds;
marshal_timer({deadline, {_, _} = Dt}) ->
    calendar:datetime_to_gregorian_seconds(Dt) - ?EPOCH_DIFF;
marshal_timer({deadline, Bin}) when is_binary(Bin) ->
    calendar:rfc3339_to_system_time(unicode:characters_to_list(Bin), [{unit, second}]).
