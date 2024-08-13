-module(prg_utils).

-include("progressor.hrl").

%% API
-export([registered_name/2]).
-export([pipe/2]).
-export([format/1]).
-export([make_ns_opts/2]).

-spec registered_name(atom(), string()) -> atom().
registered_name(BaseAtom, PostfixStr) ->
    erlang:list_to_atom(erlang:atom_to_list(BaseAtom) ++ PostfixStr).

-spec pipe([function()], term()) -> term().
pipe([], Result) -> Result;
pipe(_Funs, {error, _} = Error) -> Error;
pipe(_Funs, {break, Result}) -> Result;
pipe([F | Rest], Acc) ->
    pipe(Rest, F(Acc)).

-spec format(term()) -> binary().
format(Term) when is_binary(Term) ->
    Term;
format(Term) ->
    unicode:characters_to_binary(io_lib:format("~64000p", [Term])).

-spec make_ns_opts(atom(), namespace_opts()) -> namespace_opts().
make_ns_opts(NsId, NsOpts) ->
    PresetDefaults = #{
        namespace => erlang:atom_to_binary(NsId),
        retry_policy => ?DEFAULT_RETRY_POLICY,
        worker_pool_size => ?DEFAULT_WORKER_POOL_SIZE,
        process_step_timeout => ?DEFAULT_STEP_TIMEOUT_SEC,
        task_scan_timeout => (?DEFAULT_STEP_TIMEOUT_SEC div 2) + 1,
        last_timer_repair => false
    },
    ConfigDefaults = application:get_env(progressor, defaults, #{}),
    Defaults = maps:merge(PresetDefaults, ConfigDefaults),
    maps:merge(Defaults, NsOpts).
