-module(prg_test_utils).

-export([cleanup/1]).

%% @doc Deletes all database records for testing purposes only.
%%
%% This function truncates/resets the database tables to a clean state.
%% It is designed exclusively for use in test setups and teardowns
%% (between test cases and suites) to ensure test isolation.
%%
%% IMPORTANT: This function is NOT intended for production use.
%% Calling it in a production environment will result in IRREVERSIBLE
%% DATA LOSS and severe application disruption.
%%
%% The function is exported to allow test frameworks (e.g., Common Test, EUnit)
%% to access it, but it should be considered a private interface for testing.
%%
%% @end
-spec cleanup(_) -> _.
cleanup(#{ns := NsId} = _Opts) ->
    {ok, NSs} = application:get_env(progressor, namespaces),
    NsOpts = maps:get(NsId, NSs),
    #{storage := StorageOpts} = prg_utils:make_ns_opts(NsId, NsOpts),
    ok = prg_storage:cleanup(StorageOpts, NsId).
