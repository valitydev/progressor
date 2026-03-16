-module('0000000000003-add-init-status').

-export([perform/2]).

-spec perform(_, _) -> _.
perform(Connection, _MigrationOpts) ->
    {ok, _, [{IsInitStatusExists}]} = epg_pool:query(
        Connection,
        "select exists (SELECT 1 FROM pg_enum WHERE "
        "  enumtypid = 'process_status'::regtype and enumlabel = 'init')"
    ),
    _ =
        case IsInitStatusExists of
            true ->
                ok;
            false ->
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "ALTER TYPE process_status ADD VALUE 'init'"
                )
        end,
    ok.
