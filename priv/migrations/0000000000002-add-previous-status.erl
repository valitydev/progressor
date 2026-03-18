-module('0000000000002-add-previous-status').

-export([perform/2]).

-spec perform(_, _) -> _.
perform(Connection, MigrationOpts) ->
    NsId = proplists:get_value(namespace, MigrationOpts),
    #{
        processes := ProcessesTable
    } = prg_pg_utils:tables(NsId),
    ProcessesTableStr = string:replace(ProcessesTable, "\"", "'", all),
    {ok, _, [{IsPrevStatusExists}]} = epg_pool:query(
        Connection,
        "SELECT exists (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
        "  AND table_name = " ++ ProcessesTableStr ++ " AND column_name = 'previous_status')"
    ),
    _ =
        case IsPrevStatusExists of
            true ->
                ok;
            false ->
                %% create columns
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "ALTER TABLE " ++ ProcessesTable ++
                        "  ADD COLUMN previous_status process_status, "
                        "  ADD COLUMN status_changed_at TIMESTAMP WITH TIME ZONE"
                ),
                %% set values
                {ok, _} = epg_pool:query(
                    Connection,
                    "UPDATE " ++ ProcessesTable ++
                        " SET previous_status = status, status_changed_at = created_at"
                ),
                %% set NOT NULL constraint
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "ALTER TABLE " ++ ProcessesTable ++
                        "  ALTER COLUMN previous_status SET NOT NULL,"
                        "  ALTER COLUMN status_changed_at SET NOT NULL"
                )
        end,
    ok.
