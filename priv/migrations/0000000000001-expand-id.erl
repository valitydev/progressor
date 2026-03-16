-module('0000000000001-expand-id').

-export([perform/2]).

-spec perform(_, _) -> _.
perform(Connection, MigrationOpts) ->
    NsId = proplists:get_value(namespace, MigrationOpts),
    #{
        processes := ProcessesTable,
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    lists:foreach(
        fun(T) ->
            TableStr = string:replace(T, "\"", "'", all),
            {ok, _, [{VarSize}]} = epg_pool:query(
                Connection,
                "SELECT character_maximum_length FROM information_schema.columns "
                "WHERE table_name = " ++ TableStr ++ " AND column_name = 'process_id'"
            ),
            case VarSize < 256 of
                true ->
                    {ok, _, _} = epg_pool:query(
                        Connection,
                        "ALTER TABLE " ++ T ++ "ALTER COLUMN process_id TYPE VARCHAR(256)"
                    );
                false ->
                    skip
            end
        end,
        [ProcessesTable, TaskTable, ScheduleTable, RunningTable, EventsTable]
    ),
    ok.
