-module('0000000000004-fix-indexes').

-export([perform/2]).

-spec perform(_, _) -> _.
perform(Connection, MigrationOpts) ->
    NsId = proplists:get_value(namespace, MigrationOpts),
    #{
        tasks := TaskTable,
        schedule := ScheduleTable,
        running := RunningTable,
        events := EventsTable
    } = prg_pg_utils:tables(NsId),
    {ok, _, _} = drop_index(Connection, "process_idx"),
    {ok, _, _} = drop_index(Connection, "task_idx"),
    {ok, _, _} = create_index(Connection, EventsTable, "process_idx", "(process_id)"),
    {ok, _, _} = create_index(Connection, TaskTable, "process_idx", "(process_id)"),
    {ok, _, _} = create_index(Connection, ScheduleTable, "process_idx", "(process_id)"),
    {ok, _, _} = create_index(Connection, RunningTable, "task_idx", "(task_id)"),
    ok.

drop_index(Connection, IndexName) ->
    {ok, _, [{IsIndexExists}]} = epg_pool:query(
        Connection,
        "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)",
        [IndexName]
    ),
    case IsIndexExists of
        true ->
            epg_pool:query(Connection, "DROP INDEX " ++ IndexName);
        false ->
            {ok, [], []}
    end.

create_index(Connection, Table, Index, Fields) ->
    create_index(Connection, Table, Index, " HASH ", Fields).

create_index(Connection, Table, Index, IndexType, Fields) ->
    %% unwrap table name and wrap index name
    IndexName = "\"" ++ string:replace(Table, "\"", "", all) ++ "_" ++ Index ++ "\"",
    %% re-wrap for using in WHERE section
    IndexNameStr = string:replace(IndexName, "\"", "'", all),
    {ok, _, [{IsIndexExists}]} = epg_pool:query(
        Connection,
        "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = " ++ IndexNameStr ++ " )"
    ),
    case IsIndexExists of
        true ->
            {ok, [], []};
        false ->
            epg_pool:query(
                Connection,
                "CREATE INDEX " ++ IndexName ++
                    " on " ++ Table ++
                    " USING " ++ IndexType ++ " " ++ Fields
            )
    end.
