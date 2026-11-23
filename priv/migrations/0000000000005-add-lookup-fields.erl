-module('0000000000005-add-lookup-fields').

-export([perform/2]).

-define(SUB_ID_CONSTRAINT, "sub_id_requires_subnamespace").

-spec perform(_, _) -> _.
perform(Connection, MigrationOpts) ->
    NsId = proplists:get_value(namespace, MigrationOpts),
    #{processes := ProcessesTable} = prg_pg_utils:tables(NsId),
    {ok, _, _} = epg_pool:query(
        Connection,
        "ALTER TABLE " ++ ProcessesTable ++
            " ADD COLUMN IF NOT EXISTS subnamespace TEXT CHECK (subnamespace <> ''), "
            " ADD COLUMN IF NOT EXISTS sub_id TEXT CHECK (sub_id <> ''), "
            " ADD COLUMN IF NOT EXISTS aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[] "
            "   CHECK (array_position(aliases, '') IS NULL AND array_position(aliases, NULL) IS NULL)"
    ),
    {ok, _, [{IsConstraintExists}]} = epg_pool:query(
        Connection,
        "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2)",
        [unicode:characters_to_binary(ProcessesTable), ?SUB_ID_CONSTRAINT]
    ),
    _ =
        case IsConstraintExists of
            true ->
                ok;
            false ->
                {ok, _, _} = epg_pool:query(
                    Connection,
                    "ALTER TABLE " ++ ProcessesTable ++ " ADD CONSTRAINT " ++ ?SUB_ID_CONSTRAINT ++
                        " CHECK (sub_id IS NULL OR subnamespace IS NOT NULL)"
                )
        end,
    lists:foreach(
        fun({Suffix, Definition}) ->
            IndexName = "\"" ++ atom_to_list(NsId) ++ "_processes_" ++ Suffix ++ "\"",
            {ok, _, _} = epg_pool:query(
                Connection,
                "CREATE INDEX IF NOT EXISTS " ++ IndexName ++ " ON " ++ ProcessesTable ++ Definition
            )
        end,
        [
            {"subnamespace_idx", " (subnamespace)"},
            {"sub_id_idx", " (subnamespace, sub_id)"},
            {"aliases_idx", " USING GIN (aliases)"}
        ]
    ),
    ok.
