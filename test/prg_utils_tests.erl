-module(prg_utils_tests).

-include_lib("eunit/include/eunit.hrl").

-spec valid_identifier_test_() -> _.
valid_identifier_test_() ->
    Valid = [<<"a">>, <<"NULLfoo">>, <<" space ">>, <<"юникод"/utf8>>, <<"{\"x\",\\}">>],
    Invalid = [
        <<>>,
        <<0>>,
        <<"a", 0, "b">>,
        %% invalid UTF-8: unicode:characters_to_binary/1 returns {error, ...}
        <<"a", 255>>,
        %% truncated UTF-8 sequence: unicode:characters_to_binary/1 returns {incomplete, ...}
        <<"a", 16#D0>>,
        "text",
        [<<"a">>],
        undefined,
        null,
        1
    ],
    [?_assert(prg_utils:valid_identifier(V)) || V <- Valid] ++
        [?_assertNot(prg_utils:valid_identifier(V)) || V <- Invalid].
