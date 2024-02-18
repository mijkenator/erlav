-module(union_tests).

-include_lib("eunit/include/eunit.hrl").


scalar_union_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/union_null.avsc">>),
    Pairs = [{<<"field1">>, <<"kokokkokok">>}, {<<"field2">>, 22222}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

