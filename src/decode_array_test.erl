-module(decode_array_test).

-include_lib("eunit/include/eunit.hrl").


scalar_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Pairs = [{<<"arrayField">>, [1,2,3,4,5,6,7,8,9,10,9,8,7,6,5,4,3,2,1]}],
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

str_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    Pairs = [{<<"arrayField">>, [<<"aaaaaa3">>, <<"bbbbbb2">>, <<"cccccccccccccc1">>]}],
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
