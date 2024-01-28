-module(encode_prim).

-include_lib("eunit/include/eunit.hrl").


integer_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer.avsc">>),
    Pairs = [{<<"intField1">>, 111}, {<<"intField2">>, 22222}, {<<"intField3">>, 33333}, {<<"intField4">>, 4}, {<<"intField5">>, 555555555}, {<<"intField6">>, 6666}, {<<"intField7">>, 7777777}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

prim_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/prims.avsc">>),
    Pairs = [{<<"intField1">>, 111}, {<<"floatField2">>, 22.22}, {<<"intField3">>, 33333}, {<<"doubleField4">>, 44.44}, {<<"intField5">>, 555555555}, {<<"intField6">>, 6666}, {<<"intField7">>, 7777777}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        case is_float(Value) of
            true ->
                Diff = abs(Value1 - Value),
                ?assert(Diff < 0.00001)
            ;_ -> 
                ?assertEqual(Value, Value1)
        end
    end, Pairs),
    ok.

prim_fast_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/prims.avsc">>),
    Pairs = [{<<"intField1">>, 111}, {<<"floatField2">>, 22.22}, {<<"intField3">>, 33333}, {<<"doubleField4">>, 44.44}, {<<"intField5">>, 555555555}, {<<"intField6">>, 6666}, {<<"intField7">>, 7777777}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        case is_float(Value) of
            true ->
                Diff = abs(Value1 - Value),
                ?assert(Diff < 0.00001)
            ;_ -> 
                ?assertEqual(Value, Value1)
        end
    end, Pairs),
    ok.
