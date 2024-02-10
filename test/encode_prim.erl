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

integer_null1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer_null.avsc">>),
    Pairs = [{<<"intField1">>, 111}, {<<"intField2">>, 22222}, {<<"intField3">>, 33333}, {<<"intField4">>, 4}, {<<"intField5">>, 555555555}, {<<"intField6">>, 6666}],
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

integer_null2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer_null.avsc">>),
    Pairs = [{<<"intField3">>, 33333}, {<<"intField5">>, 555555555}],
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

prim_binary_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/string.avsc">>),
    Pairs = [{<<"stringField">>, <<"abcdefg1234567890 987654321">>}],
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

strings_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/strings.avsc">>),
    Pairs = [
             {<<"stringField1">>, <<"abcdefg1234567890 987654321">>},
             {<<"stringField2">>, <<"22222222222 endof2">>},
             {<<"stringField3">>, <<"33333333333 endof3">>},
             {<<"stringField4">>, <<"44444444444 endof4">>}
            ],
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

types_test() ->
    SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
    Pairs = [
             {<<"intField">>, 123},
             {<<"longField">>, 1234567890},
             {<<"floatField">>, 33.44},
             {<<"doubleField">>, 11.22},
             {<<"stringField">>, <<"str0987654321">>},
             {<<"boolField">>, true},
             {<<"bytesField">>, <<1,2,3,4,5,6,7>>},
             {<<"stringField2">>, <<"str1234567890">>}
            ],
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

