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

complex_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>),
    Pairs = [{<<"arrayField">>, [<<"aaaaaa3">>, 1111, 77, <<"cccccccccccccc1">>, 0, 1, <<"sasd">>]}],
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

complex2_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_multi_type2.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),

    %Pairs = [{<<"arrayField">>, [1111, [1,2,3,4,5], 77]}],
    Pairs = [{<<"arrayField">>, [1, [2], 3]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded erlav: ~p ~n", [Encoded]),

    E2 = Encoder1(Term),
    ?debugFmt("Encoded erlavro: ~p ~n", [E2]),
    TE = Decoder(Encoded),
    ?debugFmt("decoded erlav->erlavro: ~p ~n", [TE]),

    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.
