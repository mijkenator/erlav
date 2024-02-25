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

    Pairs = [{<<"arrayField">>, [1111, [1,2,3,4,5], 77]}],
    %Pairs = [{<<"arrayField">>, [1, [2], 3]}],
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

array_of_recs_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 111,
                <<"rec2field">> => <<"str1">>,
                <<"rec3field">> => 3333,
                <<"rec4field">> => 1,
                <<"rec5field">> => true,
                <<"rec6field">> => 1.11,
                <<"rec7field">> => 2.222
             },
            #{
                <<"rec1field">> => 2111,
                <<"rec2field">> => <<"2str1">>,
                <<"rec3field">> => 23333,
                <<"rec4field">> => 21,
                <<"rec5field">> => true,
                <<"rec6field">> => 21.11,
                <<"rec7field">> => 22.222
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

array_of_recs0_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs0.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_recs1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs1.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.
