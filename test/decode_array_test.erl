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

array_of_recs2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs_nullable.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2,
                <<"rec3field">> => 4
             },
            #{
                <<"rec1field">> => 22,
                <<"rec3field">> => 34
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_arr3_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array_array.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    %Term = #{
    %    <<"arrayField">> => [
    %        [ [1,2,3], [4,5,6], [7,8,9] ],
    %        [ [11,22,33], [14,15,16], [117,118,119] ]
    %    ]
    %},
    Term = #{
        <<"arrayField">> => [
            [ [1,1,1] ]
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_arr4_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,1,1], [2,3,4], [44], [9,8,7]
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3], [4,5,6], [7,8,9], [0], [0], [1,1,1,1,1,1], [5]
        ]
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

%
% test for array of nullable arrays
%
array_of_arr5_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_many.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_many.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,2,3,4], [<<"asdad">>, <<"121212">>]
        ],
        <<"endf">> => <<"kokoko">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),

    ok.

array_of_arr6_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple1.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            1,2,3,4
        ],
        <<"endlf">> => 111
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),

    ok.

array_of_arr7_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array2.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,1,1], [2,3,4], [44], [9,8,7]
        ],
        <<"endl1">> => 11
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3], [4,5,6], [7,8,9], [0], [0], [1,1,1,1,1,1], [5]
        ],
        <<"endl1">> => 11
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

array_of_arr8_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple2.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,2], [1,1,1,1], [<<"str1">>, <<"str2">>], [1], [<<"1122233">>]
        ],
        <<"endf">> => <<"kokokoko">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    ?debugFmt("=============================================!!!!!!!!!!!======================", []),
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3,4], [<<"asdad">>, <<"121212">>]
        ],
        <<"endf">> => <<"kokoko">>
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

array_bad_type_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    
    Pairs = [{<<"arrayField">>, [<<"2">>]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    ok.

array_bad_type1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple1.avsc">>),
    
    Pairs = [{<<"arrayField">>, [<<"2">>]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    Msg1 = erlav_nif:erlav_encode(SchemaId, maps:from_list([{<<"arrayField">>, [2, <<"2">>]}])),
    ?debugFmt("Encoded: ~p ~n", [Msg1]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg1),

    ok.

array_bad_type2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    
    Pairs = [{<<"arrayField">>, [2]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    ok.

complex_array_bad_type_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>),
    Pairs = [{<<"arrayField">>, [2.2]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),

    ?assert({error, "Rec:Interop field:arrayField", 8} == Encoded),
    ok.
