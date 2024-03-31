-module(decode_map_test).

-include_lib("eunit/include/eunit.hrl").

m1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    %{ok, SchemaJSON1} = file:read_file("test/tschema_map.avsc"),
    %Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    %Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term = #{
        <<"mapField">> => 
            #{
                <<"f1">> => 2,
                <<"f2">> => 4,
                <<"f3">> => 6,
                <<"f4">> => 8
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

m2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map_str.avsc">>),
    Term = #{
        <<"mapField">> => 
            #{
                <<"f1">> => <<"sadasdasd1111">>,
                <<"f2">> => <<"fgdfgdfgdf2222">>,
                <<"f3">> => <<"dgdgdfgdfgdf3333">>,
                <<"f4">> => <<"dgdfg4444">>
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% nullable scalar map
m3_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_str_null.avsc">>),
    Term = #{
        <<"mapField">> => 
            #{
                <<"f1">> => <<"sadasdasd1111">>,
                <<"f2">> => <<"fgdfgdfgdf2222">>,
                <<"f3">> => <<"dgdgdfgdfgdf3333">>,
                <<"f4">> => <<"dgdfg4444">>
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% map of arrays
m4_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_arr.avsc">>),
    Term = #{
        <<"key">> => 
            #{
                <<"f1">> => [1],
                <<"f2">> => [1,2,3,4,5,6,7,8,9],
                <<"f3">> => [745546456],
                <<"f4">> => [123123,564564,6786867,78978978,78978978,9999]
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% map of records
m5_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_rec.avsc">>),
    Term = #{
        <<"key">> => 
            #{
                <<"f1">> => #{<<"f1">> => <<"lallalala">>, <<"f2">> => 9999},
                <<"f2">> => #{<<"f1">> => <<"kokkokoko">>}
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.
