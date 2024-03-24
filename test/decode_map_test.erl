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
    ?assert(true == false),
    ok.

% map of arrays
m4_test() ->
    ?assert(true == false),
    ok.

% map of records
m5_test() ->
    ?assert(true == false),
    ok.
