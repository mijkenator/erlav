-module(complex_arrays).

-include_lib("eunit/include/eunit.hrl").

a1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple3.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple3.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3,4], [<<"asdad">>, <<"121212">>], [1,2,4]
        ],
        <<"endf">> => <<"kok">>
    },
    Encoded0 = Encoder(Term1),
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("NIF-Encoded: ~p ~n", [Encoded1]),
    ?debugFmt("Erl-Encoded: ~p ~n", [Encoded0]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

a2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple3.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple3.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3,4], 567, [<<"asdad">>, <<"121212">>], [1,2,4], 799, [<<"stry">>, 123, <<"kkokoko">>, 99]
        ],
        <<"endf">> => <<"kok">>,
        <<"arr2">> => [1,2,3]
    },
    Encoded0 = Encoder(Term1),
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("NIF-Encoded: ~p ~n", [Encoded1]),
    ?debugFmt("Erl-Encoded: ~p ~n", [Encoded0]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.
