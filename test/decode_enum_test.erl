-module(decode_enum_test).

-include_lib("eunit/include/eunit.hrl").

e1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/enum.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/enum.avsc"),
    %Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term = #{
        <<"enumField">> => <<"DIAMONDS">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded NIF: ~p ~n", [Encoded]),
    Encoded1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("Encoded Erlavro: ~p ~n", [Encoded1]),


    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.


e2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/enum_null.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/enum_null.avsc"),
    %Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term = #{
        <<"enumField">> => <<"DIAMONDS">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded NIF: ~p ~n", [Encoded]),
    Encoded1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("Encoded Erlavro: ~p ~n", [Encoded1]),


    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

e3_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/enum_null.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/enum_null.avsc"),
    %Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder = avro:make_simple_encoder(SchemaJSON1, []),
    Term = #{
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded NIF: ~p ~n", [Encoded]),
    Encoded1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("Encoded Erlavro: ~p ~n", [Encoded1]),


    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.
