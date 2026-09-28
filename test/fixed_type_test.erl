-module(fixed_type_test).

-include_lib("eunit/include/eunit.hrl").

%% Avro "fixed" -- a fixed-size binary value with no wire-encoded length
%% prefix (unlike bytes/string, whose length is read from the data). See
%% #39: this type had no implementation anywhere in the schema parser or
%% decoder, and a field using it was silently dropped by both erlav's own
%% encoder and decoder (no bytes written, no bytes read -- and worse, when
%% decoding another Avro implementation's bytes containing real fixed-size
%% data, every field after the fixed field came back desynced/wrong).

%% ============================================================
%% fixedField as a direct (non-union) record field
%% ============================================================

fixed_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),
    Term = #{
        <<"beforeField">> => 111,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"afterField">> => 222
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

%% A fixed field is a fixed number of bytes with no length prefix at all
%% (contrast bytes/string) -- verify the fields on either side of it in
%% the record are still read from the correct offset (this is exactly
%% what a missing/broken decode_fixed would desync, per #39).
fixed_does_not_desync_surrounding_fields_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),
    Term = #{
        <<"beforeField">> => 999999999,
        <<"fixedField">> => <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
        <<"afterField">> => 888888888
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(999999999, maps:get(<<"beforeField">>, Re1)),
    ?assertEqual(888888888, maps:get(<<"afterField">>, Re1)).

%% Wrong-size binary for the field's declared fixed size must fail loudly
%% (a clean {error,_,_}), not silently truncate/pad or write the wrong
%% number of bytes onto the wire.
fixed_wrong_size_rejected_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),
    Term = #{
        <<"beforeField">> => 1,
        <<"fixedField">> => <<1,2,3>>, % declared size is 16
        <<"afterField">> => 2
    },
    ?assertMatch({error, _, _}, erlav_nif:erlav_encode(SchemaId, Term)).

%% Cross-validate against erlavro: bytes produced by erlav's encoder for a
%% fixed field must decode with erlavro's own decoder, and bytes produced
%% by erlavro's encoder must decode correctly with erlav_decode_fast --
%% same convention used by decode_enum_test.erl/negative_block_count_test.erl
%% for other types.
erlavro_interop_test() ->
    {ok, SchemaJSON} = file:read_file("test/fixed_type.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Decoder = avro:make_simple_decoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),

    Term = #{
        <<"beforeField">> => 111,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"afterField">> => 222
    },

    %% erlav bytes decode correctly with erlavro
    ErlavEncoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroDecodedOfErlavBytes = maps:from_list(Decoder(ErlavEncoded)),
    ?assertEqual(Term, ErlavroDecodedOfErlavBytes),

    %% erlavro bytes decode correctly with erlav_decode_fast
    TermList = [{<<"beforeField">>, 111},
                {<<"fixedField">>, <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>},
                {<<"afterField">>, 222}],
    ErlavroEncoded = iolist_to_binary(Encoder(TermList)),
    ErlavDecodedOfErlavroBytes = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(Term, ErlavDecodedOfErlavroBytes),

    %% Byte-for-byte identical, since fixed has one unambiguous wire form.
    ?assertEqual(ErlavroEncoded, ErlavEncoded).

%% ============================================================
%% fixedField as a nullable union member (["null", fixed])
%% ============================================================

fixed_nullable_present_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_nullable.avsc">>),
    Term = #{
        <<"beforeField">> => 111,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"afterField">> => 222
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

fixed_nullable_erlavro_interop_test() ->
    {ok, SchemaJSON} = file:read_file("test/fixed_type_nullable.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_nullable.avsc">>),

    TermList = [{<<"beforeField">>, 111},
                {<<"fixedField">>, <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>},
                {<<"afterField">>, 222}],
    ErlavroEncoded = iolist_to_binary(Encoder(TermList)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(#{
        <<"beforeField">> => 111,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"afterField">> => 222
    }, Re1).

%% ============================================================
%% priv/tschema1.avsc -- the repo's own bundled reference schema, which
%% declares a fixedField and previously could not even be registered via
%% erlav_init/1 without hitting the unimplemented "fixed" type (#39).
%% ============================================================

tschema1_loads_and_roundtrips_fixed_field_test() ->
    SchemaId = erlav_nif:erlav_init(<<"priv/tschema1.avsc">>),
    Term = #{
        <<"intField">> => 42,
        <<"longField">> => 123456789,
        <<"stringField">> => <<"hello">>,
        <<"boolField">> => true,
        <<"floatField">> => 1.5,
        <<"doubleField">> => 2.5,
        <<"bytesField">> => <<1,2,3>>,
        <<"arrayField">> => [1.1, 2.2],
        <<"mapField">> => #{<<"k">> => #{<<"label">> => <<"foo">>}},
        <<"unionField">> => true,
        <<"enumField">> => <<"B">>,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"recordField">> => #{<<"label">> => <<"root">>, <<"children">> => []}
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(
        <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        maps:get(<<"fixedField">>, Re1)
    ),
    ?assertEqual(42, maps:get(<<"intField">>, Re1)),
    ?assertEqual(#{<<"label">> => <<"root">>, <<"children">> => []},
                 maps:get(<<"recordField">>, Re1)).
