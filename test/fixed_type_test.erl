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

%% ============================================================
%% Gaps identified in review: erlav_decode (legacy iterator path),
%% map<fixed>, array<record<fixed>>, missing required fixed field,
%% fixed-vs-string/long union ambiguity, zero-size fixed.
%% ============================================================

%% erlav_decode (the non-fast, iterator-based legacy path) only ever
%% handles obj_type == 0 && scalar_type >= 0 (plain scalar fields) --
%% decode() for that path has no fixed/array/map/record/enum/union
%% branches at all. This documents that known limitation (same
%% convention as correctness_test.erl's decode_paths_string_test) rather
%% than silently leaving it uncovered: fixedField is dropped and every
%% field after it desyncs, exactly the failure mode #39 originally
%% reported for erlav_decode_fast before it was fixed there.
legacy_decode_does_not_support_fixed_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),
    Term = #{
        <<"beforeField">> => 111,
        <<"fixedField">> => <<1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16>>,
        <<"afterField">> => 222
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode(SchemaId, Encoded),
    ?debugFmt("legacy decode result (fixedField unsupported): ~p ~n", [Re1]),
    ?assertNot(maps:is_key(<<"fixedField">>, Re1)),
    ?assertNotEqual(222, maps:get(<<"afterField">>, Re1, notfound)).

%% map<fixed> -- values are a fixed-size binary rather than a scalar.
map_of_fixed_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_map.avsc">>),
    Term = #{<<"mapField">> => #{
        <<"k1">> => <<1,2,3,4>>,
        <<"k2">> => <<5,6,7,8>>
    }},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

%% array<record> where the record itself has a fixed field -- exercises
%% decode_fixed reached via decode()'s per-record-field loop rather than
%% directly off decodevalue()'s obj_type switch.
array_of_record_with_fixed_field_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_array_record.avsc">>),
    Term = #{<<"arrayField">> => [
        #{<<"id">> => 1, <<"hash">> => <<1,2,3,4>>},
        #{<<"id">> => 2, <<"hash">> => <<5,6,7,8>>}
    ]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

%% A required (non-union, non-array) fixed field absent from the input
%% map has no branch in encode_field_or_default's missing-field
%% defaulting -- documents the current behavior (silently omitted, no
%% bytes written for it) so a future change to reject/require it instead
%% is a deliberate, visible test update rather than an unnoticed
%% regression either way.
missing_required_fixed_field_is_silently_omitted_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type.avsc">>),
    Term = #{<<"beforeField">> => 111, <<"afterField">> => 222},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded (fixedField absent): ~p ~n", [Encoded]),
    %% No AvroException -- the field is just skipped, unlike a missing
    %% non-nullable scalar field (e.g. long), which does throw.
    ?assertEqual(<<222,1,188,3>>, Encoded).

%% A union of ["string", "long", fixed] with a binary input is ambiguous
%% between the string and fixed branches. encodeunion tries members in
%% declaration order and accepts the first one that succeeds, so string
%% (which accepts any binary) always wins -- fixed is never chosen, no
%% matter the input's actual length. Documents this as known behavior
%% (same category of ambiguity as #13's record-vs-map union issue) rather
%% than silently leaving it untested.
union_prefers_string_over_fixed_for_binary_input_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_union_ambiguous.avsc">>),
    Term = #{<<"f">> => <<1,2,3,4>>}, % exactly 4 bytes, MD5's declared size
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    %% Round-trips (the bytes are indistinguishable on the wire either
    %% way), but via the string branch, not the fixed branch -- the value
    %% comes back correctly only because string and fixed happen to
    %% decode to the same Erlang binary. A caller relying on this field
    %% actually being encoded as fixed would be misled.
    ?assertEqual(Term, Re1).

%% Zero-size fixed is legal Avro (a fixed field that's always zero bytes
%% on the wire).
zero_size_fixed_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/fixed_type_zero_size.avsc">>),
    Term = #{<<"f">> => <<>>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assertEqual(<<>>, Encoded),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Re1).
