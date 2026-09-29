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

% map of arrays
m6_test() ->
    %?assert(true == false),
    SchemaId = erlav_nif:erlav_init(<<"test/map_arr_extra.avsc">>),
    Term = #{
        <<"key">> => 
            #{
                <<"f1">> => [1]
             },
        <<"key1">> => 
            #{
                <<"f2">> => [2]
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded1: ~p ~n", [Encoded]),
    
    {ok, SchemaJSON1} = file:read_file("test/map_arr_extra.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    E2 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("Encoded2: ~p ~n", [E2]),
    M = to_map(Decoder(Encoded)),
    ?debugFmt("Decoded result: ~p ~n", [M]),
    ?debugFmt("============================= ~n ~n", []),

    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% map whose *values* (not the map field itself) are a union of scalars,
% e.g. {"type": "map", "values": ["null", "string"]}. Regression test:
% SchemaItem previously left scalar_type unset (-1) for this shape (only
% obj_field was set), which erlav_decode_fast relied on directly and would
% badarg on. erlav_encode happened to self-heal by recomputing the scalar
% type from obj_field on every call, so only decode was actually broken --
% but both sides now read the same precomputed scalar_type.
m7_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>),
    Term = #{
        <<"mapField">> =>
            #{
                <<"k1">> => <<"hello">>,
                <<"k2">> => <<"world">>
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% #32: m7_test above only ever self-round-trips through erlav's own
% encoder (encode -> decode, both sides previously agreeing to skip the
% per-value union type-index byte). Decoding spec-compliant bytes from
% another Avro implementation that *does* write the discriminator byte
% used to desync the cursor and crash the whole BEAM VM via a garbage
% length handed to enif_make_new_binary. This decodes erlavro-encoded
% bytes directly, closing that gap.
m7_erlavro_interop_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => <<"hello">>}},

    {ok, SchemaJSON} = file:read_file("test/map_union_scalar_values.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro Encoded: ~p ~n", [ErlavroEncoded]),

    Re1 = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

% Same map-of-union shape, but the union's non-null member is itself
% complex (a record) rather than a scalar -- exercises decode_union's
% decodevalue() fallback (not just decode_scalar()) reached via
% decode_map's values_are_union branch.
m8_map_union_record_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_record_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => #{<<"label">> => <<"foo">>}}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

m8_erlavro_interop_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_record_values.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/map_union_record_values.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    TermList = [{<<"mapField">>, [{<<"k1">>, [{<<"label">>, <<"foo">>}]}]}],
    ErlavroEncoded = iolist_to_binary(Encoder(TermList)),
    ?debugFmt("erlavro Encoded: ~p ~n", [ErlavroEncoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(#{<<"mapField">> => #{<<"k1">> => #{<<"label">> => <<"foo">>}}}, Re1).

% Map values are a non-nullable multi-branch union (["string", "long"]) --
% exercises encodeunion/decode_union's non-nullable branch-search path
% (as opposed to m7/m8's nullable-union path) via decode_map.
m9_map_union_multi_scalar_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_multi_scalar_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => <<"hello">>, <<"k2">> => 42}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

% ============================================================
% Gaps identified in review of #32's fix: negative block-count
% interaction, empty map, map<union> nested in array, array-valued
% union member, explicit null value, and the legacy decode path.
% ============================================================

% #15's negative block-count encoding (use_negative_block_count) and
% #32's values_are_union handling were each added independently and
% never exercised together -- both touch encodemap's `target`
% scratch-buffer bookkeeping, so this locks in that the combination
% produces the same bytes erlavro's default encoder does (erlavro emits
% the negative form by default; see #15).
m10_negative_block_count_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>, [use_negative_block_count]),
    Term = #{<<"mapField">> => #{<<"k1">> => <<"hello">>, <<"k2">> => <<"world">>}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1),

    {ok, SchemaJSON} = file:read_file("test/map_union_scalar_values.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    TermList = [{<<"mapField">>, [{<<"k1">>, <<"hello">>}, {<<"k2">>, <<"world">>}]}],
    ErlavroEncoded = iolist_to_binary(Encoder(TermList)),
    ?debugFmt("erlavro Encoded: ~p ~n", [ErlavroEncoded]),
    ?assertEqual(ErlavroEncoded, Encoded).

% An empty map still takes the values_are_union branch (the check
% happens before the item-count loop even starts) -- verify that
% branch's block-header bookkeeping doesn't misbehave with zero entries.
m11_empty_map_union_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>),
    Term = #{<<"mapField">> => #{}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assertEqual(<<0>>, Encoded),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Re1).

% map<union> nested inside an array -- exercises the interaction between
% #32's fix (decode_map/encodemap -> encodeunion/decode_union) and #44's
% fix (decode_array's complex-array-of-one-type branch dispatching
% through decodevalue() instead of decode()) in the same call chain:
% decode_array -> decodevalue -> decode_map -> decode_union. Neither
% fix's tests exercised the other's code path.
m12_array_of_map_union_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_map_union_values.avsc">>),
    Term = #{<<"arrayField">> => [
        #{<<"k1">> => <<"hello">>},
        #{<<"k2">> => <<"world">>}
    ]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

% Map values union whose non-null member is an array (rather than the
% scalar/record shapes m7/m8 already cover) -- exercises decode_union's
% array-dispatch branch (obj_type == 2 -> decode_array) and encodeunion's
% matching encode-side branch, reached via decode_map/encodemap's
% values_are_union path.
m13_map_union_array_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_array_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => [1,2,3]}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

m13_erlavro_interop_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_array_values.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/map_union_array_values.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    TermList = [{<<"mapField">>, [{<<"k1">>, [1,2,3]}]}],
    ErlavroEncoded = iolist_to_binary(Encoder(TermList)),
    ?debugFmt("erlavro Encoded: ~p ~n", [ErlavroEncoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(#{<<"mapField">> => #{<<"k1">> => [1,2,3]}}, Re1).

% An explicit null/undefined value for a map-of-nullable-union entry
% currently fails to encode -- the same pre-existing "nullable field
% explicit undefined" limitation that affects every nullable type in
% this codebase (record fields included), not something #32's fix
% introduced or is meant to address. Documented here (same convention as
% correctness_test.erl's decode_paths_string_test) so a future change to
% that general limitation is a deliberate, visible test update rather
% than a silent behavior change for this specific shape.
m14_explicit_null_value_currently_rejected_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => undefined}},
    ?assertMatch({error, _, _}, erlav_nif:erlav_encode(SchemaId, Term)).

% The legacy iterator-based erlav_decode only ever handles flat scalar
% record fields (see correctness_test.erl's decode_paths_string_test for
% the same limitation with plain string fields) -- a mapField whose
% values are a union is silently dropped entirely, same as it would be
% for a plain map or any other non-scalar field. Documents that this
% known limitation still applies to the values_are_union shape rather
% than leaving it unverified.
m15_legacy_decode_does_not_support_map_union_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>),
    Term = #{<<"mapField">> => #{<<"k1">> => <<"hello">>}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode(SchemaId, Encoded),
    ?debugFmt("legacy decode result (mapField unsupported): ~p ~n", [Re1]),
    ?assertEqual(#{}, Re1).

to_map([{_,_}|_] = L) ->
    maps:from_list([{K, to_map(V)} || {K, V} <- L]);
to_map(V) -> V.
