-module(negative_block_count_test).

-include_lib("eunit/include/eunit.hrl").

%
% Issue #15: erlavro emits Avro's "negative block count" form for
% essentially every array/map it encodes -- a negative item count followed
% by a signed long giving the block's byte length, then the items -- while
% erlav_decode/erlav_decode_fast previously only understood the plain
% positive-count form. These tests cover:
%   1. Decoding erlavro-produced (negative-block) bytes -- unconditional,
%      no opt-in needed, since a decoder must handle any valid encoding.
%   2. erlav_init/2 with the `use_negative_block_count` option, which makes
%      erlav's own encoder emit the same negative-block form.
%   3. That erlav_init/1 (no options) keeps emitting today's plain
%      positive-count bytes, unchanged.
%

% ---------------------------------------------------------------------------
% Decode fix: bytes produced by erlavro's own encoder (negative block form)
% must round-trip through erlav_decode_fast. This is the exact repro from
% issue #15.
% ---------------------------------------------------------------------------

decode_erlavro_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/array_simple.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [1,2,3]},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    % erlavro's encoder writes the negative-block form -- confirm the first
    % byte really is the negative-count marker before trusting the rest of
    % the assertion (zigzag(-3) = 5).
    ?assertMatch(<<5, _/binary>>, ErlavroEncoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?debugFmt("decode result: ~p ~n", [Decoded]),
    ?assertEqual(Term, Decoded),
    ok.

decode_erlavro_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_map.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"mapField">> => #{<<"f1">> => 2, <<"f2">> => 4}},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?debugFmt("decode result: ~p ~n", [Decoded]),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Array-of-union items, decoded from erlavro's negative-block bytes --
% companion to the named-enum-ref repro from issue #15's description.
decode_erlavro_array_of_union_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union1.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_of_union1.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"arrayField">> => [
            1,
            2,
            #{<<"rec1field">> => 10, <<"rec3field">> => 20},
            3
        ]
    },
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?debugFmt("decode result: ~p ~n", [Decoded]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

% Empty array/map from erlavro: still just a single 0 byte, no block header
% at all -- must decode to an empty list/map, same as today.
decode_erlavro_empty_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/array_simple.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => []},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    ?assertEqual(<<0>>, ErlavroEncoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(Term, Decoded),
    ok.

% ---------------------------------------------------------------------------
% Encode opt-in: erlav_init/2 with use_negative_block_count makes erlav's
% own encoder emit the negative-block form, and the result must still
% round-trip through erlav_decode_fast.
% ---------------------------------------------------------------------------

encode_negative_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>, [use_negative_block_count]),
    Term = #{<<"arrayField">> => [1,2,3]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    % zigzag(-3) = 5, matching erlavro's own output for the same term (see
    % decode_erlavro_array_test).
    ?assertMatch(<<5, _/binary>>, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded),
    ok.

% Same term/schema, compared byte-for-byte against erlavro's own encoder --
% erlav always emits its whole array/map as a single block, matching
% erlavro's shape exactly for this case.
encode_negative_array_matches_erlavro_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>, [use_negative_block_count]),
    {ok, SchemaJSON} = file:read_file("test/array_simple.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [1,2,3]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlav: ~p erlavro: ~p ~n", [Encoded, ErlavroEncoded]),
    ?assertEqual(ErlavroEncoded, Encoded),
    ok.

encode_negative_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>, [use_negative_block_count]),
    Term = #{<<"mapField">> => #{<<"f1">> => 2, <<"f2">> => 4}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Empty array/map under use_negative_block_count must still be a single 0
% byte -- no spurious negative-block header for zero items.
encode_negative_empty_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>, [use_negative_block_count]),
    Term = #{<<"arrayField">> => []},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(<<0>>, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded),
    ok.

encode_negative_empty_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>, [use_negative_block_count]),
    Term = #{<<"mapField">> => #{}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(<<0>>, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Nested propagation: map-of-arrays -- the negative-block flag must reach
% both the outer map and the inner array nodes.
encode_negative_nested_map_of_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_arr.avsc">>, [use_negative_block_count]),
    Term = #{
        <<"key">> =>
            #{
                <<"f1">> => [1],
                <<"f2">> => [1,2,3,4,5]
             }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Nested propagation: array-of-records -- the negative-block flag must
% reach the outer array; record encoding itself has no block form.
encode_negative_nested_array_of_records_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs.avsc">>, [use_negative_block_count]),
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
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

% Array-items union under use_negative_block_count -- exercises the
% rollback-on-mismatch (target->resize(saved_size)) paths writing into the
% scratch buffer instead of `ret` directly.
encode_negative_array_of_union_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>, [use_negative_block_count]),
    Term = #{<<"arrayField">> => [<<"aaaaaa3">>, 1111, 77, <<"cccccccccccccc1">>, 0, 1, <<"sasd">>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% ---------------------------------------------------------------------------
% Regression: erlav_init/1 (no options) is unaffected -- still emits the
% plain positive-count form.
% ---------------------------------------------------------------------------

encode_default_still_positive_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => [1,2,3]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    % zigzag(3) = 6, today's plain positive-count form -- unchanged from
    % before this change.
    ?assertEqual(<<6,2,4,6,0>>, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded),
    ok.

% Same filename, both options -- must resolve to two distinct schema ids
% (each registered against its own encoding mode) rather than the second
% call reusing the first's id.
init_distinct_ids_per_mode_test() ->
    Id1 = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Id2 = erlav_nif:erlav_init(<<"test/array_simple.avsc">>, [use_negative_block_count]),
    ?assertNotEqual(Id1, Id2),
    % Repeat calls with the same mode must return the same id each time.
    ?assertEqual(Id1, erlav_nif:erlav_init(<<"test/array_simple.avsc">>)),
    ?assertEqual(Id2, erlav_nif:erlav_init(<<"test/array_simple.avsc">>, [use_negative_block_count])),
    ok.

% ---------------------------------------------------------------------------
% Remaining shape coverage: the tests above exercise a representative
% sample (scalar array/map, one level of nesting, one union shape). These
% round out the matrix with the shapes not yet touched -- string-valued
% array/map, a map with union/nullable scalar values (the shape that had
% its own separate scalar_type gap, see decode_map_test:m7_test), map of
% records, two-level nested array-of-array (negative block on both levels
% at once), and the remaining array-items-union member kinds (enum,
% long+array+record together, null+long+record). Each is checked both
% ways: decoding erlavro's own negative-block bytes, and erlav's opt-in
% encoder round-tripping (with a byte-for-byte match against erlavro where
% the shapes are simple enough for that to be meaningful).
% ---------------------------------------------------------------------------

decode_erlavro_string_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_str.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [<<"a">>, <<"bb">>]},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(Term, Decoded),
    ok.

encode_negative_string_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>, [use_negative_block_count]),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_str.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [<<"a">>, <<"bb">>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?assertEqual(ErlavroEncoded, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded),
    ok.

decode_erlavro_string_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map_str.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_map_str.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"mapField">> => #{<<"k">> => <<"v">>}},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

encode_negative_string_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map_str.avsc">>, [use_negative_block_count]),
    {ok, SchemaJSON} = file:read_file("test/tschema_map_str.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"mapField">> => #{<<"k">> => <<"v">>}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?assertEqual(ErlavroEncoded, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Map whose *values* are a union of scalars ({"type": "map", "values":
% ["null", "string"]}). NOTE: unlike every other shape in this file, there
% is deliberately no decode_erlavro_* counterpart here -- decode_map's
% "map of scalars" fast path (si->obj_field != "complex") never accounts
% for the per-value union type-index byte that a spec-compliant writer
% (erlavro included) emits for this shape, so feeding it erlavro's bytes
% desyncs the read cursor and can crash the whole VM (bad string length ->
% huge binary_alloc), not just fail cleanly. That is a distinct,
% pre-existing bug in encodemap/decode_map's union-values handling
% (confirmed present on master, independent of the negative-block-count
% change here) -- out of scope for this PR; tracked separately. erlav's
% own round-trip below is unaffected since its encoder and decoder agree
% (both omit the discriminator), so it's safe to exercise the
% negative-block encoding path in isolation.
encode_negative_map_union_scalar_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_union_scalar_values.avsc">>, [use_negative_block_count]),
    Term = #{<<"mapField">> => #{<<"k1">> => <<"hello">>}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Map of records ({"type": "map", "values": {"type": "record", ...}}) --
% companion to decode_map_test:m5_test.
decode_erlavro_map_of_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_rec.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/map_rec.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"key">> => #{<<"a">> => #{<<"f1">> => <<"hello">>, <<"f2">> => 5}}},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

encode_negative_map_of_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/map_rec.avsc">>, [use_negative_block_count]),
    Term = #{<<"key">> => #{<<"a">> => #{<<"f1">> => <<"hello">>, <<"f2">> => 5}}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

% Two-level nested array-of-array ({"type": "array", "items": {"type":
% "array", "items": "long"}}) -- both the outer and inner array must
% independently read/write the negative-block form.
decode_erlavro_nested_array_of_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/array_array.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [[1,1,1],[2,3,4]]},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

encode_negative_nested_array_of_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array.avsc">>, [use_negative_block_count]),
    {ok, SchemaJSON} = file:read_file("test/array_array.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [[1,1,1],[2,3,4]]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlav: ~p erlavro: ~p ~n", [Encoded, ErlavroEncoded]),
    ?assertEqual(ErlavroEncoded, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    ok.

% Array-items union with an enum member (long | enum) -- companion to
% array_of_union_enum_items_test in decode_array_test.erl.
decode_erlavro_array_of_union_enum_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union3.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_of_union3.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{<<"arrayField">> => [1, <<"TWO">>]},
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

encode_negative_array_of_union_enum_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union3.avsc">>, [use_negative_block_count]),
    Term = #{<<"arrayField">> => [1, <<"TWO">>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

% Array-items union with more than one non-scalar member (long | array |
% record) -- companion to array_of_union_multi_nonscalar_items_test.
% Exercises array_multi_type_child_index resolving distinct childItems
% slots while writing into the negative-block scratch buffer.
decode_erlavro_array_of_union_multi_nonscalar_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union4.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_of_union4.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"arrayField">> => [
            1,
            [10, 20],
            #{<<"rec1field">> => 5, <<"rec3field">> => 6}
        ]
    },
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

encode_negative_array_of_union_multi_nonscalar_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union4.avsc">>, [use_negative_block_count]),
    Term = #{
        <<"arrayField">> => [
            1,
            [10, 20],
            #{<<"rec1field">> => 5, <<"rec3field">> => 6}
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps_deep(Term, Decoded)),
    ok.

% Array-items union with "null" alongside a scalar and a record member
% (null | long | record) -- companion to
% array_of_union_null_record_items_test. erlavro accepts the atom `null`
% (not `undefined`) for this union member; erlav's decoder must still
% surface it as `undefined`, matching the Erlang-side convention used
% throughout this library.
decode_erlavro_array_of_union_null_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union6.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_array_of_union6.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    ErlavroEncoded = iolist_to_binary(Encoder(#{
        <<"arrayField">> => [1, null, #{<<"a">> => 5}]
    })),
    ?debugFmt("erlavro bytes: ~p ~n", [ErlavroEncoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assertEqual(#{<<"arrayField">> => [1, undefined, #{<<"a">> => 5}]}, Decoded),
    ok.

encode_negative_array_of_union_null_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union6.avsc">>, [use_negative_block_count]),
    Term = #{<<"arrayField">> => [1, undefined, #{<<"a">> => 5}]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("erlav (negative) bytes: ~p ~n", [Encoded]),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded),
    ok.

% ---------------------------------------------------------------------------
% Multi-block decode: the Avro spec allows an array/map to be split across
% several blocks (each with its own leading count, positive or negative)
% before the terminating 0. Neither erlavro's encoder nor erlav's own
% (either mode) ever emits more than one block, so this shape can't be
% reached via either encoder above -- hand-crafted here to prove
% decode_array's/decode_map's block loop actually loops more than once,
% not just that it tolerates a single negative-form block.
% ---------------------------------------------------------------------------

decode_multi_block_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    % block 1: positive count=2, items 1,2 (zigzag: 2,4)
    % block 2: negative count=-1 (zigzag(-1)=1), block byte length=1 (zigzag(1)=2), item 3 (zigzag: 6)
    % terminator: 0
    MultiBlock = <<4, 2,4, 1, 2, 6, 0>>,
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, MultiBlock),
    ?debugFmt("multi-block decode: ~p ~n", [Decoded]),
    ?assertEqual(#{<<"arrayField">> => [1,2,3]}, Decoded),
    ok.

decode_multi_block_map_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    % block 1: positive count=1, key "f1" (len 2, zigzag(2)=4, bytes "f1"), value 2 (zigzag: 4)
    % block 2: negative count=-1 (zigzag(-1)=1), block byte length (zigzag of actual byte count),
    %          key "f2" (len 2, zigzag(2)=4, bytes "f2"), value 4 (zigzag: 8)
    % terminator: 0
    Block2Body = <<4, $f, $2, 8>>,
    Block2Len = byte_size(Block2Body),
    MultiBlock = <<2, 4, $f, $1, 4,
                   1, (Block2Len bsl 1),
                   Block2Body/binary,
                   0>>,
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, MultiBlock),
    ?debugFmt("multi-block decode: ~p ~n", [Decoded]),
    ?assert(true == tst_utils:compare_maps(#{<<"mapField">> => #{<<"f1">> => 2, <<"f2">> => 4}}, Decoded)),
    ok.
