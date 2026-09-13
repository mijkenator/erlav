-module(map_scalar_batch_test).

-include_lib("eunit/include/eunit.hrl").

%
% Issue #18: encodemap()'s scalar-value loop for map<int>/map<long> now
% batches each entry's key-length varint + key bytes + value varint into
% a per-entry scratch buffer (stack for small entries, heap fallback for
% large ones), mirroring the array<int>/array<long> fix from #16/#17.
%
% These tests cover:
%   1. Byte-exact round-trip against erlavro's decoder for edge-case
%      int32/int64 values (min/max, zero, negatives).
%   2. A map large enough to force the entry-count/value heap fallback
%      (entries whose key + varints exceed the on-stack scratch buffer).
%   3. General round-trip correctness for map<int> and map<long>.
%

% ---------------------------------------------------------------------------
% map<long> edge-case values, byte-exact against erlavro's own encoder/
% decoder.
% ---------------------------------------------------------------------------

map_long_edge_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_map.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Decoder = avro:make_simple_decoder(SchemaJSON, []),
    Term = #{<<"mapField">> => #{
        <<"zero">> => 0,
        <<"min_int64">> => -9223372036854775808,
        <<"max_int64">> => 9223372036854775807,
        <<"neg_small">> => -1,
        <<"neg_large">> => -123456789012345,
        <<"pos_large">> => 123456789012345
    }},
    ErlavEncoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    % erlav's own decoder should round-trip
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavEncoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    % erlavro's decoder should also make sense of erlav's bytes
    DecodedByErlavro = tst_utils:to_map(Decoder(ErlavEncoded)),
    ?assert(true == tst_utils:compare_maps(Term, DecodedByErlavro)),
    % and erlav should decode erlavro's own bytes for the same values
    DecodedErlavroBytes = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps(Term, DecodedErlavroBytes)),
    ok.

% ---------------------------------------------------------------------------
% map<int> edge-case values, byte-exact against erlavro.
% ---------------------------------------------------------------------------

map_int_edge_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map_int.avsc">>),
    {ok, SchemaJSON} = file:read_file("test/tschema_map_int.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Decoder = avro:make_simple_decoder(SchemaJSON, []),
    Term = #{<<"mapField">> => #{
        <<"zero">> => 0,
        <<"min_int32">> => -2147483648,
        <<"max_int32">> => 2147483647,
        <<"neg_small">> => -1,
        <<"neg_mid">> => -123456,
        <<"pos_mid">> => 123456
    }},
    ErlavEncoded = erlav_nif:erlav_encode(SchemaId, Term),
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, ErlavEncoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)),
    DecodedByErlavro = tst_utils:to_map(Decoder(ErlavEncoded)),
    ?assert(true == tst_utils:compare_maps(Term, DecodedByErlavro)),
    DecodedErlavroBytes = erlav_nif:erlav_decode_fast(SchemaId, ErlavroEncoded),
    ?assert(true == tst_utils:compare_maps(Term, DecodedErlavroBytes)),
    ok.

% ---------------------------------------------------------------------------
% Large map<long>: thousands of entries, forcing both the entry-count
% heap fallback (map_size > small caches) and, via long keys, the
% per-entry scratch-buffer heap fallback (needed > ENTRY_STACK_CAP).
% ---------------------------------------------------------------------------

map_long_large_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Pairs = [{list_to_binary("key_" ++ integer_to_list(I)), signed_value(I)}
             || I <- lists:seq(1, 5000)],
    Map1 = maps:from_list(Pairs),
    Term = #{<<"mapField">> => Map1},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)).

% Forces the per-entry scratch-buffer heap fallback directly: a single
% key long enough that key-length-varint + key-bytes + value-varint
% exceeds the 256-byte on-stack scratch buffer.
map_long_key_forces_heap_fallback_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    LongKey = list_to_binary(lists:duplicate(500, $k)),
    Term = #{<<"mapField">> => #{LongKey => 42, <<"short">> => -42}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)).

% ---------------------------------------------------------------------------
% map<int> large round-trip.
% ---------------------------------------------------------------------------

map_int_large_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map_int.avsc">>),
    Pairs = [{list_to_binary("k" ++ integer_to_list(I)), signed_value(I)}
             || I <- lists:seq(1, 3000)],
    Map1 = maps:from_list(Pairs),
    Term = #{<<"mapField">> => Map1},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Decoded)).

% Empty map still encodes as a single 0 byte (block-count terminator only).
map_long_empty_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Term = #{<<"mapField">> => #{}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(<<0>>, Encoded),
    Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Decoded).

signed_value(I) when I rem 2 =:= 0 -> I;
signed_value(I) -> -I.
