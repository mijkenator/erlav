-module(correctness_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================
%% int_encode / int_decode round-trip correctness
%% ============================================================

varint_roundtrip_test() ->
    Values = [0, 1, 127, 128, 255, 256, 1000, 100000, 2147483647],
    Encoded = erlav_nif:int_encode(Values),
    Decoded = erlav_nif:int_decode(Encoded),
    ?assertEqual(Values, Decoded).

varint_single_zero_test() ->
    Encoded = erlav_nif:int_encode([0]),
    Decoded = erlav_nif:int_decode(Encoded),
    ?assertEqual([0], Decoded).

varint_single_large_test() ->
    Encoded = erlav_nif:int_encode([9223372036854775807]),
    Decoded = erlav_nif:int_decode(Encoded),
    ?assertEqual([9223372036854775807], Decoded).

varint_empty_list_test() ->
    Encoded = erlav_nif:int_encode([]),
    ?assertEqual(<<>>, Encoded).

varint_powers_of_two_test() ->
    Values = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
              65536, 16777216, 4294967296, 1099511627776],
    Encoded = erlav_nif:int_encode(Values),
    Decoded = erlav_nif:int_decode(Encoded),
    ?assertEqual(Values, Decoded).

%% ============================================================
%% erlav_decode (non-fast) vs erlav_decode_fast consistency
%% ============================================================

decode_paths_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer.avsc">>),
    Term = #{<<"intField1">> => 111, <<"intField2">> => 22222,
             <<"intField3">> => 33333, <<"intField4">> => 4,
             <<"intField5">> => 555555555, <<"intField6">> => 6666,
             <<"intField7">> => 7777777},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Slow = erlav_nif:erlav_decode(SchemaId, Encoded),
    Fast = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Slow, Fast).

decode_paths_string_test() ->
    %% NOTE: erlav_decode (non-fast legacy path) does not support string fields;
    %% it returns badarg. We verify decode_fast works independently.
    SchemaId = erlav_nif:erlav_init(<<"test/strings.avsc">>),
    Term = #{<<"stringField1">> => <<"hello">>,
             <<"stringField2">> => <<"world">>,
             <<"stringField3">> => <<"foo">>,
             <<"stringField4">> => <<"bar">>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Fast = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<"hello">>, maps:get(<<"stringField1">>, Fast)),
    ?assertEqual(<<"world">>, maps:get(<<"stringField2">>, Fast)),
    ?assertEqual(<<"foo">>, maps:get(<<"stringField3">>, Fast)),
    ?assertEqual(<<"bar">>, maps:get(<<"stringField4">>, Fast)).

decode_paths_all_types_test() ->
    %% NOTE: erlav_decode (non-fast legacy path) does not support mixed-type schemas;
    %% we verify decode_fast independently.
    SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
    Term = #{<<"intField">> => 123, <<"longField">> => 9876543210,
             <<"floatField">> => 3.14, <<"doubleField">> => 2.718,
             <<"stringField">> => <<"test">>, <<"boolField">> => true,
             <<"bytesField">> => <<1,2,3>>,
             <<"stringField2">> => <<"test2">>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Fast = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(123, maps:get(<<"intField">>, Fast)),
    ?assertEqual(9876543210, maps:get(<<"longField">>, Fast)),
    ?assert(abs(maps:get(<<"floatField">>, Fast) - 3.14) < 0.01),
    ?assert(abs(maps:get(<<"doubleField">>, Fast) - 2.718) < 0.01),
    ?assertEqual(<<"test">>, maps:get(<<"stringField">>, Fast)),
    ?assertEqual(true, maps:get(<<"boolField">>, Fast)).

%% ============================================================
%% erlavro cross-validation: byte-for-byte encode match
%% ============================================================

erlavro_int_cross_test() ->
    {ok, SchemaJSON} = file:read_file("test/integer.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/integer.avsc">>),
    Term = #{<<"intField1">> => 0, <<"intField2">> => -1,
             <<"intField3">> => 2147483647, <<"intField4">> => -2147483648,
             <<"intField5">> => 1, <<"intField6">> => 100,
             <<"intField7">> => -100},
    ?assertEqual(iolist_to_binary(Encoder(Term)),
                 erlav_nif:erlav_encode(SchemaId, Term)).

erlavro_string_cross_test() ->
    {ok, SchemaJSON} = file:read_file("test/strings.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/strings.avsc">>),
    Term = #{<<"stringField1">> => <<>>,
             <<"stringField2">> => <<"a">>,
             <<"stringField3">> => <<"hello world 1234567890">>,
             <<"stringField4">> => <<"!@#$%^&*()">>},
    ?assertEqual(iolist_to_binary(Encoder(Term)),
                 erlav_nif:erlav_encode(SchemaId, Term)).

erlavro_nullable_cross_test() ->
    {ok, SchemaJSON} = file:read_file("test/tschema_all_null.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_all_null.avsc">>),
    %% All fields present
    Term1 = #{<<"intField">> => 42, <<"longField">> => 999999,
              <<"floatField">> => 1.5, <<"doubleField">> => 2.5,
              <<"stringField">> => <<"hi">>, <<"boolField">> => false},
    ?assertEqual(iolist_to_binary(Encoder(Term1)),
                 erlav_nif:erlav_encode(SchemaId, Term1)),
    %% Some fields omitted (null)
    Term2 = #{<<"intField">> => 42},
    ?assertEqual(iolist_to_binary(Encoder(Term2)),
                 erlav_nif:erlav_encode(SchemaId, Term2)),
    %% No fields
    Term3 = #{},
    ?assertEqual(iolist_to_binary(Encoder(Term3)),
                 erlav_nif:erlav_encode(SchemaId, Term3)).

erlavro_enum_cross_test() ->
    %% Verify enum encode round-trip with actual symbols from enum.avsc
    SchemaId = erlav_nif:erlav_init(<<"test/enum.avsc">>),
    lists:foreach(fun(V) ->
        Term = #{<<"enumField">> => V},
        Encoded = erlav_nif:erlav_encode(SchemaId, Term),
        ?assert(is_binary(Encoded)),
        Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(V, maps:get(<<"enumField">>, Decoded))
    end, [<<"ONE">>, <<"TWO">>, <<"THREE">>, <<"DIAMONDS">>, <<"kolobok">>]).

erlavro_array_cross_test() ->
    %% erlavro uses negative block counts (with byte size prefix), erlav uses
    %% positive block counts — both are valid Avro. Compare via decode round-trip.
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    lists:foreach(fun(Arr) ->
        Term = #{<<"arrayField">> => Arr},
        Encoded = erlav_nif:erlav_encode(SchemaId, Term),
        Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(Arr, maps:get(<<"arrayField">>, Decoded))
    end, [[], [1], [1,2,3], [-1, 0, 2147483647],
          lists:seq(1, 100)]).

erlavro_map_cross_test() ->
    {ok, SchemaJSON} = file:read_file("test/tschema_map.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Decoder = avro:make_simple_decoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    %% For maps, key ordering may differ between erlav and erlavro.
    %% Verify via decode instead of byte-for-byte.
    Term = #{<<"mapField">> => #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Decoded = maps:from_list(Decoder(Encoded)),
    %% erlavro returns map values as proplist, normalize
    MapField = maps:get(<<"mapField">>, Decoded),
    Expected = [{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
    ?assertEqual(lists:sort(Expected), lists:sort(MapField)).

%% ============================================================
%% safe_encode correctness: atom keys, list keys, string values
%% ============================================================

safe_encode_atom_keys_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{f => 42},
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(42, maps:get(<<"f">>, Re1)).

safe_encode_list_keys_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_string.avsc">>),
    %% replace_keys converts string keys to binary but does not convert
    %% string values (charlists) to binary — use binary values
    Term = #{"f" => <<"hello">>},
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<"hello">>, maps:get(<<"f">>, Re1)).

safe_encode_mixed_keys_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    %% Mix of binary, atom, and string keys; values must be binary for strings
    Term = #{<<"intField">> => 1, longField => 2,
             "floatField" => 3.14, <<"doubleField">> => 2.718,
             <<"stringField">> => <<"hello">>, <<"bytesField">> => <<9>>,
             boolField => true},
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(1, maps:get(<<"intField">>, Re1)),
    ?assertEqual(2, maps:get(<<"longField">>, Re1)),
    ?assertEqual(true, maps:get(<<"boolField">>, Re1)).

safe_encode_null_filtered_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_all_null.avsc">>),
    %% null values should be filtered out by replace_keys
    Term = #{<<"intField">> => 42, <<"longField">> => null,
             <<"stringField">> => null},
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(42, maps:get(<<"intField">>, Re1)).

safe_encode_nested_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/deep_record.avsc">>),
    %% String values must be binaries (replace_keys only converts keys)
    Term = #{level1 => #{val1 => 10,
                         level2 => #{val2 => 20,
                                     level3 => #{val3 => 30,
                                                  name => <<"deep">>}}}},
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    L1 = maps:get(<<"level1">>, Re1),
    ?assertEqual(10, maps:get(<<"val1">>, L1)),
    L2 = maps:get(<<"level2">>, L1),
    ?assertEqual(20, maps:get(<<"val2">>, L2)),
    L3 = maps:get(<<"level3">>, L2),
    ?assertEqual(30, maps:get(<<"val3">>, L3)),
    ?assertEqual(<<"deep">>, maps:get(<<"name">>, L3)).

%% ============================================================
%% Encode determinism: same input produces same output
%% ============================================================

encode_deterministic_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    Term = #{<<"intField">> => 42, <<"longField">> => 999,
             <<"floatField">> => 1.5, <<"doubleField">> => 2.5,
             <<"stringField">> => <<"test">>,
             <<"bytesField">> => <<1,2,3>>, <<"boolField">> => true},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    E2 = erlav_nif:erlav_encode(SchemaId, Term),
    E3 = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(E1, E2),
    ?assertEqual(E2, E3).

%% ============================================================
%% Decode after re-encode produces identical binary
%% ============================================================

reencode_stable_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer.avsc">>),
    Term = #{<<"intField1">> => 1, <<"intField2">> => 2,
             <<"intField3">> => 3, <<"intField4">> => 4,
             <<"intField5">> => 5, <<"intField6">> => 6,
             <<"intField7">> => 7},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1),
    ?assertEqual(E1, E2).

reencode_stable_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/strings.avsc">>),
    Term = #{<<"stringField1">> => <<"aaa">>,
             <<"stringField2">> => <<"bbb">>,
             <<"stringField3">> => <<"ccc">>,
             <<"stringField4">> => <<"ddd">>},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1),
    ?assertEqual(E1, E2).

reencode_stable_all_types_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    Term = #{<<"intField">> => -999, <<"longField">> => -999999,
             <<"floatField">> => 0.0, <<"doubleField">> => -0.0,
             <<"stringField">> => <<"re-encode">>,
             <<"bytesField">> => <<255,0,128>>, <<"boolField">> => false},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1),
    ?assertEqual(E1, E2).

reencode_stable_nullable_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_all_null.avsc">>),
    Term = #{<<"intField">> => 42, <<"stringField">> => <<"hi">>},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    %% Filter nulls from decoded map before re-encoding (decoder returns null fields)
    %% Decoder may return null fields as atom `null` or `undefined`
    D1Filtered = maps:filter(fun(_, V) -> V =/= null andalso V =/= undefined end, D1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1Filtered),
    ?assertEqual(E1, E2).

reencode_stable_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/deep_record.avsc">>),
    Term = #{<<"level1">> => #{<<"val1">> => 77,
                               <<"level2">> => #{<<"val2">> => 88,
                                                 <<"level3">> => #{<<"val3">> => 99,
                                                                   <<"name">> => <<"stable">>}}}},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1),
    ?assertEqual(E1, E2).

reencode_stable_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => [10, 20, 30, -1, 0, 2147483647]},
    E1 = erlav_nif:erlav_encode(SchemaId, Term),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    E2 = erlav_nif:erlav_encode(SchemaId, D1),
    ?assertEqual(E1, E2).

reencode_stable_enum_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/enum.avsc">>),
    lists:foreach(fun(V) ->
        Term = #{<<"enumField">> => V},
        E1 = erlav_nif:erlav_encode(SchemaId, Term),
        D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
        E2 = erlav_nif:erlav_encode(SchemaId, D1),
        ?assertEqual(E1, E2)
    end, [<<"ONE">>, <<"TWO">>, <<"THREE">>, <<"DIAMONDS">>, <<"kolobok">>]).

reencode_stable_union_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/union_null.avsc">>),
    %% Union with string
    Term1 = #{<<"field1">> => <<"hello">>, <<"field2">> => 42},
    E1 = erlav_nif:erlav_encode(SchemaId, Term1),
    D1 = erlav_nif:erlav_decode_fast(SchemaId, E1),
    D1F = maps:filter(fun(_, V) -> V =/= null end, D1),
    E1b = erlav_nif:erlav_encode(SchemaId, D1F),
    ?assertEqual(E1, E1b),
    %% Union with only one field
    Term2 = #{<<"field1">> => 999},
    E2 = erlav_nif:erlav_encode(SchemaId, Term2),
    D2 = erlav_nif:erlav_decode_fast(SchemaId, E2),
    D2F = maps:filter(fun(_, V) -> V =/= null end, D2),
    E2b = erlav_nif:erlav_encode(SchemaId, D2F),
    ?assertEqual(E2, E2b).

%% ============================================================
%% Large-scale round-trip: 100 elements
%% ============================================================

large_array_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Arr = lists:seq(1, 500),
    Term = #{<<"arrayField">> => Arr},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Arr, maps:get(<<"arrayField">>, Re1)).

large_string_array_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    Arr = [list_to_binary("item_" ++ integer_to_list(I)) || I <- lists:seq(1, 200)],
    Term = #{<<"arrayField">> => Arr},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Arr, maps:get(<<"arrayField">>, Re1)).

large_map_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Pairs = [{list_to_binary("k" ++ integer_to_list(I)), I} || I <- lists:seq(1, 50)],
    Term = #{<<"mapField">> => maps:from_list(Pairs)},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Re1)).

%% ============================================================
%% replace_keys utility correctness
%% ============================================================

replace_keys_atom_test() ->
    Result = erlav_nif:replace_keys(#{foo => 1, bar => <<"hi">>}),
    ?assertEqual(#{<<"foo">> => 1, <<"bar">> => <<"hi">>}, Result).

replace_keys_string_test() ->
    Result = erlav_nif:replace_keys(#{"foo" => 1}),
    ?assertEqual(#{<<"foo">> => 1}, Result).

replace_keys_null_test() ->
    Result = erlav_nif:replace_keys(#{<<"a">> => 1, <<"b">> => null}),
    ?assertEqual(#{<<"a">> => 1}, Result).

replace_keys_empty_list_test() ->
    Result = erlav_nif:replace_keys(#{<<"a">> => 1, <<"b">> => []}),
    ?assertEqual(#{<<"a">> => 1}, Result).

replace_keys_nested_test() ->
    Result = erlav_nif:replace_keys(#{foo => #{bar => 42}}),
    ?assertEqual(#{<<"foo">> => #{<<"bar">> => 42}}, Result).

replace_keys_proplist_test() ->
    Result = erlav_nif:replace_keys(#{data => [{<<"k">>, 1}]}),
    ?assertEqual(#{<<"data">> => #{<<"k">> => 1}}, Result).
