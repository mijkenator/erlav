-module(roundtrip_test).

-include_lib("eunit/include/eunit.hrl").

%% --- All primitive types in one record ---

all_types_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    Term = #{
        <<"intField">> => 42,
        <<"longField">> => 9876543210,
        <<"floatField">> => 3.14,
        <<"doubleField">> => 2.718281828,
        <<"stringField">> => <<"hello world">>,
        <<"bytesField">> => <<1,2,3,4,5>>,
        <<"boolField">> => true
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(42, maps:get(<<"intField">>, Re1)),
    ?assertEqual(9876543210, maps:get(<<"longField">>, Re1)),
    ?assert(abs(maps:get(<<"floatField">>, Re1) - 3.14) < 0.01),
    ?assert(abs(maps:get(<<"doubleField">>, Re1) - 2.718281828) < 0.00001),
    ?assertEqual(<<"hello world">>, maps:get(<<"stringField">>, Re1)),
    ?assertEqual(<<1,2,3,4,5>>, maps:get(<<"bytesField">>, Re1)),
    ?assertEqual(true, maps:get(<<"boolField">>, Re1)).

all_types_false_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    Term = #{
        <<"intField">> => -1,
        <<"longField">> => -1,
        <<"floatField">> => -1.5,
        <<"doubleField">> => -1.5,
        <<"stringField">> => <<"test">>,
        <<"bytesField">> => <<0>>,
        <<"boolField">> => false
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(-1, maps:get(<<"intField">>, Re1)),
    ?assertEqual(-1, maps:get(<<"longField">>, Re1)),
    ?assert(abs(maps:get(<<"floatField">>, Re1) - (-1.5)) < 0.01),
    ?assert(abs(maps:get(<<"doubleField">>, Re1) - (-1.5)) < 0.00001),
    ?assertEqual(<<"test">>, maps:get(<<"stringField">>, Re1)),
    ?assertEqual(<<0>>, maps:get(<<"bytesField">>, Re1)),
    ?assertEqual(false, maps:get(<<"boolField">>, Re1)).

%% --- Cross-validate with erlavro ---

all_types_erlavro_test() ->
    {ok, SchemaJSON} = file:read_file("test/all_types.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/all_types.avsc">>),
    Term = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"floatField">> => 23.12,
        <<"doubleField">> => 11.2345,
        <<"stringField">> => <<"cross_validate">>,
        <<"bytesField">> => <<1,99,57,127,0,56>>,
        <<"boolField">> => true
    },
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ErlavEncoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(ErlavroEncoded, ErlavEncoded).

%% --- Union tests ---

union_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/multi_union.avsc">>),
    Term = #{<<"f1">> => <<"hello">>, <<"f2">> => <<"world">>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<"hello">>, maps:get(<<"f1">>, Re1)),
    ?assertEqual(<<"world">>, maps:get(<<"f2">>, Re1)).

union_long_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/multi_union.avsc">>),
    Term = #{<<"f1">> => 12345, <<"f2">> => 67890},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(12345, maps:get(<<"f1">>, Re1)),
    ?assertEqual(67890, maps:get(<<"f2">>, Re1)).

union_null_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/multi_union.avsc">>),
    Term = #{},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    _Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ok.

union_mixed_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/multi_union.avsc">>),
    Term = #{<<"f1">> => <<"string_val">>, <<"f2">> => 999},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<"string_val">>, maps:get(<<"f1">>, Re1)),
    ?assertEqual(999, maps:get(<<"f2">>, Re1)).

%% --- Nullable record ---

nullable_record_present_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/nullable_record.avsc">>),
    Term = #{
        <<"id">> => 42,
        <<"sub">> => #{<<"val">> => 100, <<"name">> => <<"test">>}
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(42, maps:get(<<"id">>, Re1)),
    Sub = maps:get(<<"sub">>, Re1),
    ?assertEqual(100, maps:get(<<"val">>, Sub)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Sub)).

nullable_record_null_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/nullable_record.avsc">>),
    Term = #{<<"id">> => 7},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(7, maps:get(<<"id">>, Re1)).

%% --- Deep nested record ---

deep_record_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/deep_record.avsc">>),
    Term = #{
        <<"level1">> => #{
            <<"val1">> => 1,
            <<"level2">> => #{
                <<"val2">> => 2,
                <<"level3">> => #{
                    <<"val3">> => 3,
                    <<"name">> => <<"deep">>
                }
            }
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    L1 = maps:get(<<"level1">>, Re1),
    ?assertEqual(1, maps:get(<<"val1">>, L1)),
    L2 = maps:get(<<"level2">>, L1),
    ?assertEqual(2, maps:get(<<"val2">>, L2)),
    L3 = maps:get(<<"level3">>, L2),
    ?assertEqual(3, maps:get(<<"val3">>, L3)),
    ?assertEqual(<<"deep">>, maps:get(<<"name">>, L3)).

%% --- Map round-trips ---

map_single_entry_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Term = #{<<"mapField">> => #{<<"only">> => 42}},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Re1)).

map_many_entries_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Pairs = [{list_to_binary("key" ++ integer_to_list(I)), I} || I <- lists:seq(1, 15)],
    Term = #{<<"mapField">> => maps:from_list(Pairs)},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(true == tst_utils:compare_maps(Term, Re1)).

%% --- Array edge cases ---

single_element_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => [42]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual([42], maps:get(<<"arrayField">>, Re1)).

empty_array_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => []},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual([], maps:get(<<"arrayField">>, Re1)).

array_negative_values_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => [-1, -100, -2147483648, 0, 2147483647]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual([-1, -100, -2147483648, 0, 2147483647], maps:get(<<"arrayField">>, Re1)).

%% --- String array round-trip ---

string_array_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    Term = #{<<"arrayField">> => [<<"">>, <<"a">>, <<"hello world">>, <<"1234567890">>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual([<<"">>, <<"a">>, <<"hello world">>, <<"1234567890">>], maps:get(<<"arrayField">>, Re1)).
