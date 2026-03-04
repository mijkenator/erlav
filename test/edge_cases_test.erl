-module(edge_cases_test).

-include_lib("eunit/include/eunit.hrl").

%% --- Zero values ---

zero_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{<<"f">> => 0},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(0, maps:get(<<"f">>, Re1)).

zero_long_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
    Term = #{<<"f">> => 0},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(0, maps:get(<<"f">>, Re1)).

zero_double_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    Term = #{<<"f">> => 0.0},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assert(abs(maps:get(<<"f">>, Re1) - 0.0) < 0.00001).

%% --- Negative integers ---

neg_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    lists:foreach(fun(V) ->
        Term = #{<<"f">> => V},
        Encoded = erlav_nif:erlav_encode(SchemaId, Term),
        Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(V, maps:get(<<"f">>, Re1))
    end, [-1, -100, -32768, -2147483648]).

neg_long_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
    lists:foreach(fun(V) ->
        Term = #{<<"f">> => V},
        Encoded = erlav_nif:erlav_encode(SchemaId, Term),
        Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(V, maps:get(<<"f">>, Re1))
    end, [-1, -100, -2147483648, -9223372036854775808]).

%% --- Boundary values round-trip ---

int_max_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{<<"f">> => 2147483647},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(2147483647, maps:get(<<"f">>, Re1)).

int_min_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{<<"f">> => -2147483648},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(-2147483648, maps:get(<<"f">>, Re1)).

long_max_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
    Term = #{<<"f">> => 9223372036854775807},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(9223372036854775807, maps:get(<<"f">>, Re1)).

long_min_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
    Term = #{<<"f">> => -9223372036854775808},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(-9223372036854775808, maps:get(<<"f">>, Re1)).

%% --- Boolean false ---

bool_false_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_bool.avsc">>),
    Term = #{<<"boolField">> => false},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(false, maps:get(<<"boolField">>, Re1)).

bool_true_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_bool.avsc">>),
    Term = #{<<"boolField">> => true},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(true, maps:get(<<"boolField">>, Re1)).

%% --- Empty string ---

empty_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_string.avsc">>),
    Term = #{<<"f">> => <<>>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<>>, maps:get(<<"f">>, Re1)).

%% --- Empty bytes ---

empty_bytes_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/bytes.avsc">>),
    Term = #{<<"key">> => <<>>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<>>, maps:get(<<"key">>, Re1)).

%% --- Large string ---

large_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_string.avsc">>),
    LargeStr = list_to_binary(lists:duplicate(10000, $A)),
    Term = #{<<"f">> => LargeStr},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(LargeStr, maps:get(<<"f">>, Re1)).

%% --- Float/double edge values ---

small_double_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    Term = #{<<"f">> => 1.0e-38},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    V = maps:get(<<"f">>, Re1),
    ?assert(abs(V - 1.0e-38) < 1.0e-45).

large_double_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    Term = #{<<"f">> => 1.7e+308},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    V = maps:get(<<"f">>, Re1),
    ?assert(abs(V - 1.7e+308) < 1.0e+295).

negative_double_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    Term = #{<<"f">> => -99.99},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    V = maps:get(<<"f">>, Re1),
    ?assert(abs(V - (-99.99)) < 0.00001).
