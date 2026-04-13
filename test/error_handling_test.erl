-module(error_handling_test).

-include_lib("eunit/include/eunit.hrl").

%% --- Wrong type errors ---

string_for_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{<<"f">> => <<"not_an_int">>},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertMatch({error, _, _}, Ret).

int_for_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_string.avsc">>),
    Term = #{<<"f">> => 42},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertMatch({error, _, _}, Ret).

%% --- Empty map for required fields ---

empty_map_required_int_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    %% Either returns an error or produces some output; verify it doesn't crash
    ?assert(is_binary(Ret) orelse is_tuple(Ret)).

empty_map_required_string_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_string.avsc">>),
    Term = #{},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assert(is_binary(Ret) orelse is_tuple(Ret)).

%% --- Schema init returns same ID for same file ---

schema_init_idempotent_test() ->
    Id1 = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Id2 = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    ?assertEqual(Id1, Id2).

%% --- Different schemas get different IDs ---

schema_init_different_test() ->
    Id1 = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Id2 = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
    ?assertNotEqual(Id1, Id2).

%% --- Bad enum value ---

bad_enum_value_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/enum.avsc">>),
    Term = #{<<"enumField">> => <<"NONEXISTENT">>},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertMatch({error, _, 11}, Ret).

%% --- Wrong type in array ---

wrong_array_element_type_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Term = #{<<"arrayField">> => [<<"string_not_int">>]},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertMatch({error, _, _}, Ret).

%% --- Int overflow for int field ---

int_overflow_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    Term = #{<<"f">> => 2147483648},
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertMatch({error, _, _}, Ret).
