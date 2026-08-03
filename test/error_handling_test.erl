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

%% --- Unknown schema id ---
%%
%% erlav_encoders_map (c_src/erlav_nif.cpp) is keyed by the integer schema id
%% returned from erlav_init/1. A ref that was never registered (or a
%% completely bogus integer) used to be looked up via std::map::operator[],
%% which silently inserts a null SchemaItem* entry for a missing key --
%% encode/decode then dereferenced that null pointer and crashed the NIF
%% (took the whole BEAM down with it, not just the calling process). The
%% lookup now goes through find_schema/1 (std::map::find, a true read) and
%% returns badarg for an unknown id instead. These are regression tests for
%% that fix, not just "does it error" checks -- an unrecognized SchemaId must
%% raise badarg, not crash the VM or hang.

unknown_schema_id_encode_test() ->
    ?assertError(badarg, erlav_nif:erlav_encode(999999999, #{<<"f">> => 1})).

unknown_schema_id_decode_test() ->
    ?assertError(badarg, erlav_nif:erlav_decode(999999999, <<1, 2, 3>>)).

unknown_schema_id_decode_fast_test() ->
    ?assertError(badarg, erlav_nif:erlav_decode_fast(999999999, <<1, 2, 3>>)).

%% 0 is never handed out as a real schema id (ids start at 1 -- see
%% erlav_init_nif) -- it must be rejected like any other unknown id.
schema_id_zero_encode_test() ->
    ?assertError(badarg, erlav_nif:erlav_encode(0, #{<<"f">> => 1})).

%% A negative ref can't be a real schema id (ids are always >= 1) and must
%% be rejected the same way as any other unknown id.
negative_schema_id_encode_test() ->
    ?assertError(badarg, erlav_nif:erlav_encode(-1, #{<<"f">> => 1})).
