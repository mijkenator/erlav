-module(erlav_array_decode_test).

-include_lib("eunit/include/eunit.hrl").

%% erlav_array_test.erl's coverage for array<record>/array<map>/etc. only
%% ever decodes the *encoded* bytes with avro:make_simple_decoder
%% (erlavro's own decoder) -- never with erlav_nif:erlav_decode/
%% erlav_decode_fast. That blind spot is exactly how #44 went unnoticed:
%% decode_array's complex-array-of-one-type branch called decode() (the
%% record-field-loop decoder) unconditionally instead of decodevalue()
%% (the general per-type dispatcher used everywhere else), which happened
%% to "work" for array<record> by coincidence but crashed the VM on
%% array<map> and silently returned #{} for every element of
%% array<fixed>/array<enum>. These tests close that gap by decoding
%% through erlav's own decoder, not just erlavro's.

array_of_map_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_map.avsc">>),
    Term = #{<<"arrayField">> => [
        #{<<"rec1field">> => <<"lalalal">>, <<"rec2field">> => <<"koko">>}
    ]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

array_of_record_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs.avsc">>),
    Term = #{<<"arrayField">> => [
        #{
            <<"rec1field">> => 1,
            <<"rec2field">> => <<"koko">>,
            <<"rec3field">> => 2,
            <<"rec4field">> => 112233,
            <<"rec5field">> => true,
            <<"rec6field">> => 11.22,
            <<"rec7field">> => 33.44
        }
    ]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    [M1] = maps:get(<<"arrayField">>, Re1),
    ?assertEqual(1, maps:get(<<"rec1field">>, M1)),
    ?assertEqual(<<"koko">>, maps:get(<<"rec2field">>, M1)),
    ?assertEqual(112233, maps:get(<<"rec4field">>, M1)),
    ?assertEqual(true, maps:get(<<"rec5field">>, M1)).

array_of_array_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array.avsc">>),
    Term = #{<<"arrayField">> => [[1,2,3], [4,5], [6], [7,8,9,10]]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(Term, Re1).

%% array<fixed> -- previously decoded every element to #{} instead of
%% the fixed-size binary, with no crash and no error (silent data loss).
array_of_fixed_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_fixed.avsc">>),
    Term = #{<<"arrayField">> => [<<1,2,3,4>>, <<5,6,7,8>>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).

%% array<enum> -- previously decoded every element to #{} instead of the
%% enum symbol string, with no crash and no error (silent data loss).
array_of_enum_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_enum.avsc">>),
    Term = #{<<"arrayField">> => [<<"A">>, <<"B">>, <<"C">>]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1).
