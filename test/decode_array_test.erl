-module(decode_array_test).

-include_lib("eunit/include/eunit.hrl").


scalar_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    Pairs = [{<<"arrayField">>, [1,2,3,4,5,6,7,8,9,10,9,8,7,6,5,4,3,2,1]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

str_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    Pairs = [{<<"arrayField">>, [<<"aaaaaa3">>, <<"bbbbbb2">>, <<"cccccccccccccc1">>]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

complex_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>),
    Pairs = [{<<"arrayField">>, [<<"aaaaaa3">>, 1111, 77, <<"cccccccccccccc1">>, 0, 1, <<"sasd">>]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

complex2_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_multi_type2.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),

    Pairs = [{<<"arrayField">>, [1111, [1,2,3,4,5], 77]}],
    %Pairs = [{<<"arrayField">>, [1, [2], 3]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded erlav: ~p ~n", [Encoded]),

    E2 = Encoder1(Term),
    ?debugFmt("Encoded erlavro: ~p ~n", [E2]),
    TE = Decoder(Encoded),
    ?debugFmt("decoded erlav->erlavro: ~p ~n", [TE]),

    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    lists:foreach(fun({Key, Value}) -> 
        Value1 = maps:get(Key, Re1),
        ?assertEqual(Value, Value1)
    end, Pairs),
    ok.

array_of_recs_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs.avsc">>),
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
             },
            #{
                <<"rec1field">> => 2111,
                <<"rec2field">> => <<"2str1">>,
                <<"rec3field">> => 23333,
                <<"rec4field">> => 21,
                <<"rec5field">> => true,
                <<"rec6field">> => 21.11,
                <<"rec7field">> => 22.222
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

array_of_recs0_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs0.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_recs1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs1.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_recs2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_ofrecs_nullable.avsc">>),
    Term = #{
        <<"arrayField">> => [
            #{
                <<"rec1field">> => 2,
                <<"rec3field">> => 4
             },
            #{
                <<"rec1field">> => 22,
                <<"rec3field">> => 34
             }
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_arr3_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array_array.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    %Term = #{
    %    <<"arrayField">> => [
    %        [ [1,2,3], [4,5,6], [7,8,9] ],
    %        [ [11,22,33], [14,15,16], [117,118,119] ]
    %    ]
    %},
    Term = #{
        <<"arrayField">> => [
            [ [1,1,1] ]
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

array_of_arr4_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,1,1], [2,3,4], [44], [9,8,7]
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3], [4,5,6], [7,8,9], [0], [0], [1,1,1,1,1,1], [5]
        ]
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

%
% test for array of nullable arrays
%
array_of_arr5_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_many.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_many.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,2,3,4], [<<"asdad">>, <<"121212">>]
        ],
        <<"endf">> => <<"kokoko">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),

    ok.

array_of_arr6_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple1.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            1,2,3,4
        ],
        <<"endlf">> => 111
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),

    ok.

array_of_arr7_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_array2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array2.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,1,1], [2,3,4], [44], [9,8,7]
        ],
        <<"endl1">> => 11
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3], [4,5,6], [7,8,9], [0], [0], [1,1,1,1,1,1], [5]
        ],
        <<"endl1">> => 11
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

array_of_arr8_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_null_simple2.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_null_simple2.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
        <<"arrayField">> => [
            [1,2], [1,1,1,1], [<<"str1">>, <<"str2">>], [1], [<<"1122233">>]
        ],
        <<"endf">> => <<"kokokoko">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    Re2 = Decoder(Encoded),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re2]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    
    ?debugFmt("=============================================!!!!!!!!!!!======================", []),
    Term1 = #{
        <<"arrayField">> => [
            [1,2,3,4], [<<"asdad">>, <<"121212">>]
        ],
        <<"endf">> => <<"kokoko">>
    },
    Encoded1 = erlav_nif:erlav_encode(SchemaId, Term1),
    ?debugFmt("Encoded: ~p ~n", [Encoded1]),
    Re3 = erlav_nif:erlav_decode_fast(SchemaId, Encoded1),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re3]),
    Re4 = Decoder(Encoded1),
    ?debugFmt("Erlavro Decoded result: ~n ~p ~n", [Re4]),
    ?assert(true == tst_utils:compare_maps(Term1, Re3)),

    ok.

array_bad_type_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple.avsc">>),
    
    Pairs = [{<<"arrayField">>, [<<"2">>]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    ok.

array_bad_type1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_simple1.avsc">>),
    
    Pairs = [{<<"arrayField">>, [<<"2">>]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    Msg1 = erlav_nif:erlav_encode(SchemaId, maps:from_list([{<<"arrayField">>, [2, <<"2">>]}])),
    ?debugFmt("Encoded: ~p ~n", [Msg1]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg1),

    ok.

array_bad_type2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_str.avsc">>),
    
    Pairs = [{<<"arrayField">>, [2]}],
    Term = maps:from_list(Pairs),

    Msg = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Msg]),
    ?assert({error, "Rec:Interop field:arrayField", 8} == Msg),

    ok.

complex_array_bad_type_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>),
    Pairs = [{<<"arrayField">>, [2.2]}],
    Term = maps:from_list(Pairs),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),

    ?assert({error, "Rec:Interop field:arrayField", 8} == Encoded),
    ok.

% array whose *items* are a union of a scalar (long) and a record, e.g.
% {"type": "array", "items": ["long", {"type": "record", ...}]}.
% Regression test for tschema_array_of_union1.avsc: encoding/decoding an
% array mixing plain longs with union-member records should round-trip.
array_of_union_record_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union1.avsc">>),
    Term = #{
        <<"arrayField">> => [
            1,
            2,
            #{<<"rec1field">> => 10, <<"rec3field">> => 20},
            3
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% Companion to array_of_union_record_items_test -- array items union of a
% scalar (long) and a map, e.g.
% {"type": "array", "items": ["long", {"type": "map", "values": "string"}]}.
array_of_union_map_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union2.avsc">>),
    Term = #{
        <<"arrayField">> => [
            1,
            2,
            #{<<"k1">> => <<"v1">>, <<"k2">> => <<"v2">>},
            3
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% Companion to array_of_union_record_items_test -- array items union of a
% scalar (long) and an enum, e.g.
% {"type": "array", "items": ["long", {"type": "enum", "symbols": [...]}]}.
% On the Erlang side both string and enum union members are plain
% binaries, so the encoder must be able to tell them apart (tries string
% first, falls back to enum).
array_of_union_enum_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union3.avsc">>),
    Term = #{
        <<"arrayField">> => [
            1, <<"TWO">>, 2, <<"ONE">>
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% Array items union with more than one non-scalar member: long, nested
% array, and record all in the same union. Exercises
% array_multi_type_child_index -- each non-scalar member must resolve to
% its own childItems slot instead of colliding on childItems[0].
array_of_union_multi_nonscalar_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union4.avsc">>),
    Term = #{
        <<"arrayField">> => [
            1,
            [10, 20, 30],
            #{<<"rec1field">> => 10, <<"rec3field">> => 20},
            2
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% Array items union that includes "null" alongside scalars, e.g.
% {"type": "array", "items": ["null", "long", "string"]}. `undefined`
% array elements must round-trip as the null union member, interleaved
% with plain scalar elements.
array_of_union_null_scalar_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union5.avsc">>),
    Term = #{
        <<"arrayField">> => [1, undefined, <<"x">>, undefined]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1),
    ok.

% Companion to array_of_union_null_scalar_items_test -- "null" alongside a
% scalar and a record member, e.g.
% {"type": "array", "items": ["null", "long", {"type": "record", ...}]}.
array_of_union_null_record_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union6.avsc">>),
    Term = #{
        <<"arrayField">> => [1, undefined, #{<<"a">> => 5}, undefined]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1),
    ok.

% An atom other than `undefined` (e.g. `true`) is not a valid Avro value
% for any member of ["null", "long", "string"] and must be rejected
% rather than silently misencoded.
array_of_union_bad_atom_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union5.avsc">>),
    Term = #{<<"arrayField">> => [1, true]},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assertEqual({error, "Rec:Interop field:arrayField", 8}, Encoded),
    ok.

% Regression test for a named-type *reference* inside an array-items
% union, e.g. {"type": "array", "items": ["long", "named_union_item"]}
% where "named_union_item" is a record defined elsewhere in the schema
% (not an inline object, unlike tschema_array_of_union1..6.avsc). Prior to
% the fix, resolve_user_types never inlined this reference (it only
% covered namespace-qualified names in top-level fields), so
% set_array_multi_types silently treated the bare name as if it were a
% scalar keyword; decode_array then fell through its eletype dispatch
% with no matching branch, desyncing the read cursor and handing
% enif_make_list_from_array uninitialized memory -- a segfault, not a
% clean failure. See test/tschema_array_of_union7.avsc.
array_of_union_named_type_ref_items_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union7.avsc">>),
    Term = #{
        <<"definingField">> => [#{<<"rec1field">> => 1, <<"rec3field">> => 2}],
        <<"arrayField">> => [
            1,
            2,
            #{<<"rec1field">> => 10, <<"rec3field">> => 20},
            3
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% A self-referential named type (a record whose own field references its
% own name, e.g. a tree/linked-list shape: {"name":"Node", "fields":
% [..., {"type": {"type":"array","items":"Node"}}]}) must not hang or
% crash schema resolution. resolve_named_type_refs's `active` guard
% expands a self-reference once per recursion path rather than looping
% forever, so a shallow tree (depth <= 2, i.e. root -> children with no
% grandchildren) round-trips; a deeper tree currently exceeds what a
% single JSON-substitution pass can inline and fails cleanly with a
% catchable encode error (see self_referential_deep_test) rather than
% hanging or crashing -- full unbounded recursive-schema support would
% need SchemaItem to support self-referencing pointers, which is a
% separate, larger change than this bug fix.
self_referential_shallow_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_self_ref.avsc">>),
    Term = #{
        <<"label">> => <<"root">>,
        <<"children">> => [
            #{<<"label">> => <<"child1">>, <<"children">> => []},
            #{<<"label">> => <<"child2">>, <<"children">> => []}
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

% Companion to self_referential_shallow_test -- a grandchild-deep tree
% goes past what the current single-pass resolver can inline. This must
% still fail as a clean, catchable {error, _, _} tuple, not hang or
% crash, documenting today's depth limit.
self_referential_deep_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_self_ref.avsc">>),
    Term = #{
        <<"label">> => <<"root">>,
        <<"children">> => [
            #{<<"label">> => <<"child1">>, <<"children">> => [
                #{<<"label">> => <<"grandchild1">>, <<"children">> => []}
            ]}
        ]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assertMatch({error, _, _}, Encoded),
    ok.

% Belt-and-suspenders regression test for decode_array's defensive `else`
% branch: an array-items union member whose name never resolves to
% anything (not a scalar, not "null", not a registered record/enum) must
% surface as a catchable {error, _, _} tuple from erlav_decode_fast, not
% crash the VM. Before this fix, decode_array's eletype dispatch fell
% through silently (no bytes consumed, uninitialized memory handed to
% enif_make_list_from_array); after the mkh_avro2.hh resolver rewrite,
% an unresolvable name can still reach here for a schema that genuinely
% doesn't define the referenced type anywhere (a real schema/data bug,
% not the named-type-in-union case this PR fixes) -- crafted by hand here
% since the encoder itself refuses to produce bytes for such a union
% member (see array_of_union_bad_atom_test-style encode failures).
array_of_union_unresolved_member_decode_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array_of_union_unresolved.avsc">>),
    % arrayLen=1 (varint 2), union type_index=1 i.e. the unresolved
    % "totally_unknown_type" member (varint 2), end-of-array marker (0).
    Bad = <<2, 2, 0>>,
    Ret = erlav_nif:erlav_decode_fast(SchemaId, Bad),
    ?debugFmt("decode result: ~p ~n", [Ret]),
    ?assertMatch({error, _, 10}, Ret),
    ok.
