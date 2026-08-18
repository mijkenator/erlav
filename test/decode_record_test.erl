-module(decode_record_test).

-include_lib("eunit/include/eunit.hrl").


rec1_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record_i.avsc">>),
    Term = #{
        <<"intField">> => 7777,
        <<"recordField">> => #{
            <<"rec1field">> => 11,
            <<"rec2field">> => <<"asasasasa23456789">>,
            <<"rec3field">> => 3333
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

rec2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record3.avsc">>),
    Term = #{
        <<"recordField">> => #{
                <<"rec1field">> => 11,
                <<"rec3field">> => 3333
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

rec3_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/rec_of_rec.avsc">>),
    Term = #{
        <<"recordField">> => #{
                <<"rec2field">> => 117711,
                <<"rec1field">> => #{
                    <<"intrecf1">> => 56789
                }
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

% Regression test for #20's single-pass encoderecord optimization: a record
% with more than SMALL_RECORD_FIELD_THRESHOLD (3) fields takes the
% map-iterator + field_index_by_name path instead of per-field
% enif_get_map_value lookups. Exercises all present, all missing, nullable
% and non-nullable, and array fields in the same record to cover both
% branches of the "found vs not found" logic on that path.
wide_record_all_present_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record_wide.avsc">>),
    Term = #{
        <<"field1">> => 111,
        <<"field2">> => <<"hello">>,
        <<"field3">> => 222,
        <<"field4">> => [1, 2, 3],
        <<"field5">> => 3.14,
        <<"field6">> => <<"world">>
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Term, Re1)),
    ok.

wide_record_missing_nullable_and_array_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record_wide.avsc">>),
    % field3 and field6 (nullable) and field4 (array, defaults to empty)
    % are omitted from the input map entirely -- exercises the "not
    % present" branch of the single-pass lookup for a mix of field kinds.
    Term = #{
        <<"field1">> => 111,
        <<"field2">> => <<"hello">>,
        <<"field5">> => 3.14
    },
    Expected = Term#{
        <<"field3">> => undefined,
        <<"field4">> => [],
        <<"field6">> => undefined
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded result: ~n ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps(Expected, Re1)),
    ok.
