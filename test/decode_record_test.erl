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
