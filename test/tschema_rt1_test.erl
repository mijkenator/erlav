-module(tschema_rt1_test).

-include_lib("eunit/include/eunit.hrl").

%% Round-trip coverage for test/tschema_rt1.avsc -- a real-world nested
%% schema (record -> nullable record -> nullable record -> array of
%% record -> map of array of union[long, named record]) exercising every
%% shape in the file: plain scalars, a nested record (app.ext), an array
%% of records (imp), a map of array of record (filtered_ids), a plain
%% array of record (impression_responses), and a map of array of
%% union[long, <named-type-reference>] (filtered_flight_ids and its two
%% siblings) -- the case that used to segfault (see
%% array_of_union_named_type_ref_items_test in decode_array_test.erl for
%% the isolated regression test of that specific bug).

full_record_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_rt1.avsc">>),
    Term = #{
        <<"event">> => <<"impression">>,
        <<"timestamp">> => 12345.678,
        <<"hostname">> => <<"host1">>,
        <<"bid_request">> => #{
            <<"id">> => <<"bid123">>,
            <<"app">> => #{
                <<"ext">> => #{
                    <<"exchange_id">> => 1,
                    <<"exchange_seller_id">> => 2,
                    <<"exchange_seller_alphanumeric_id">> => <<"abc">>
                }
            },
            <<"imp">> => [
                #{
                    <<"filtered_ids">> => #{
                        <<"grp1">> => [
                            #{<<"buyer_id">> => 1, <<"campaign_id">> => 2, <<"flight_id">> => 3}
                        ]
                    },
                    <<"impression_responses">> => [
                        #{<<"buyer_id">> => 10, <<"campaign_id">> => 20, <<"flight_id">> => 30}
                    ],
                    <<"filtered_flight_ids">> => #{
                        <<"grp2">> => [
                            100,
                            #{<<"buyer_id">> => 1, <<"campaign_id">> => 2, <<"flight_id">> => 3}
                        ]
                    },
                    <<"filtered_paced_flight_ids">> => #{
                        <<"grp3">> => [200]
                    },
                    <<"filtered_internal_auction_flight_ids">> => #{
                        <<"grp4">> => [
                            #{<<"buyer_id">> => 4, <<"campaign_id">> => 5, <<"flight_id">> => 6}
                        ]
                    }
                }
            ]
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

%% Every top-level/nested "type": ["null", ...] field left absent (or, for
%% the map fields, empty) should decode back as `undefined`/an empty map
%% rather than crashing or dropping unrelated fields.
minimal_record_roundtrip_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_rt1.avsc">>),
    Term = #{<<"event">> => <<"click">>},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?assertEqual(<<"click">>, maps:get(<<"event">>, Re1)),
    ?assertEqual(undefined, maps:get(<<"timestamp">>, Re1)),
    ?assertEqual(undefined, maps:get(<<"hostname">>, Re1)),
    ?assertEqual(undefined, maps:get(<<"bid_request">>, Re1)),
    ok.

%% filtered_flight_ids (and its two siblings) is a map whose values are
%% arrays of union[long, filteres_ids_map_value] -- the named-type
%% reference that used to segfault decode_array. Exercise the "long"
%% member on its own, isolated from the "record" member covered above.
filtered_flight_ids_long_only_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_rt1.avsc">>),
    Term = #{
        <<"event">> => <<"impression">>,
        <<"bid_request">> => #{
            <<"imp">> => [
                #{
                    <<"filtered_flight_ids">> => #{
                        <<"grp">> => [1, 2, 3]
                    }
                }
            ]
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    Imp = hd(maps:get(<<"imp">>, maps:get(<<"bid_request">>, Re1))),
    ?assertEqual([1, 2, 3], maps:get(<<"grp">>, maps:get(<<"filtered_flight_ids">>, Imp))),
    ok.
