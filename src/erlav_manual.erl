-module(erlav_manual).

-export([ man_tst/1, recofrec/0]).

man_tst(1) ->
    Term1 = #{
            <<"event">> => <<"event9">>,
            <<"sampling">> => <<"sampling3">>,
            <<"timestamp">> => 111.222,
            <<"seller_request_cpm">> => 1111,
            <<"seller_request_fee_cpm">> => 2222,
            <<"seller_trader_fee_cpm">> => 3333,
            <<"maxtime">> => 4444,
            <<"filtered_app_compatible_result">> => #{
                <<"facr_key2">> => [777777, 888],
                <<"facr_key1">> => [55555, 66666]
            },
            <<"filtered_ssp_result">> => #{
                <<"fsr_key2">> => [3333, 44444],
                <<"fsr_key1">> => [1111, 22222]
            },
            <<"error">> => <<"errorDescription">>,
            <<"error_info">> => #{
                <<"exchange_id">> => 1,
                <<"iplist_id">> => 2,
                <<"ip">> => <<"127.0.0.1">>,
                <<"exchange_seller_id">> => 4,
                <<"exchange_seller_app_id">> => 5,
                <<"exchange_seller_site_id">> => 6
            },
            <<"bid_response">> => #{
                <<"id">> => <<"bid_response_id">>,
                <<"seatbid">> => [
                    #{
                        <<"bidid">> => <<"bidid">>,
                        <<"bid">> => [1,2,3,4,5],
                        <<"cur">> => <<"cur">>
                    }
                ]
            }
    },
    ctst(Term1);
man_tst(2) ->
    Term1 = #{
            <<"event">> => <<"eeeee">>,
            <<"bid_response">> => #{
                <<"id">> => <<"bid_response_id">>,
                <<"bidid">> => <<"bbbbb">>,
                <<"cur">> => <<"ccccc">>,
                <<"seatbid">> => [
                    #{
                      <<"bid">> => [1,2,1,2,1]
                    }
                ]
            }
    },
    ctst(Term1);
man_tst(3) ->
    Term1 = #{
            <<"event">> => <<"eeeee">>,
            <<"bid_response">> => #{
                <<"id">> => <<"bid_response_id">>,
                <<"bidid">> => <<"bbbbb">>,
                <<"cur">> => <<"ccccc">>,
                <<"seatbid">> => [
                    #{
                        <<"bid">> => [
                            #{
                              <<"id">> => <<"seatbid_bid_id1">>
%                              <<"ext">> => #{}
                            }
                        ]
                    }
                ]
            }
    },
    ctst(Term1).

ctst(Term1) ->
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    SchemaId = erlav_nif:create_encoder(<<"test/opnrtb_test1.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    io:format("2.C++ ret: ~p ~n", [Re2]),
    T2 = Decoder(Re2),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok.

recofrec() ->
    {ok, SchemaJSON1} = file:read_file("test/rec_of_rec.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:create_encoder(<<"test/rec_of_rec.avsc">>),
    Term1 = #{
        <<"recordField">> => #{
            <<"rec2field">> => 2
         }
    },
    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    io:format("2.C++ ret: ~p ~n", [Re2]),
    T2 = Decoder(Re2),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    
    Term2 = #{
        <<"recordField">> => #{
            <<"rec2field">> => 2,
            <<"rec1field">> => #{
                    <<"intrecf1">> => 1
                }
         }
    },
    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term2)) ]),
    Re22 = erlav_nif:do_encode(SchemaId, Term2),
    io:format("2.C++ ret: ~p ~n", [Re22]),
    T22 = Decoder(Re22),
    io:format("3.C++ decoded ret: ~p ~n", [T22]),
    ok.
