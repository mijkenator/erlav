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
    ctst(Term1);
man_tst(4) ->
    Sid1 = erlav_nif:create_encoder(<<"test/array_null_array.avsc">>),
    Sid2 = erlav_nif:create_encoder(<<"test/array_null_arraymaps.avsc">>),
    io:format("Sid1: ~p , Sid2: ~p ~n", [Sid1, Sid2]),
    {ok, SchemaJSON1} = file:read_file("test/array_null_arraymaps.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
             <<"arrayField">> => [
                                  [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}]
                ]
            },

    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term)) ]),
    Re2 = erlav_nif:do_encode(Sid2, Term),
    io:format("2.C++ ret: ~p ~n", [Re2]),
    T2 = Decoder(Re2),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(5) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid2 = erlav_nif:create_encoder(<<"test/array_arraymaps.avsc">>),
    io:format("~n ------------------------------------ ~n", []),
    io:format("Sid: ~p ~n", [Sid2]),
    {ok, SchemaJSON1} = file:read_file("test/array_arraymaps.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
             <<"arrayField">> => [
                                  [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}]
                ]
            },

    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term)) ]),
    Re2 = erlav_nif:do_encode(Sid2, Term),
    io:format("2.C++ ret: ~p ~n", [Re2]),
    T2 = Decoder(Re2),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(6) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid2 = erlav_nif:create_encoder(<<"test/array_null_recursive_array.avsc">>),
    io:format("~n ------------------------------------ ~n", []),
    io:format("Sid: ~p ~n", [Sid2]),
    Sid = erlav_nif:erlav_init(<<"test/array_null_recursive_array.avsc">>),
    io:format("~n ------------------------------------ ~n", []),
    io:format("Sid: ~p ~n", [Sid]),
    io:format("~n ------------------------------------ ~n", []),
    {ok, SchemaJSON1} = file:read_file("test/array_null_recursive_array.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
             <<"arrayField">> => [
                                  [
                                    [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}],
                                    [#{<<"m21key">> => <<"m21value">>}, #{<<"m22key">> => <<"m22value">>}]
                                  ]
                ]
            },

    io:format("1.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term)) ]),
    Re2 = erlav_nif:do_encode(Sid2, Term),
    io:format("2.C++ ret: ~p ~n", [Re2]),
    T2 = Decoder(Re2),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(7) ->
    io:format("~n ------------------------------------ ~n", []),
    %Sid = erlav_nif:erlav_init(<<"test/array_null_recursive_array.avsc">>),
    %Sid = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Sid = erlav_nif:erlav_init(<<"priv/tschema21.avsc">>),
    Sid2 = erlav_nif:create_encoder(<<"priv/tschema21.avsc">>),
    io:format("Sid: ~p ~n", [Sid]),
    Term = #{
        <<"intField">> => 1,
        <<"longField">> => <<"aaaaa">>,
        <<"floatField">> => 1.1,
        <<"doubleField">> => 2.2,
        <<"stringField">> => <<"lalal">>,
        <<"boolField">> => false 
    },
    io:format("Encoding: ~n", []),
    Ret = erlav_nif:erlav_encode(Sid, Term),
    io:format("ERLAV2 ret: ~p ~n", [Ret]),
    Re2 = erlav_nif:do_encode(Sid2, Term),
    io:format("ERLAV1 ret: ~p ~n", [Re2]),
    Eq = Re2 =:= Ret,
    io:format("Same return: ~p ~n", [Eq]),
    ok;
man_tst(8) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid = erlav_nif:erlav_init(<<"test/array_array_array.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/array_array_array.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
        <<"arrayField">> => [ [[1,2],[3,4],[5,6]], [[7,8], [999,1099]], [ [1199,1299] ]  ]
    },
    Ret = erlav_nif:erlav_encode(Sid, Term1),
    io:format("1.ERLAV2 ret: ~p ~n", [Ret]),
    io:format("2.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    T2 = Decoder(Ret),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(9) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid = erlav_nif:erlav_init(<<"test/tschema_record3.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/tschema_record3.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
        <<"recordField">> => #{
            <<"rec1field">> => 1,
            <<"rec3field">> => 2
        }
    },
    Ret = erlav_nif:erlav_encode(Sid, Term1),
    io:format("1.ERLAV2 ret: ~p ~n", [Ret]),
    io:format("2.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    T2 = Decoder(Ret),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(10) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid = erlav_nif:erlav_init(<<"test/map_arr.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/map_arr.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
        <<"key">> => #{
            <<"m1field">> => [1,2,3,4,5,6,7,8,9,10],
            <<"m3field">> => [999,888,777,333,666]
        }
    },
    Ret = erlav_nif:erlav_encode(Sid, Term1),
    io:format("1.ERLAV2 ret: ~p ~n", [Ret]),
    io:format("2.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    T2 = Decoder(Ret),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok;
man_tst(11) ->
    io:format("~n ------------------------------------ ~n", []),
    Sid = erlav_nif:erlav_init(<<"test/bytes.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/bytes.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
        <<"key">> => <<1,1,1,99,111>>
    },
    Ret = erlav_nif:erlav_encode(Sid, Term1),
    io:format("1.ERLAV2 ret: ~p ~n", [Ret]),
    io:format("2.Erl ret: ~p ~n", [ iolist_to_binary(Encoder(Term1)) ]),
    T2 = Decoder(Ret),
    io:format("3.C++ decoded ret: ~p ~n", [T2]),
    ok.

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

