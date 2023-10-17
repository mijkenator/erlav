-module(erlav_encode_test).

-include_lib("eunit/include/eunit.hrl").

primitive_types_test() ->
    {ok, SchemaJSON} = file:read_file("priv/tschema2.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true,
        <<"stringField">> => <<"asdadasdasdasd3453534dfgdgd123456789">>,
        <<"bytesField">> => <<1,99,57,127,0,56>>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

enum_test() ->
    {ok, SchemaJSON} = file:read_file("test/enum.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"enumField">> => <<"DIAMONDS">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/enum.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret),
    
    BadTerm = #{
        <<"enumField">> => <<"DIAMONDS1">>
    },
    {error, ErrMsg, ErrCode} = erlav_nif:erlav_encode(SchemaId, BadTerm),
    io:format("c++ ret: ~p ~n", [ErrMsg]),
    ?assertEqual(ErrCode, 11).

enum_null_test() ->
    {ok, SchemaJSON} = file:read_file("test/enum_null.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"enumField">> => <<"DIAMONDS">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/enum_null.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret),
    
    BadTerm = #{
        <<"enumField">> => <<"DIAMONDS1">>
    },
    {error, ErrMsg, ErrCode} = erlav_nif:erlav_encode(SchemaId, BadTerm),
    io:format("c++ ret: ~p ~n", [ErrMsg]),
    ?assertEqual(ErrCode, 11).

create_encoder_test() ->
    Ret = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
    io:format("CET1 ret: ~p ~n", [Ret]),
    Ret1 = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
    io:format("CET2 ret: ~p ~n", [Ret]),
    ?assertEqual(Ret, Ret1),
    ok.

null_all_test() ->
    {ok, SchemaJSON} = file:read_file("test/tschema_all_null.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true,
        <<"stringField">> => <<"asdadasdasdasd3453534dfgdgd123456789">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_all_null.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(R1, Ret),

    Term2 = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true
    },
    R21 = iolist_to_binary(Encoder(Term2)),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_all_null.avsc">>),
    Ret2 = erlav_nif:erlav_encode(SchemaId, Term2),
    ?assertEqual(R21, Ret2),
    
    Term3 = #{
        <<"intField">> => 1,
        <<"longField">> => 2
    },
    R31 = iolist_to_binary(Encoder(Term3)),
    Ret3 = erlav_nif:erlav_encode(SchemaId, Term3),
    ?assertEqual(R31, Ret3),
    
    ok.


array_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_array.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_array.avsc">>),
    Term1 = #{
        <<"arrayField">> => [1]
    },
    Term2 = #{
        <<"arrayField">> => [1,1]
    },
    Term3 = #{
        <<"arrayField">> => [1,1,1]
    },
    Term4 = #{
        <<"arrayField">> => [1,10000,1]
    },
    Term5 = #{
        <<"arrayField">> => [10000]
    },
    A1 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),
    ?assertEqual([1], A1),
    ?assertEqual(Term1, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),
    ?assertEqual(Term2, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term2)))),
    ?assertEqual(Term3, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term3)))),
    ?assertEqual(Term4, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term4)))),
    ?assertEqual(Term5, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term5)))),

    ok.

array_m_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_multi_type.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type.avsc">>),
    Term1 = #{
        <<"arrayField">> => [1, 11, 111, 1111, 11111, 111111111111]
    },
    E1 = erlav_nif:erlav_encode(SchemaId, Term1),
    E2 = Encoder1(Term1),
    ?debugFmt("Erlang encoded: ~p ~n", [E2]),
    ?debugFmt("Erlav encoded: ~p ~n", [E1]),

    A1 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),
    ?assertEqual([1, 11, 111, 1111, 11111, 111111111111], A1),
    ?assertEqual(Term1, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),

    Term2 = #{
        <<"arrayField">> => [<<"lalala">>, <<"kokoko">>]
    },
    E21 = erlav_nif:erlav_encode(SchemaId, Term2),
    E22 = Encoder1(Term2),
    ?debugFmt("Erlang encoded: ~p ~n", [E22]),
    ?debugFmt("Erlav encoded: ~p ~n", [E21]),

    A2 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term2)))),
    ?assertEqual([<<"lalala">>, <<"kokoko">>], A2),
    ok.

array_m2_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_multi_type2.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:erlav_init(<<"test/array_multi_type2.avsc">>),
    Term1 = #{
        <<"arrayField">> => [1, [3]]
    },
    E1 = erlav_nif:erlav_encode(SchemaId, Term1),
    E2 = Encoder1(Term1),
    ?debugFmt("Erlang encoded: ~p ~n", [E2]),
    ?debugFmt("Erlav encoded: ~p ~n", [E1]),

    A1 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),
    ?assertEqual([1, [3]], A1),
    ?assertEqual(Term1, maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term1)))),

    Term2 = #{
        <<"arrayField">> => [1, [<<"lalala">>, 55, <<"kokoko">>],3]
    },
    E21 = erlav_nif:erlav_encode(SchemaId, Term2),
    E22 = Encoder1(Term2),
    ?debugFmt("Erlang encoded: ~p ~n", [E22]),
    ?debugFmt("Erlav encoded: ~p ~n", [E21]),

    A2 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term2)))),
    ?assertEqual([1, [<<"lalala">>, 55, <<"kokoko">>], 3], A2),
    
    Term3 = #{
        <<"arrayField">> => [1, [3, 4, 5], 2, [7], 99, [9,6,7,8,9], 11]
    },
    E31 = erlav_nif:erlav_encode(SchemaId, Term3),
    E32 = Encoder1(Term3),
    ?debugFmt("Erlang encoded: ~p ~n", [E32]),
    ?debugFmt("Erlav encoded: ~p ~n", [E31]),

    A3 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:erlav_encode(SchemaId, Term3)))),
    ?assertEqual([1, [3, 4, 5], 2, [7], 99, [9,6,7,8,9], 11], A3),
    ok.

map_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_map.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"mapField">> => 
              #{
                <<"k1">> => 1,
                <<"k2">> => 2
                }
    },
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    RTerm = maps:from_list(Decoder(Re2)),
    ?assertEqual(RTerm, #{<<"mapField">> => [{<<"k1">>,1},{<<"k2">>,2}]}),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [RTerm]),[append]),
    Term2 = #{
              <<"mapField">> => 
              #{
                <<"aaa">> => 1,
                <<"bbb">> => 2,
                <<"ccc">> => 3
                }
    },
    Term3 = #{
              <<"mapField">> => 
              #{
                <<"aaa">> => 1,
                <<"ccc">> => 2,
                <<"bbb">> => 3
                }
    },
    Term4 = #{
              <<"mapField">> => 
              #{
                <<"ccc">> => 1,
                <<"bbb">> => 2,
                <<"aaa">> => 3
                }
    },
    Ret2 = erlav_nif:erlav_encode(SchemaId, Term2),
    Ret3 = erlav_nif:erlav_encode(SchemaId, Term3),
    Ret4 = erlav_nif:erlav_encode(SchemaId, Term4),
    TRet2 = maps:from_list(Decoder(Ret2)),
    TRet3 = maps:from_list(Decoder(Ret3)),
    TRet4 = maps:from_list(Decoder(Ret4)),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet2]),[append]),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet3]),[append]),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet4]),[append]),
    ?assertEqual(TRet2, #{<<"mapField">> => [{<<"aaa">>,1},{<<"bbb">>,2},{<<"ccc">>,3}]}),
    ?assertEqual(TRet3, #{<<"mapField">> => [{<<"aaa">>,1},{<<"bbb">>,3},{<<"ccc">>,2}]}),
    ?assertEqual(TRet4, #{<<"mapField">> => [{<<"aaa">>,3},{<<"bbb">>,2},{<<"ccc">>,1}]}),
    ok.

map_null_test() ->
    {ok, SchemaJSON1} = file:read_file("test/map_int_null.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"mapField">> => 
              #{
                <<"k1">> => 1,
                <<"k2">> => 2
                }
    },
    SchemaId = erlav_nif:erlav_init(<<"test/map_int_null.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    RTerm = maps:from_list(Decoder(Re2)),
    ?assertEqual(RTerm, #{<<"mapField">> => [{<<"k1">>,1},{<<"k2">>,2}]}),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [RTerm]),[append]),
    Term2 = #{
              <<"mapField">> => 
              #{
                <<"aaa">> => 1,
                <<"bbb">> => 2,
                <<"ccc">> => 3
                }
    },
    Term3 = #{
              <<"mapField">> => 
              #{
                <<"aaa">> => 1,
                <<"ccc">> => 2,
                <<"bbb">> => 3
                }
    },
    Term4 = #{
              <<"mapField">> => 
              #{
                <<"ccc">> => 1,
                <<"bbb">> => 2,
                <<"aaa">> => 3
                }
    },
    Ret2 = erlav_nif:erlav_encode(SchemaId, Term2),
    Ret3 = erlav_nif:erlav_encode(SchemaId, Term3),
    Ret4 = erlav_nif:erlav_encode(SchemaId, Term4),
    TRet2 = maps:from_list(Decoder(Ret2)),
    TRet3 = maps:from_list(Decoder(Ret3)),
    TRet4 = maps:from_list(Decoder(Ret4)),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet2]),[append]),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet3]),[append]),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [TRet4]),[append]),
    ?assertEqual(TRet2, #{<<"mapField">> => [{<<"aaa">>,1},{<<"bbb">>,2},{<<"ccc">>,3}]}),
    ?assertEqual(TRet3, #{<<"mapField">> => [{<<"aaa">>,1},{<<"bbb">>,3},{<<"ccc">>,2}]}),
    ?assertEqual(TRet4, #{<<"mapField">> => [{<<"aaa">>,3},{<<"bbb">>,2},{<<"ccc">>,1}]}),
    ok.

null_map_test() ->
    {ok, SchemaJSON1} = file:read_file("test/map_int_null.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"mapField">> => 
              #{
                <<"k1">> => 1,
                <<"k2">> => 2
                }
    },
    SchemaId = erlav_nif:erlav_init(<<"test/map_int_null.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    RTerm = maps:from_list(Decoder(Re2)),
    io:format("======================================================== ~n"),
    io:format("~p ~n", [RTerm]),
    io:format("======================================================== ~n"),
    ?assertEqual(RTerm, #{<<"mapField">> => [{<<"k1">>,1},{<<"k2">>,2}]}),
    ok.

record_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_record.avsc"),
    %Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"recordField">> => #{
                    <<"rec1field">> => 1,
                    <<"rec2field">> => <<"koko">>,
                    <<"rec3field">> => 2
                }
    },
    
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [RTerm]),[append]),
    ?assertEqual(1, proplists:get_value(<<"rec1field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(<<"koko">>, proplists:get_value(<<"rec2field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(2, proplists:get_value(<<"rec3field">>, maps:get(<<"recordField">>, M))).

record2_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_record2.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"recordField">> => #{
                    <<"rec1field">> => 1,
                    <<"rec2field">> => <<"koko">>,
                    <<"rec3field">> => 2,
                    <<"rec4field">> => 112233,
                    <<"rec5field">> => true,
                    <<"rec6field">> => 11.22,
                    <<"rec7field">> => 33.44
                }
    },
    
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record2.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [RTerm]),[append]),
    ?assertEqual(1, proplists:get_value(<<"rec1field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(<<"koko">>, proplists:get_value(<<"rec2field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(2, proplists:get_value(<<"rec3field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(112233, proplists:get_value(<<"rec4field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(true, proplists:get_value(<<"rec5field">>, maps:get(<<"recordField">>, M))),
    %?assertEqual(33.44, proplists:get_value(<<"rec7field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(11.22, proplists:get_value(<<"rec6field">>, maps:get(<<"recordField">>, M))).

record3_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_record3.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"recordField">> => #{
                    <<"rec1field">> => 1,
                    <<"rec3field">> => 2
                }
    },
    
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_record3.avsc">>),
    Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    %file:write_file("/tmp/rfile.txt", io_lib:format("~p~n", [RTerm]),[append]),
    ?assertEqual(1, proplists:get_value(<<"rec1field">>, maps:get(<<"recordField">>, M))),
    ?assertEqual(2, proplists:get_value(<<"rec3field">>, maps:get(<<"recordField">>, M))).


multistring_null_test() ->
    {ok, SchemaJSON} = file:read_file("test/perfstr_null.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"stringField1">> => <<"111asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField2">> => <<"222asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField3">> => <<"333asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField4">> => <<"444asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField5">> => <<"555asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField6">> => <<"666asdadasdasdasd3453534dfgdgd123456789">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("=============== MSTRING erlang =================== ~n ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/perfstr_null.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("=============== MSTRING c++ =================== ~n ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

multistring_test() ->
    {ok, SchemaJSON} = file:read_file("test/perfstr.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"stringField1">> => <<"111asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField2">> => <<"222asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField3">> => <<"333asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField4">> => <<"444asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField5">> => <<"555asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField6">> => <<"666asdadasdasdasd3453534dfgdgd123456789">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("=============== MSTRING erlang =================== ~n ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/perfstr.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("=============== MSTRING c++ =================== ~n ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

multistring_b64_test() ->
    Term = #{
         <<"stringField1">> => <<"Ym05k/DOEysNuDAmuE9qSF6B6scJ4N+dfnvKDmv+89qE/6KK7FPLtklm2srs3LrmFHU=">>,
         <<"stringField2">> => <<"rTY5OUZ1eWC0IsX07JkOJKd49sMGfpcwohuCKKzMu5CAj8V+mUlySmYFpIqn1rj88FA=">>,
         <<"stringField3">> => <<"GiQ5QE24HoWETjYHSMwX0ek3uM6OBLpTCCRj91VXE+n8jyLGeFk1eSkLFy9iMyNlG6U=">>,
         <<"stringField4">> => <<"Hgtvph4GDVfsOFBhfQI8FkpJx0/essrDSUcbJI2Z+iXlClpkl62u1UntWcxN1U1BnNQ=">>,
         <<"stringField5">> => <<"6OS4YNuKObjnNYG8jRbSZ9x53W1pUYQU5mqLFBe2+qOmZgCmrjebR3L+FRGJekXZC2I=">>,
         <<"stringField6">> => <<"zu3X50DdE8Stpzh0HuPwEFerVqhM0EHrpEW7L7YhzmMNJ2/P6uTN7oEDmATHk+qnIQE=">>
    },
    {ok, SchemaJSON} = file:read_file("test/perfstr.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    R1 = iolist_to_binary(Encoder(Term)),
    ?debugFmt("=============== MSTRING gen erlang =================== ~n ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/perfstr.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("=============== MSTRING gen c++ =================== ~n ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

