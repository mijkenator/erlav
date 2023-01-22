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
        <<"stringField">> => <<"asdadasdasdasd3453534dfgdgd123456789">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:create_encoder(<<"priv/tschema2.avsc">>),
    Ret = erlav_nif:do_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

create_encoder_test() ->
    Ret = erlav_nif:create_encoder(<<"priv/tschema2.avsc">>),
    io:format("CET1 ret: ~p ~n", [Ret]),
    Ret1 = erlav_nif:create_encoder(<<"priv/tschema2.avsc">>),
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
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_all_null.avsc">>),
    Ret = erlav_nif:do_encode(SchemaId, Term),
    ?assertEqual(R1, Ret),

    Term2 = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true
    },
    R21 = iolist_to_binary(Encoder(Term2)),
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_all_null.avsc">>),
    Ret2 = erlav_nif:do_encode(SchemaId, Term2),
    ?assertEqual(R21, Ret2),
    
    Term3 = #{
        <<"intField">> => 1,
        <<"longField">> => 2
    },
    R31 = iolist_to_binary(Encoder(Term3)),
    Ret3 = erlav_nif:do_encode(SchemaId, Term3),
    ?assertEqual(R31, Ret3),
    
    ok.


array_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_array.avsc"),
    Encoder1 = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_array.avsc">>),
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
    A1 = maps:get(<<"arrayField">>,  maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term1)))),
    ?assertEqual([1], A1),
    ?assertEqual(Term1, maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term1)))),
    ?assertEqual(Term2, maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term2)))),
    ?assertEqual(Term3, maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term3)))),
    ?assertEqual(Term4, maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term4)))),
    ?assertEqual(Term5, maps:from_list(Decoder(erlav_nif:do_encode(SchemaId, Term5)))),

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
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_map.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
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
    Ret2 = erlav_nif:do_encode(SchemaId, Term2),
    Ret3 = erlav_nif:do_encode(SchemaId, Term3),
    Ret4 = erlav_nif:do_encode(SchemaId, Term4),
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
