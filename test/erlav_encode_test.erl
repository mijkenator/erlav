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

