-module(erlav_int_max_test).

-include_lib("eunit/include/eunit.hrl").

int_test() ->
    {ok, SchemaJSON} = file:read_file("test/int_long.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 2147483647
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/int_long.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

int_moremax_test() ->
    {ok, SchemaJSON} = file:read_file("test/int_long.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 2147483648 
    },
    R1 = try iolist_to_binary(Encoder(Term)) of
        R1et -> R1et
    catch
        _:_ -> error
    end,
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/int_long.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, element(1, Ret)).

long_test() ->
    {ok, SchemaJSON} = file:read_file("test/int_long.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"longField">> => 9223372036854775807
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/int_long.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

long_moremax_test() ->
    {ok, SchemaJSON} = file:read_file("test/int_long.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"longField">> => 9223372036854775808
    },
    R1 = try iolist_to_binary(Encoder(Term)) of
        R1et -> R1et
    catch
        _:_ -> error
    end,
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/int_long.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, element(1, Ret)).

