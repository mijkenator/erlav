-module(udt_test).

-include_lib("eunit/include/eunit.hrl").

simple1_test() ->
    {ok, SchemaJSON} = file:read_file("test/user_def_type1.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"id">> => 1,
        <<"name">> => <<"name">>,
        <<"publication">> => <<"pub">>,
        <<"author">> => #{
            <<"id">> => 2,
            <<"name">> => <<"aname1">>,
            <<"publication">> => <<"pub">>
        },
        <<"coauthor">> => #{
            <<"id">> => 3,
            <<"name">> => <<"aname2">>,
            <<"publication">> => <<"pub">>
        }
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/user_def_type1.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).

simple2_test() ->
    {ok, SchemaJSON} = file:read_file("test/user_def_type2.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"id">> => 1,
        <<"name">> => <<"name">>,
        <<"publication">> => <<"pub">>,
        <<"author">> => #{
            <<"id">> => 2,
            <<"name">> => <<"aname1">>,
            <<"publication">> => <<"pub">>
        },
        <<"coauthor">> => #{
            <<"id">> => 3,
            <<"name">> => <<"aname2">>,
            <<"publication">> => <<"pub">>
        },
		<<"authors">> => #{
            <<"a1">> => #{
                <<"id">> => 4,
                <<"name">> => <<"aname3">>,
                <<"publication">> => <<"pub">>
            },
            <<"a2">> => #{
                <<"id">> => 5,
                <<"name">> => <<"aname4">>,
                <<"publication">> => <<"pub">>
            }
		}
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:erlav_init(<<"test/user_def_type2.avsc">>),
    Ret = erlav_nif:erlav_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).
