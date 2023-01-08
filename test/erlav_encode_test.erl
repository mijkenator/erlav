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
    ok.

perf_test() ->
    {ok, SchemaJSON} = file:read_file("priv/perf_schema.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true,
        <<"stringField">> => <<"asdadasdasdasd3453534dfgdgd123456789">>,
        <<"stringField1">> => <<"1f9a8371-659e-4efb-95c3-d6bda20fd000">>,
        <<"stringField2">> => <<"02c52125-d49d-4df2-a6c1-fa49a0286694">>,
        <<"stringField3">> => <<"62f267fb-276c-4452-8967-406d732cb621">>,
        <<"stringField4">> => <<"316693e0-253c-485e-b42a-f091a49993de">>,
        <<"stringField5">> => <<"483f4ebc-dcba-46f2-bc85-4f42ecb357bf">>,
        <<"stringField6">> => <<"0d177b44-c867-4b2e-a4b9-5866aff23720">>,
        <<"stringField7">> => <<"1869ab3c-8949-477f-804f-221722e39304">>,
        <<"stringField8">> => <<"05c38ce8-3573-46d8-9d1c-3ca4a8abc451">>,
        <<"stringField9">> => <<"31d55319-16f7-4bfe-b1b0-973b258eb758">>,
        <<"stringField10">> => <<"be9add20-cc6b-480c-aaf4-0a59081d40fd">>
    },
    R1 = iolist_to_binary(Encoder(Term)),
    io:format("R1: ~p ~n", [R1]),
    SchemaId = erlav_nif:create_encoder(<<"priv/perf_schema.avsc">>),
    Ret = erlav_nif:do_encode(SchemaId, Term),
    io:format("c++ ret: ~p ~n", [Ret]),
    ?assertEqual(R1, Ret).



