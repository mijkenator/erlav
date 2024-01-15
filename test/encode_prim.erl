-module(encode_prim).

-include_lib("eunit/include/eunit.hrl").


integer_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/integer.avsc">>),
    Term = #{
        <<"intField1">> => 111,
        <<"intField2">> => 2222222,
        <<"intField3">> => 33333,
        <<"intField4">> => 4,
        <<"intField5">> => 555555555,
        <<"intField6">> => 6666
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    Re1 = erlav_nif:erlav_decode(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ok.
