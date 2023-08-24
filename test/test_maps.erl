-module(test_maps).

-include_lib("eunit/include/eunit.hrl").

m2m_compare_test() ->
    {ok, [Term1|_]} = file:consult("test/test_maps.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    ok.

term1_test() ->
    {ok, [Term1|_]} = file:consult("test/real_data1.term"),
    Map1 = erlav_nif:replace_keys(jiffy:decode(Term1, [return_maps])),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    ok.

