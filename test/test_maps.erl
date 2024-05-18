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
    %M = erlav_nif:erlav_decode_fast(SchemaId, Re1),
    %tst_utils:compare_maps(Term1, M),
    ok.

term2_test() ->
    {ok, [Term1|_]} = file:consult("test/tst.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result: ~p ~n", [M]),
    ok.

term5_test() ->
    {ok, [Term1|_]} = file:consult("test/tst5.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result: ~p ~n", [M]),
    ok.

term3_test() ->
    {ok, [Term1|_]} = file:consult("test/tst1.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result: ~p ~n", [M]),
    ok.

term4_test() ->
    {ok, [Term1|_]} = file:consult("test/tst1.term"),
    {ok, [Term2|_]} = file:consult("test/tst2.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("Term: ~p ~n", [Term2]),
    ?debugFmt("Map: ~p ~n", [Map1]),
    ?assert(Map1 == Term2).

term6_test() ->
    {ok, [Term1|_]} = file:consult("test/tst6.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/gf.avsc">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/gf.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result: ~p ~n", [M]),
    ok.

term7_test() ->
    {ok, [Term1|_]} = file:consult("test/tst7.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_arr.avs">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/tschema_arr.avs"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result::::: ~p ~n", [M]),
    CRet = tst_utils:compare_maps_extra_fields(Map1, M),
    ?assertEqual(CRet, true),
    ok.

term8_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_arr.avs"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    ?debugFmt("====== 8 started ==========  ~n", []),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_arr.avs">>),
    ?debugFmt("====== 8 inited  ==========  ~n", []),
    {ok, Terms} = file:consult("test/fails.term"),
    ?debugFmt("====== 8 data loaded  ==========  ~n", []),

    F = fun(Term1) ->
        ?debugFmt("============================================ ~n", []),
        Map1 = erlav_nif:replace_keys(Term1),
        ?debugFmt("New map: ~p ~n", [Map1]),
        Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
        ?debugFmt("Encde result: ~p ~n", [Re1]),
        M = to_map(Decoder(Re1)),
        ?debugFmt("Decoded result::::: ~p ~n", [M]),
        CRet = tst_utils:compare_maps_extra_fields(Map1, M),
        ?assertEqual(CRet, true)
    end,
    lists:foreach(F, Terms),
    ok.

term9_test() ->
    {ok, [Term1|_]} = file:consult("test/tst9.term"),
    Map1 = erlav_nif:replace_keys(Term1),
    ?debugFmt("New map: ~p ~n", [Map1]),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_arr.avs">>),
    Re1 = erlav_nif:erlav_safe_encode(SchemaId, Map1),
    ?debugFmt("Encde result: ~p ~n", [Re1]),
    {ok, SchemaJSON1} = file:read_file("test/tschema_arr.avs"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    M = to_map(Decoder(Re1)),
    ?debugFmt("Decoded result::::: ~p ~n", [M]),
    CRet = tst_utils:compare_maps_extra_fields(Map1, M),
    ?assertEqual(CRet, true),
    ok.

to_map([{_,_}|_] = L) ->
    maps:from_list([{K, to_map(V)} || {K, V} <- L]);
to_map(V) -> V.

filter_null_values([{_,_}|_] = L) -> filter_null_values(maps:from_list(L));
filter_null_values(L) when is_list(L) -> [filter_null_values(E) || E <- L];
filter_null_values(#{} = V) ->
    M1 = maps:filter(fun(_, null) -> false; (_,_) -> true end, V),
    maps:map(fun(_, Val) -> filter_null_values(Val) end, M1);
filter_null_values(V) -> V.
