-module(erlav_ortb1_test).

-include_lib("eunit/include/eunit.hrl").
    
ortb1_test() ->
    ?debugFmt("=============== openRTB test 1 ===================  ~n ~n", []),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    ?debugFmt("=============== openRTB test 1 :1: ===================  ~n ~n", []),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    ?debugFmt("=============== openRTB test 1 :2: ===================  ~n ~n", []),
    {ok, Terms} = file:consult("test/opnrtb_test1.data"),

    lists:foreach(fun(Term1) -> 
        Re2 = erlav_nif:erlav_encode(SchemaId, Term1),
        M = to_map(Decoder(Re2)),
        compare_maps(Term1, M)
    end, Terms).

compare_maps(M1, M2) ->
    KL1 = maps:keys(M1),
    Event = maps:get(<<"event">>, M1),
    ?debugFmt("Test for event: ~p ~n", [Event]),
    lists:foreach(fun(Key) ->
        V1 = maps:get(Key, M1),
        V2 = filter_null_values(maps:get(Key, M2)),
        ?debugFmt("Key ~p: V1:~p  V2:~p ~n", [Key, V1, V2]),
        ?assertEqual(V1, V2)
    end, KL1),
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
