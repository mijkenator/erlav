-module(field_test).

-include_lib("eunit/include/eunit.hrl").
    

to_map([{_,_}|_] = L) ->
    maps:from_list([{K, to_map(V)} || {K, V} <- L]);
to_map(V) -> V.

filter_null_values([{_,_}|_] = L) -> filter_null_values(maps:from_list(L));
filter_null_values(L) when is_list(L) -> [filter_null_values(E) || E <- L];
filter_null_values(#{} = V) ->
    M1 = maps:filter(fun(_, null) -> false; (_,_) -> true end, V),
    maps:map(fun(_, Val) -> filter_null_values(Val) end, M1);
filter_null_values(V) -> V.


fields_test() ->
    ?debugFmt("=============== openRTB field test ===================  ~n ~n", []),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    ?debugFmt("=============== openRTB field test 1 :1: ===================  ~n ~n", []),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb.avsc">>),
    ?debugFmt("=============== openRTB field test 1 :2: ===================  ~n ~n", []),
    {ok, [Term1]} = file:consult("test/field_test.data"),
    ?debugFmt("=============== openRTB field test 1 :3: ===================  ~n ~n", []),
    case erlav_nif:erlav_encode(SchemaId, Term1) of
        Re2 when is_binary(Re2) ->
            M = to_map(Decoder(Re2)),
            compare_maps_debug(Term1, M);
        Error when is_integer(Error) ->
            ?debugFmt("==================== Encode error: ~p ~n", [Error]),
            ?assert(is_binary(Error));
        {error, Msg, _Code} = Erc ->
            ?debugFmt("==================== Encode error: ~p ~n", [Msg]),
            ?assert(is_binary(Erc))
    end,
    ok.

compare_maps_debug(M1, M2) ->
    KL1 = maps:keys(M1),
    lists:foreach(fun(Key) ->
        V1 = maps:get(Key, M1),
        V2 = filter_null_values(maps:get(Key, M2)),
        case maps:get(Key, M1) of
            V1 when is_map(V1) ->
                compare_maps_debug(V1, V2);
            [VV1|_] = V1 when is_map(VV1) ->
                lists:foreach(fun({X,Y})->
                    compare_maps_debug(X,Y)
                end ,lists:zip(V1, V2));
            V1 ->
                ?debugFmt("Key ~p: V1:~p  V2:~p ~n", [Key, V1, V2]),
                case is_float(V2) of
                    true ->
                        D = abs(V1-V2),
                        ?debugFmt("float diff: ~p ~n", [D]),
                        ?assert(D < 0.00001);
                    _ -> ?assertEqual(V1, V2)
                end
        end
    end, KL1),
    ok.

