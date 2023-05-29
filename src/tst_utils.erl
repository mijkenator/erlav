-module(tst_utils).

-export([
    compare_maps/2,
    to_map/1,
    filter_null_values/1
]).

compare_maps(M1, M2) ->
    KL1 = maps:keys(M1),
    MR = lists:map(fun(Key) ->
        V1 = maps:get(Key, M1),
        V2 = filter_null_values(maps:get(Key, M2)),
        io:format("~p ~p ~p ~n", [Key, V1, V2]),
        V1 =:= V2
    end, KL1),
    lists:all(fun(true) -> true; (_) -> false end, MR).

to_map([{_,_}|_] = L) ->
    maps:from_list([{K, to_map(V)} || {K, V} <- L]);
to_map(V) -> V.

filter_null_values([{_,_}|_] = L) -> filter_null_values(maps:from_list(L));
filter_null_values(L) when is_list(L) -> [filter_null_values(E) || E <- L];
filter_null_values(#{} = V) ->
    M1 = maps:filter(fun(_, null) -> false; (_,_) -> true end, V),
    maps:map(fun(_, Val) -> filter_null_values(Val) end, M1);
filter_null_values(V) -> V.

