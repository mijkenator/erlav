-module(tst_utils).

-export([
    compare_maps/2,
    compare_maps_deep/2,
    to_map/1,
    normalize_erlavro/1,
    filter_null_values/1,
    compare_maps_extra_fields/2
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

compare_maps_deep(M1, M2) ->
    KL1 = maps:keys(M1),
    MR = lists:map(fun(Key) ->
        V1 = maps:get(Key, M1),
        V2 = filter_null_values(maps:get(Key, M2)),
        io:format("~p ~p ~p ~n", [Key, V1, V2]),
        compare_deep(V1, V2)
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

compare_deep([], []) -> true;
compare_deep([], _) -> false;
compare_deep(_, []) -> false;
compare_deep([H1|T1], [H2|T2]) ->
    case compare_deep(H1, H2) of
        true -> compare_deep(T1, T2);
        _ -> false
    end;
compare_deep(L1, L2) when is_map(L1), is_map(L2) ->
    compare_maps_deep(L1, L2);
compare_deep(F1, F2) when is_float(F1), is_float(F2) ->
    abs(F1-F2) < 0.0001;
compare_deep(V1, V2) -> V1 =:= V2.

%
% M1 - source map
% M2 - map after decoding
% M1 can contain fields not existing in schema
%
compare_maps_extra_fields(M1, M2) ->
    KL1 = maps:keys(M1),
    MR = lists:map(fun(Key) ->
        V1 = maps:get(Key, M1),
        V2 = filter_null_values(maps:get(Key, M2, notexists)),
        case V2 of
            notexists -> true;
            _ ->
                    io:format("~p ~p ~p ~n", [Key, V1, V2]),
                    case is_float(V2) of
                        true ->
                            round(V1) =:= round(V2)
                        ;_ -> V1 =:= V2
                    end
        end
    end, KL1),
    io:format("================================ ~n ~p ~n", [MR]),
    lists:all(fun(true) -> true; (_) -> false end, MR).

%
% Turns erlavro's decoded form into the form erlav_nif:erlav_decode_fast/2
% returns, so the two can be compared with =:= :
%   * a record / map is a proplist [{Key, Value}] in erlavro, a map here
%   * an Avro null is `null` in erlavro, `undefined` here
% Caveat: erlavro returns [] for an EMPTY map or record, which is
% indistinguishable from an empty array, so an empty map does not normalise
% to #{}. Keep empty maps out of terms compared through this function.
%
normalize_erlavro([{_, _} | _] = L) ->
    maps:from_list([{K, normalize_erlavro(V)} || {K, V} <- L]);
normalize_erlavro(L) when is_list(L) -> [normalize_erlavro(E) || E <- L];
normalize_erlavro(null) -> undefined;
normalize_erlavro(V) -> V.
