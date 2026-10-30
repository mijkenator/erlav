-module(tst_utils_test).

-include_lib("eunit/include/eunit.hrl").

%% normalize_erlavro/1 turns erlavro's decoded form into erlav_decode_fast's
%% (used by the decode benchmarks in erlav_perf to check both decoders agree).

records_and_maps_become_maps_test() ->
    ?assertEqual(#{<<"a">> => 1, <<"b">> => #{<<"c">> => 2}},
                 tst_utils:normalize_erlavro([{<<"a">>, 1}, {<<"b">>, [{<<"c">>, 2}]}])).

null_becomes_undefined_test() ->
    ?assertEqual(#{<<"a">> => undefined},
                 tst_utils:normalize_erlavro([{<<"a">>, null}])),
    ?assertEqual([undefined, 1], tst_utils:normalize_erlavro([null, 1])).

lists_of_records_are_normalised_element_wise_test() ->
    ?assertEqual(#{<<"xs">> => [#{<<"k">> => 1}, #{<<"k">> => 2}]},
                 tst_utils:normalize_erlavro(
                   [{<<"xs">>, [[{<<"k">>, 1}], [{<<"k">>, 2}]]}])).

plain_lists_and_scalars_are_untouched_test() ->
    ?assertEqual([1, 2, 3], tst_utils:normalize_erlavro([1, 2, 3])),
    ?assertEqual([<<"a">>, <<"b">>], tst_utils:normalize_erlavro([<<"a">>, <<"b">>])),
    ?assertEqual(<<"s">>, tst_utils:normalize_erlavro(<<"s">>)),
    ?assertEqual(1.5, tst_utils:normalize_erlavro(1.5)).

%% Documents the caveat in the function's comment: erlavro returns [] for an
%% empty map, which is indistinguishable from an empty array.
empty_map_is_not_recoverable_test() ->
    ?assertEqual([], tst_utils:normalize_erlavro([])).
