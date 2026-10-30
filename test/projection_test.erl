-module(projection_test).

%% Partial (selective) decode (#53, docs/design-partial-decode.md):
%% erlav_projection/2 + erlav_decode_projected/2.
%%
%% The oracle for almost everything here is the invariant from the design doc:
%%   decode_projected(P, B) =:= project(Spec, decode_fast(S, B))
%% where project/2 below selects the same fields out of the full decode.

-include_lib("eunit/include/eunit.hrl").

-define(TYPES, <<"test/projection_types.avsc">>).
-define(BLOCKS, <<"test/projection_blocks.avsc">>).
-define(OPENRTB, <<"test/opnrtb_test1.avsc">>).

%% ------------------------------------------------------------------ oracle

project(Spec, Map) ->
    maps:from_list([select(Elem, Map) || Elem <- Spec]).

select({Name, Sub}, Map) ->
    {Name, narrow(Sub, maps:get(Name, Map))};
select(Name, Map) ->
    {Name, maps:get(Name, Map)}.

narrow(_Sub, undefined) -> undefined;
narrow(Sub, List) when is_list(List) -> [project(Sub, E) || E <- List];
narrow(Sub, Map) when is_map(Map) -> project(Sub, Map).

%% ------------------------------------------------------------------- terms

%% Everything present.
term_full() ->
    #{<<"fInt">> => 7, <<"fLong">> => 1 bsl 40, <<"fFloat">> => 1.5, <<"fDouble">> => 2.25,
      <<"fBool">> => true, <<"fString">> => <<"str">>, <<"fBytes">> => <<1, 2, 3>>,
      <<"fNullStr">> => <<"ns">>, <<"fEnum">> => <<"TWO">>, <<"fFixed">> => <<9, 8, 7, 6>>,
      <<"fArrLong">> => [1, 2, 3], <<"fArrStr">> => [<<"a">>, <<"b">>],
      <<"fArrArr">> => [[1], [2, 3]],
      <<"fMapLong">> => #{<<"k">> => 5},
      <<"fMapUnion">> => #{<<"a">> => <<"x">>, <<"b">> => <<"y">>},
      <<"fMapRec">> => #{<<"r">> => #{<<"a">> => 1, <<"b">> => <<"bb">>}},
      <<"fArrRec">> => [#{<<"id">> => 1, <<"tag">> => <<"t1">>},
                        #{<<"id">> => 2, <<"tag">> => <<"t2">>, <<"note">> => <<"n">>}],
      <<"fNullRec">> => #{<<"x">> => 3,
                          <<"inner">> => #{<<"y">> => 4, <<"z">> => <<"zz">>},
                          <<"w">> => <<"ww">>},
      <<"fNullArrRec">> => [#{<<"k">> => 1, <<"v">> => <<"v1">>},
                            #{<<"k">> => 2, <<"v">> => <<"v2">>}],
      <<"fUnion">> => <<"u">>, <<"fLast">> => <<"last">>}.

%% Every nullable left out (they decode as `undefined`), arrays/maps empty.
term_nulls() ->
    #{<<"fInt">> => -1, <<"fLong">> => -(1 bsl 40), <<"fFloat">> => 0.0, <<"fDouble">> => -2.5,
      <<"fBool">> => false, <<"fString">> => <<>>, <<"fBytes">> => <<>>,
      <<"fEnum">> => <<"ONE">>, <<"fFixed">> => <<0, 0, 0, 0>>,
      <<"fArrLong">> => [], <<"fArrStr">> => [], <<"fArrArr">> => [],
      <<"fMapLong">> => #{}, <<"fMapUnion">> => #{}, <<"fMapRec">> => #{},
      <<"fArrRec">> => [],
      <<"fLast">> => <<"end">>}.

%% fNullRec present but its nested nullable record is null.
term_inner_null() ->
    (term_full())#{<<"fNullRec">> => #{<<"x">> => 9, <<"w">> => <<"w9">>}}.

terms() ->
    [{full, term_full()}, {nulls, term_nulls()}, {inner_null, term_inner_null()}].

%% The decoder fills in `undefined` for absent nullable keys.
expected(Id, T) ->
    erlav_nif:erlav_decode_fast(Id, erlav_nif:erlav_encode(Id, T)).

%% Two registrations of the same schema: plain, and negative block counts (so
%% every array/map is written with a byte-length header the skip can jump).
ids() ->
    [{positive, erlav_nif:erlav_init(?TYPES)},
     {negative, erlav_nif:erlav_init(?TYPES, [use_negative_block_count])}].

compile(Id, Spec) ->
    {ok, P} = erlav_nif:erlav_projection(Id, Spec),
    P.

check(Id, Spec, Bin) ->
    Full = erlav_nif:erlav_decode_fast(Id, Bin),
    ?assertEqual(project(Spec, Full), erlav_nif:erlav_decode_projected(compile(Id, Spec), Bin)).

top_names() ->
    maps:keys(term_full()).

%% -------------------------------------------------------- oracle: top level

every_single_field_test_() ->
    [{lists:flatten(io_lib:format("~p ~p ~s", [Wire, Kind, Name])), fun() ->
          Bin = erlav_nif:erlav_encode(Id, T),
          check(Id, [Name], Bin)
      end} || {Wire, Id} <- ids(), {Kind, T} <- terms(), Name <- top_names()].

%% Pairs cover "skip a field of every type, then decode another", in both orders.
every_pair_of_fields_test_() ->
    Names = top_names(),
    {timeout, 120,
     [{lists:flatten(io_lib:format("~p ~p", [Wire, Kind])), fun() ->
          Bin = erlav_nif:erlav_encode(Id, T),
          [check(Id, [A, B], Bin) || A <- Names, B <- Names, A < B]
      end} || {Wire, Id} <- ids(), {Kind, T} <- terms()]}.

all_fields_equals_full_decode_test_() ->
    [{lists:flatten(io_lib:format("~p ~p", [Wire, Kind])), fun() ->
          Bin = erlav_nif:erlav_encode(Id, T),
          Spec = top_names(),
          ?assertEqual(erlav_nif:erlav_decode_fast(Id, Bin),
                       erlav_nif:erlav_decode_projected(compile(Id, Spec), Bin))
      end} || {Wire, Id} <- ids(), {Kind, T} <- terms()].

empty_spec_gives_empty_map_test() ->
    [{Wire, Id}, _] = ids(),
    P = compile(Id, []),
    ?assertEqual(#{}, erlav_nif:erlav_decode_projected(P, erlav_nif:erlav_encode(Id, term_full()))),
    ?assertEqual(positive, Wire).

%% Only the last field: every earlier field of every type has to be skipped.
last_field_skips_every_type_test_() ->
    [{lists:flatten(io_lib:format("~p ~p", [Wire, Kind])), fun() ->
          Bin = erlav_nif:erlav_encode(Id, T),
          ?assertEqual(#{<<"fLast">> => maps:get(<<"fLast">>, T)},
                       erlav_nif:erlav_decode_projected(compile(Id, [<<"fLast">>]), Bin))
      end} || {Wire, Id} <- ids(), {Kind, T} <- terms()].

%% ------------------------------------------------------- oracle: nested specs

nested_specs() ->
    [[{<<"fArrRec">>, [<<"id">>]}],
     [{<<"fArrRec">>, [<<"tag">>]}],
     [{<<"fArrRec">>, [<<"note">>]}],
     [{<<"fArrRec">>, [<<"id">>, <<"note">>]}],
     [{<<"fNullRec">>, [<<"x">>]}],
     [{<<"fNullRec">>, [<<"w">>]}],
     [{<<"fNullRec">>, [{<<"inner">>, [<<"z">>]}]}],
     [{<<"fNullRec">>, [{<<"inner">>, [<<"y">>]}, <<"w">>]}],
     [{<<"fNullRec">>, [<<"x">>, {<<"inner">>, [<<"y">>, <<"z">>]}, <<"w">>]}],
     [{<<"fNullArrRec">>, [<<"k">>]}],
     [{<<"fNullArrRec">>, [<<"v">>]}],
     %% Early exit inside a nested record must still leave the cursor at the
     %% right place for whatever is selected after it:
     [{<<"fNullRec">>, [<<"x">>]}, <<"fLast">>],
     [{<<"fNullRec">>, [{<<"inner">>, [<<"y">>]}]}, <<"fLast">>],
     [{<<"fArrRec">>, [<<"id">>]}, <<"fLast">>],
     [{<<"fNullArrRec">>, [<<"k">>]}, <<"fLast">>],
     [<<"fInt">>, {<<"fArrRec">>, [<<"tag">>]}, {<<"fNullRec">>, [<<"w">>]},
      {<<"fNullArrRec">>, [<<"v">>]}, <<"fUnion">>, <<"fLast">>]].

nested_specs_match_oracle_test_() ->
    [{lists:flatten(io_lib:format("~p ~p spec ~p", [Wire, Kind, N])), fun() ->
          check(Id, Spec, erlav_nif:erlav_encode(Id, T))
      end} || {Wire, Id} <- ids(), {Kind, T} <- terms(),
              {N, Spec} <- lists:zip(lists:seq(1, length(nested_specs())), nested_specs())].

null_nested_record_is_undefined_test() ->
    [{_, Id} | _] = ids(),
    Bin = erlav_nif:erlav_encode(Id, term_nulls()),
    ?assertEqual(#{<<"fNullRec">> => undefined, <<"fNullArrRec">> => undefined},
                 erlav_nif:erlav_decode_projected(
                   compile(Id, [{<<"fNullRec">>, [<<"x">>]}, {<<"fNullArrRec">>, [<<"k">>]}]), Bin)),
    %% and a null record nested one level down
    Bin2 = erlav_nif:erlav_encode(Id, term_inner_null()),
    ?assertEqual(#{<<"fNullRec">> => #{<<"inner">> => undefined}},
                 erlav_nif:erlav_decode_projected(
                   compile(Id, [{<<"fNullRec">>, [{<<"inner">>, [<<"y">>]}]}]), Bin2)).

array_of_records_projects_every_element_test() ->
    [{_, Id} | _] = ids(),
    Bin = erlav_nif:erlav_encode(Id, term_full()),
    ?assertEqual(#{<<"fArrRec">> => [#{<<"id">> => 1}, #{<<"id">> => 2}]},
                 erlav_nif:erlav_decode_projected(compile(Id, [{<<"fArrRec">>, [<<"id">>]}]), Bin)).

%% ----------------------------------------------------------------- OpenRTB

openrtb_specs() ->
    [[<<"event">>],
     [<<"event">>, <<"timestamp">>, <<"maxtime">>],
     [{<<"bid_request">>, [<<"id">>]}],
     [{<<"bid_request">>, [<<"id">>, {<<"imp">>, [<<"id">>, {<<"banner">>, [<<"w">>, <<"h">>]}]},
                           {<<"user">>, [<<"id">>]}]},
      <<"event">>],
     [{<<"bid_request">>, [{<<"device">>, [<<"ua">>, <<"ip">>]}, {<<"site">>, [<<"id">>]}]}],
     [{<<"bid_response">>, [<<"id">>]}, <<"error">>],
     [<<"filtered_ssp_result">>, <<"filtered_app_compatible_result">>],
     [<<"sampling">>, <<"seller_request_cpm">>, {<<"error_info">>, [<<"ip">>]}]].

openrtb_oracle_test_() ->
    {ok, [Term]} = file:consult("test/opnrtb_perf.data"),
    [{lists:flatten(io_lib:format("~p spec ~p", [Wire, N])), fun() ->
          Bin = erlav_nif:erlav_encode(Id, Term),
          check(Id, Spec, Bin)
      end} || {Wire, Id} <- [{positive, erlav_nif:erlav_init(?OPENRTB)},
                             {negative, erlav_nif:erlav_init(?OPENRTB, [use_negative_block_count])}],
              {N, Spec} <- lists:zip(lists:seq(1, length(openrtb_specs())), openrtb_specs())].

%% -------------------------------------------------- hand-built wire variants

zz(N) -> (N bsl 1) bxor (N bsr 63).
varint(U) when U < 16#80 -> <<U>>;
varint(U) -> <<(16#80 bor (U band 16#7f)), (varint(U bsr 7))/binary>>.
long(N) -> varint(zz(N)).
str(S) -> <<(long(byte_size(S)))/binary, S/binary>>.

%% A positive-count block, or a negative-count block carrying its byte length.
block(pos, Items) when Items =/= [] ->
    <<(long(length(Items)))/binary, (iolist_to_binary(Items))/binary>>;
block(neg, Items) when Items =/= [] ->
    Body = iolist_to_binary(Items),
    <<(long(-length(Items)))/binary, (long(byte_size(Body)))/binary, Body/binary>>.

blocks_record(Style, ArrBlocks, MapBlocks, SArrBlocks) ->
    iolist_to_binary(
      [long(5),
       [block(Style, B) || B <- ArrBlocks], long(0),
       [block(Style, B) || B <- MapBlocks], long(0),
       [block(Style, B) || B <- SArrBlocks], long(0),
       long(99)]).

blocks_wire_test_() ->
    Id = erlav_nif:erlav_init(?BLOCKS),
    Arr = [[long(1), long(2)], [long(3)], [long(-4), long(5), long(6)]],
    Map = [[[str(<<"a">>), str(<<"A">>)]], [[str(<<"b">>), str(<<"B">>)], [str(<<"c">>), str(<<"C">>)]]],
    SArr = [[str(<<"x">>), str(<<>>)], [str(binary:copy(<<"y">>, 300))]],
    Specs = [[<<"tail">>], [<<"head">>, <<"tail">>], [<<"arr">>, <<"tail">>], [<<"m">>, <<"tail">>],
             [<<"sarr">>, <<"tail">>], [<<"head">>, <<"sarr">>], [<<"arr">>, <<"m">>, <<"sarr">>]],
    [{lists:flatten(io_lib:format("~p blocks spec ~p", [Style, Spec])), fun() ->
          check(Id, Spec, blocks_record(Style, Blocks_arr, Blocks_map, Blocks_sarr))
      end} || Style <- [pos, neg],
              {Blocks_arr, Blocks_map, Blocks_sarr} <-
                  [{Arr, Map, SArr},   %% several blocks each
                   {[], [], []}],      %% empty arrays/maps
              Spec <- Specs].

%% ------------------------------------------------------------- spec errors

spec_validation_test_() ->
    [{_, Id} | _] = ids(),
    E = fun(Spec) -> erlav_nif:erlav_projection(Id, Spec) end,
    [?_assertMatch({error, _, 112}, E([<<"nope">>])),
     ?_assertMatch({error, _, 112}, E([{<<"fNullRec">>, [<<"nope">>]}])),
     ?_assertMatch({error, _, 113}, E([<<"fInt">>, <<"fInt">>])),
     ?_assertMatch({error, _, 113}, E([<<"fInt">>, {<<"fInt">>, [<<"x">>]}])),
     ?_assertMatch({error, _, 115}, E([{<<"fInt">>, [<<"x">>]}])),        %% sub-spec on a scalar
     ?_assertMatch({error, _, 115}, E([{<<"fMapRec">>, [<<"a">>]}])),     %% ... on a map
     ?_assertMatch({error, _, 115}, E([{<<"fUnion">>, [<<"a">>]}])),      %% ... on a multi-member union
     ?_assertMatch({error, _, 115}, E([{<<"fArrLong">>, [<<"a">>]}])),    %% ... on an array of scalars
     ?_assertMatch({error, _, 115}, E([{<<"fArrArr">>, [<<"a">>]}])),     %% ... on an array of arrays
     ?_assertMatch({error, _, 116}, E([{<<"fNullRec">>, []}])),
     ?_assertMatch({error, _, 111}, E([42])),
     ?_assertMatch({error, _, 111}, E([{<<"fInt">>}])),
     ?_assertMatch({error, _, 111}, E([{<<"fInt">>, [], extra}])),
     ?_assertMatch({error, _, 110}, E(not_a_list)),
     ?_assertMatch({error, _, 110}, E([<<"fInt">> | <<"improper">>])),
     ?_assertMatch({error, _, 110}, E([{<<"fNullRec">>, not_a_list}]))].

bad_arguments_test_() ->
    [{_, Id} | _] = ids(),
    P = compile(Id, [<<"fInt">>]),
    Bin = erlav_nif:erlav_encode(Id, term_full()),
    [?_assertError(badarg, erlav_nif:erlav_projection(999999, [<<"fInt">>])),
     ?_assertError(badarg, erlav_nif:erlav_projection(not_an_id, [<<"fInt">>])),
     ?_assertError(badarg, erlav_nif:erlav_decode_projected(999999, Bin)),
     ?_assertError(badarg, erlav_nif:erlav_decode_projected(not_an_id, Bin)),
     ?_assertError(badarg, erlav_nif:erlav_decode_projected(P, not_a_binary)),
     %% a schema id is not a projection id
     ?_assertError(badarg, erlav_nif:erlav_decode_projected(Id + 1000, Bin))].

%% -------------------------------------------------------------- registry

same_spec_same_id_test() ->
    [{_, Id} | _] = ids(),
    A = compile(Id, [<<"fInt">>, {<<"fNullRec">>, [<<"x">>]}]),
    ?assertEqual(A, compile(Id, [<<"fInt">>, {<<"fNullRec">>, [<<"x">>]}])),
    %% the resolved selection is what counts, not the order it was written in
    ?assertEqual(A, compile(Id, [{<<"fNullRec">>, [<<"x">>]}, <<"fInt">>])),
    ?assertNotEqual(A, compile(Id, [<<"fInt">>])),
    ?assertNotEqual(A, compile(Id, [<<"fInt">>, {<<"fNullRec">>, [<<"w">>]}])),
    ?assertNotEqual(A, compile(Id, [<<"fInt">>, <<"fNullRec">>])).

projections_are_per_schema_test() ->
    [{_, Pos}, {_, Neg}] = ids(),
    ?assertNotEqual(compile(Pos, [<<"fInt">>]), compile(Neg, [<<"fInt">>])).

%% --------------------------------------------- documented semantics (4.6)

%% Early exit: bytes after the last selected field are not validated, so a
%% projected decode succeeds on a truncated tail where a full decode errors.
unvalidated_tail_test() ->
    [{_, Id} | _] = ids(),
    Bin = erlav_nif:erlav_encode(Id, term_full()),
    Cut = binary:part(Bin, 0, 1), %% just fInt (7 -> one byte)
    ?assertMatch({error, _, _}, erlav_nif:erlav_decode_fast(Id, Cut)),
    ?assertEqual(#{<<"fInt">> => 7},
                 erlav_nif:erlav_decode_projected(compile(Id, [<<"fInt">>]), Cut)).

%% ... but everything up to and including the last selected field is checked.
selected_field_must_be_complete_test() ->
    [{_, Id} | _] = ids(),
    Bin = erlav_nif:erlav_encode(Id, term_full()),
    P = compile(Id, [<<"fString">>]),
    %% fInt, fLong, fFloat, fDouble, fBool precede fString; cut inside it
    Prefix = 1 + 6 + 4 + 8 + 1,
    ?assertMatch({error, _, _},
                 erlav_nif:erlav_decode_projected(P, binary:part(Bin, 0, Prefix + 1))),
    ?assertMatch(#{<<"fString">> := <<"str">>},
                 erlav_nif:erlav_decode_projected(P, binary:part(Bin, 0, Prefix + 4))).

%% The projected decode always copies strings, even for a schema initialised
%% with decode_sub_binaries: keeping one field must not pin the whole input.
projected_strings_are_copied_test() ->
    Id = erlav_nif:erlav_init(<<"test/tschema_array_null_str.avsc">>, [decode_sub_binaries]),
    Strs = [binary:copy(<<(65 + S rem 26)>>, 136) || S <- lists:seq(1, 100)],
    Bin = erlav_nif:erlav_encode(Id, #{<<"arrayField">> => Strs}),
    P = compile(Id, [<<"arrayField">>]),
    Parent = self(),
    spawn_link(fun() ->
        #{<<"arrayField">> := [Keep | _]} = erlav_nif:erlav_decode_projected(P, Bin),
        erlang:garbage_collect(),
        {binary, Info} = process_info(self(), binary),
        Parent ! {pinned, byte_size(Keep), lists:sum([Sz || {_, Sz, _} <- Info])}
    end),
    receive {pinned, KeepSize, Pinned} -> ?assertEqual({136, 136}, {KeepSize, Pinned}) end.

%% ----------------------------------------------------------- malformed input

%% A negative-count block whose byte length cannot be right must not be jumped.
bad_block_byte_length_test_() ->
    Id = erlav_nif:erlav_init(?BLOCKS),
    P = compile(Id, [<<"tail">>]),
    Head = long(5),
    [?_assertMatch({error, _, _},
                   erlav_nif:erlav_decode_projected(
                     P, <<Head/binary, (long(-2))/binary, (long(1000))/binary, (long(1))/binary,
                          (long(2))/binary, (long(0))/binary>>)),
     ?_assertMatch({error, _, _},
                   erlav_nif:erlav_decode_projected(
                     P, <<Head/binary, (long(-2))/binary, (long(-5))/binary, (long(1))/binary,
                          (long(2))/binary, (long(0))/binary>>)),
     ?_assertMatch({error, _, _},
                   erlav_nif:erlav_decode_projected(P, <<Head/binary, (long(-2))/binary>>)),
     ?_assertMatch({error, _, _}, erlav_nif:erlav_decode_projected(P, <<>>))].

%% Every strict prefix of a message whose last field is selected must fail.
every_truncation_of_last_field_fails_test_() ->
    {timeout, 60, fun() ->
        [{_, Id}, {_, Neg}] = ids(),
        [begin
             Bin = erlav_nif:erlav_encode(I, term_full()),
             P = compile(I, [<<"fLast">>]),
             [?assertMatch({error, _, _},
                           erlav_nif:erlav_decode_projected(P, binary:part(Bin, 0, N)))
              || N <- lists:seq(0, byte_size(Bin) - 1)],
             ?assertEqual(#{<<"fLast">> => <<"last">>}, erlav_nif:erlav_decode_projected(P, Bin))
         end || I <- [Id, Neg]]
    end}.

%% Truncation at every offset, and single byte flips, with a spread of specs:
%% the answer is a map or an {error,_,_}, never a crash or a hang.
truncation_and_corruption_never_crash_test_() ->
    {timeout, 180, fun() ->
        [{_, Id}, {_, Neg}] = ids(),
        Specs = [[<<"fLast">>], top_names(),
                 [{<<"fArrRec">>, [<<"tag">>]}, {<<"fNullRec">>, [{<<"inner">>, [<<"z">>]}]}, <<"fLast">>],
                 [{<<"fNullArrRec">>, [<<"v">>]}, <<"fMapUnion">>]],
        Ok = fun(R) -> is_map(R) orelse (is_tuple(R) andalso element(1, R) =:= error) end,
        [begin
             Bin = erlav_nif:erlav_encode(I, term_full()),
             P = compile(I, Spec),
             [?assert(Ok(erlav_nif:erlav_decode_projected(P, binary:part(Bin, 0, N))))
              || N <- lists:seq(0, byte_size(Bin))],
             [?assert(Ok(erlav_nif:erlav_decode_projected(P, flip(Bin, Off, Mask))))
              || Off <- lists:seq(0, byte_size(Bin) - 1), Mask <- [16#01, 16#80, 16#ff]]
         end || I <- [Id, Neg], Spec <- Specs]
    end}.

flip(Bin, Off, Mask) ->
    <<Pre:Off/binary, B, Post/binary>> = Bin,
    <<Pre/binary, (B bxor Mask), Post/binary>>.

%% --------------------------------------------------------------- concurrency

%% Many processes decode through the same ProjId while others keep registering
%% new projections (the registry's rare write path).
concurrent_decode_and_compile_test_() ->
    {timeout, 120, fun() ->
        [{_, Id} | _] = ids(),
        Names = lists:sort(top_names()),
        Spec = [{<<"fNullRec">>, [<<"w">>]}, <<"fLast">>, {<<"fArrRec">>, [<<"id">>]}],
        P = compile(Id, Spec),
        Parent = self(),
        Decoders = [spawn_link(fun() ->
            Terms = [T#{<<"fLast">> => <<"w", (integer_to_binary(W))/binary>>}
                     || {_, T} <- terms()],
            Bad = lists:foldl(fun(_, Acc) ->
                Acc + lists:sum([begin
                    Bin = erlav_nif:erlav_encode(Id, T),
                    case erlav_nif:erlav_decode_projected(P, Bin) =:=
                         project(Spec, erlav_nif:erlav_decode_fast(Id, Bin)) of
                        true -> 0;
                        false -> 1
                    end
                end || T <- Terms])
            end, 0, lists:seq(1, 200)),
            Parent ! {self(), Bad}
        end) || W <- lists:seq(1, 16)],
        Compilers = [spawn_link(fun() ->
            [begin
                 Sel = [N || {N, I} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
                             (Mask bsr (I rem 10)) band 1 =:= 1],
                 case Sel of
                     [] -> ok;
                     _ -> {ok, _} = erlav_nif:erlav_projection(Id, Sel)
                 end
             end || Mask <- lists:seq(C, 1023, 4)],
            Parent ! {self(), 0}
        end) || C <- [1, 2, 3, 4]],
        Bad = lists:sum([receive {Pid, B} -> B end || Pid <- Decoders ++ Compilers]),
        ?assertEqual(0, Bad)
    end}.
