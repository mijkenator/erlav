-module(decode_sub_binaries_test).

%% erlav_init/2 option `decode_sub_binaries` (#74): erlav_decode_fast/2 returns
%% strings/bytes of 65+ bytes as sub-binaries of the input instead of copying
%% them. A copy and a sub-binary compare equal, so the tests tell them apart by
%% how much binary memory the process still pins after dropping the input and
%% keeping one decoded string: a copy pins its own bytes, a sub-binary pins the
%% whole input binary.

-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, <<"test/tschema_array_null_str.avsc">>).

encode(Id, Strs) ->
    erlav_nif:erlav_encode(Id, #{<<"arrayField">> => Strs}).

strings(Len, Count) ->
    [binary:copy(<<(65 + S rem 26)>>, Len) || S <- lists:seq(1, Count)].

%% Decode Bin in a fresh process, keep only the first string, GC, and report
%% {size of the kept string, bytes of binaries the process still references}.
pinned_after_keeping_one(Id, Bin) ->
    Parent = self(),
    spawn_link(fun() ->
        #{<<"arrayField">> := [Keep | _]} = erlav_nif:erlav_decode_fast(Id, Bin),
        erlang:garbage_collect(),
        {binary, Info} = process_info(self(), binary),
        Parent ! {pinned, byte_size(Keep), lists:sum([Sz || {_, Sz, _} <- Info])}
    end),
    receive {pinned, KeepSize, Pinned} -> {KeepSize, Pinned} end.

%% ---------------------------------------------------------------- default

default_copies_strings_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA),
    Bin = encode(Id, strings(136, 100)),
    ?assertEqual({136, 136}, pinned_after_keeping_one(Id, Bin)).

init_with_unrelated_options_still_copies_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [use_negative_block_count]),
    Bin = encode(Id, strings(136, 100)),
    ?assertEqual({136, 136}, pinned_after_keeping_one(Id, Bin)).

%% ----------------------------------------------------------------- opt-in

option_returns_sub_binaries_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Bin = encode(Id, strings(136, 100)),
    {136, Pinned} = pinned_after_keeping_one(Id, Bin),
    %% the process now references the whole input binary
    ?assertEqual(byte_size(Bin), Pinned).

%% Below 65 bytes the BEAM stores a heap binary: copying is cheaper than a
%% sub-binary reference, and nothing is pinned.
option_still_copies_short_strings_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Bin = encode(Id, strings(64, 100)),
    ?assertEqual({64, 0}, pinned_after_keeping_one(Id, Bin)).

option_boundary_is_65_bytes_test_() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    [{lists:flatten(io_lib:format("length ~p", [Len])), fun() ->
          Bin = encode(Id, strings(Len, 10)),
          {Len, Pinned} = pinned_after_keeping_one(Id, Bin),
          case Len >= 65 of
              true -> ?assertEqual(byte_size(Bin), Pinned);
              false -> ?assertEqual(0, Pinned)
          end
      end} || Len <- [63, 64, 65, 66]].

%% -------------------------------------------------- same data either way

values_are_identical_with_and_without_the_option_test_() ->
    Plain = erlav_nif:erlav_init(?SCHEMA),
    Opt = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    [{lists:flatten(io_lib:format("length ~p", [Len])), fun() ->
          Strs = strings(Len, 25),
          Bin = encode(Plain, Strs),
          Expected = #{<<"arrayField">> => Strs},
          ?assertEqual(Expected, erlav_nif:erlav_decode_fast(Plain, Bin)),
          ?assertEqual(Expected, erlav_nif:erlav_decode_fast(Opt, Bin))
      end} || Len <- [0, 1, 64, 65, 100, 136, 500, 5000, 100000]].

%% The options are part of the schema's identity: same file, different
%% options, different ids (otherwise the second init would silently get the
%% first one's behaviour).
options_give_distinct_schema_ids_test() ->
    A = erlav_nif:erlav_init(?SCHEMA),
    B = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    C = erlav_nif:erlav_init(?SCHEMA, [use_negative_block_count]),
    D = erlav_nif:erlav_init(?SCHEMA, [use_negative_block_count, decode_sub_binaries]),
    ?assertEqual(4, length(lists:usort([A, B, C, D]))),
    %% and repeating an init returns the same id
    ?assertEqual(B, erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries])),
    ?assertEqual(D, erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries, use_negative_block_count])).

combined_with_negative_block_count_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [use_negative_block_count, decode_sub_binaries]),
    Strs = strings(136, 40),
    Bin = encode(Id, Strs),
    ?assertEqual(#{<<"arrayField">> => Strs}, erlav_nif:erlav_decode_fast(Id, Bin)),
    {136, Pinned} = pinned_after_keeping_one(Id, Bin),
    ?assertEqual(byte_size(Bin), Pinned).

%% ------------------------------------------------- inputs that are not plain

%% The offsets passed to the BEAM are relative to the input term, so an input
%% that is itself a sub-binary (at a non-zero offset, or not byte aligned)
%% must still decode correctly.
input_that_is_a_sub_binary_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Strs = strings(136, 30),
    Bin = encode(Id, Strs),
    Big = <<(crypto:strong_rand_bytes(777))/binary, Bin/binary, (crypto:strong_rand_bytes(333))/binary>>,
    Sub = binary:part(Big, 777, byte_size(Bin)),
    ?assertEqual(#{<<"arrayField">> => Strs}, erlav_nif:erlav_decode_fast(Id, Sub)).

input_that_is_not_byte_aligned_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Strs = strings(136, 30),
    Bin = encode(Id, Strs),
    <<_:3, Rest/bitstring>> = <<0:3, Bin/binary>>,
    ?assertEqual(#{<<"arrayField">> => Strs}, erlav_nif:erlav_decode_fast(Id, Rest)).

%% Decoded strings must stay valid after the input is gone and GC has run.
results_outlive_the_input_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Collector = self(),
    Strs = [strings(100 + N, 20) || N <- lists:seq(1, 50)],
    lists:foreach(fun(S) ->
        spawn(fun() ->
            Bin = encode(Id, S),
            Collector ! {S, maps:get(<<"arrayField">>, erlav_nif:erlav_decode_fast(Id, Bin))}
        end)
    end, Strs),
    Got = [receive {S, G} -> {S, G} end || _ <- Strs],
    [erlang:garbage_collect(P) || P <- processes()],
    ?assertEqual([], [S || {S, G} <- Got, S =/= G]).

%% Malformed input still returns an error and does not leave state behind that
%% affects the next decode on the same scheduler.
truncated_input_then_good_input_test() ->
    Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
    Strs = strings(136, 30),
    Bin = encode(Id, Strs),
    Cut = binary:part(Bin, 0, byte_size(Bin) - 50),
    lists:foreach(fun(_) ->
        ?assertMatch({error, _, _}, erlav_nif:erlav_decode_fast(Id, Cut)),
        ?assertEqual(#{<<"arrayField">> => Strs}, erlav_nif:erlav_decode_fast(Id, Bin))
    end, lists:seq(1, 20)).

%% -------------------------------------------------------------- concurrency

%% Many processes decode different inputs at once through the same schema id.
%% The input term travels with each call (not in global/thread state), so
%% every result must match its own input.
concurrent_decodes_each_see_their_own_input_test_() ->
    {timeout, 120, fun() ->
        Id = erlav_nif:erlav_init(?SCHEMA, [decode_sub_binaries]),
        Parent = self(),
        Workers = 32,
        Pids = [spawn_link(fun() ->
                    Bad = lists:foldl(fun(I, Acc) ->
                        Strs = [<<"w", (integer_to_binary(W))/binary, "-", (integer_to_binary(I))/binary, "-",
                                  (integer_to_binary(S))/binary, ":",
                                  (binary:copy(<<(65 + (W + S) rem 26)>>, 90 + S rem 100))/binary>>
                                || S <- lists:seq(1, 40)],
                        Got = maps:get(<<"arrayField">>,
                                       erlav_nif:erlav_decode_fast(Id, encode(Id, Strs))),
                        Acc + case Got =:= Strs of true -> 0; false -> 1 end
                    end, 0, lists:seq(1, 200)),
                    Parent ! {self(), Bad}
                end) || W <- lists:seq(1, Workers)],
        Bad = lists:sum([receive {P, B} -> B end || P <- Pids]),
        ?assertEqual(0, Bad)
    end}.
