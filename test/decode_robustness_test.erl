%% Decoder robustness: malformed / truncated input, and erlav_decode/2
%% (the legacy iterator-based decoder) behaviour.
%%
%% Malformed input used to be able to crash the whole VM (#30 truncated /
%% overlong input, #38 union branch index, #40 block counts; all fixed), which
%% would take the eunit run down with it. So every
%% malformed-input decode runs in a disposable `peer` node; the test sees one
%% of:
%%
%%   {ok, Term}       the NIF returned (Term may be {error, Msg, Code} or garbage)
%%   {raised, Reason} the NIF raised an Erlang exception
%%   vm_died          the peer VM went away (segfault, abort, binary_alloc ...)
%%   timeout          the call hung and the peer was killed
%%
%% The contract these tests check is: malformed input yields
%% {error, Msg, Code} and never crashes or hangs the VM.
%%
%% Tests that pass today always run. Tests that exercise a still-open bug
%% are opt-in so they do not turn the suite red:
%%
%%     ERLAV_KNOWN_BUGS=1 rebar3 eunit --module=decode_robustness_test
%%
%% When the referenced issue is fixed, move its case from known_bug_cases/0
%% to the always-on tests and delete it from the list.
-module(decode_robustness_test).

-include_lib("eunit/include/eunit.hrl").

-define(PEER_KEY, {?MODULE, peer}).
-define(CALL_TIMEOUT, 4000).
%% eunit's default per-test timeout is 5s, too short for tests that may need
%% to start a peer (~0.7s each) or wait out a hung call.
-define(TEST_TIMEOUT, 120).

%% ---------------------------------------------------------------- harness

ensure_peer() ->
    case persistent_term:get(?PEER_KEY, undefined) of
        Peer when is_pid(Peer) ->
            case is_process_alive(Peer) of
                true -> Peer;
                false -> start_peer()
            end;
        undefined ->
            start_peer()
    end.

start_peer() ->
    Args = lists:append([["-pa", P] || P <- code:get_path()]),
    %% ERL_CRASH_DUMP_SECONDS=0: no erl_crash.dump in the project root when
    %% the peer aborts on purpose.
    %%
    %% connection => 0 (TCP), not standard_io: erlav_init currently prints
    %% "ENUM" to stdout for enum schemas (schema_item.hh), which would be
    %% read as garbage on peer's stdio control channel and kill the peer.
    {ok, Peer, _Node} = peer:start(#{
        name => peer:random_name(),
        args => Args,
        connection => 0,
        env => [{"ERL_CRASH_DUMP_SECONDS", "0"}]
    }),
    persistent_term:put(?PEER_KEY, Peer),
    Peer.

stop_peer() ->
    case persistent_term:get(?PEER_KEY, undefined) of
        Peer when is_pid(Peer) ->
            catch peer:stop(Peer),
            persistent_term:erase(?PEER_KEY);
        undefined ->
            ok
    end.

kill_peer(Peer) ->
    catch peer:stop(Peer),
    persistent_term:erase(?PEER_KEY),
    ok.

%% Run M:F(A) in the peer. Never crashes or hangs the caller.
call(M, F, A) ->
    Peer = ensure_peer(),
    try
        {ok, peer:call(Peer, M, F, A, ?CALL_TIMEOUT)}
    catch
        exit:{timeout, _} ->
            kill_peer(Peer),
            timeout;
        exit:_ ->
            kill_peer(Peer),
            vm_died;
        error:Reason ->
            {raised, Reason}
    end.

path(File) -> list_to_binary(filename:join("test", File)).

%% Decode Bin with the named NIF in the peer (schema is (re)initialised each
%% time, since a restarted peer has an empty schema registry).
decode(Nif, File, Bin) ->
    case call(erlav_nif, erlav_init, [path(File)]) of
        {ok, Id} when is_integer(Id) -> call(erlav_nif, Nif, [Id, Bin]);
        Other -> {init_failed, Other}
    end.

decode_fast(File, Bin) -> decode(erlav_decode_fast, File, Bin).

%% Safe (local VM) helpers for building *valid* input.
encode(File, Term) ->
    Id = erlav_nif:erlav_init(path(File)),
    erlav_nif:erlav_encode(Id, Term).

%% What a robust decoder must do with malformed input.
-define(assertRejected(Outcome), ?assertMatch({ok, {error, _, _}}, Outcome)).

%% A few zero bytes after the malformed field, so a failure is attributable
%% to the malformed value itself and not to simply running out of input.
-define(PAD, <<0:128>>).

fixture_all() ->
    {"robust_all.avsc", #{
        <<"l">> => 300,
        <<"i">> => -5,
        <<"s">> => <<"hello">>,
        <<"b">> => <<1, 2, 3>>,
        <<"d">> => 2.5,
        <<"f">> => 1.5,
        <<"t">> => true,
        <<"n">> => 7,
        <<"al">> => [1, 2, 3],
        <<"as">> => [<<"x">>, <<"yy">>],
        <<"m">> => #{<<"k">> => 1, <<"j">> => 2},
        <<"r">> => #{<<"x">> => 9, <<"y">> => <<"z">>},
        <<"e">> => <<"TWO">>,
        <<"u">> => <<"str">>
    }}.

fixture_array_union() ->
    {"robust_array_union.avsc", #{
        <<"a">> => [<<"a">>, undefined, <<"bcd">>]
    }}.

%% ---------------------------------------------------- always-on: the harness

harness_reports_vm_death_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertEqual(vm_died, call(erlang, halt, [1])),
        %% and recovers with a fresh peer
        ?assertEqual({ok, 3}, call(erlang, '+', [1, 2]))
    end}.

harness_reports_timeout_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertEqual(timeout, call(timer, sleep, [?CALL_TIMEOUT + 5000])),
        ?assertEqual({ok, 3}, call(erlang, '+', [1, 2]))
    end}.

harness_reports_raised_exception_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertEqual({raised, badarg}, call(erlang, list_to_atom, [1]))
    end}.

%% ------------------------------------------ always-on: oracle for the tests

%% The fixtures decode correctly when they are well-formed (so a failure of a
%% malformed-input test is about the malformation, not the fixture).
valid_fixtures_decode_in_peer_test_() ->
    [
        {File, {timeout, ?TEST_TIMEOUT, fun() ->
            ?assertEqual({ok, Term}, decode_fast(File, encode(File, Term)))
        end}}
     || {File, Term} <- [fixture_all(), fixture_array_union()]
    ].

%% The truncation tests below expect *every* proper prefix of a valid
%% encoding to be rejected. Confirm that expectation against an independent
%% implementation (erlavro) rather than assuming it: if erlavro accepted
%% some prefix, "decoder must error" would be wrong for that offset.
erlavro_rejects_every_proper_prefix_test_() ->
    [
        {File, fun() ->
            {ok, Json} = file:read_file(filename:join("test", File)),
            Dec = avro:make_simple_decoder(Json, []),
            Bin = encode(File, Term),
            ?assert(is_list(Dec(Bin))),
            Accepted = [
                N
             || N <- lists:seq(0, byte_size(Bin) - 1),
                not is_error(catch Dec(binary:part(Bin, 0, N)))
            ],
            ?assertEqual([], Accepted)
        end}
     || {File, Term} <- [fixture_all(), fixture_array_union()]
    ].

is_error({'EXIT', _}) -> true;
is_error({error, _}) -> true;
is_error(_) -> false.

%% ----------------------------- always-on: malformed input handled correctly

%% 10+ continuation bytes: not a valid 64-bit varint.
varint_overflow_long_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Bin = <<(binary:copy(<<255>>, 12))/binary, 1>>,
        ?assertRejected(decode_fast("robust_scalars.avsc", Bin))
    end}.

%% Same, for the 32-bit varint used for union branch indexes.
varint_overflow_union_index_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Bin = <<(binary:copy(<<255>>, 7))/binary, 1>>,
        ?assertRejected(decode_fast("robust_union.avsc", Bin))
    end}.

%% Same, where the block count of an array is the overflowing varint.
varint_overflow_block_count_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Bin = <<(binary:copy(<<255>>, 12))/binary, 1>>,
        ?assertRejected(decode_fast("robust_array.avsc", Bin))
    end}.

enum_index_out_of_range_test_() ->
    %% zigzag(99) = 198 -> <<198,1>>; zigzag(-1) = 1 -> <<1>>
    [
        {Name, {timeout, ?TEST_TIMEOUT, fun() ->
            ?assertRejected(decode_fast("enum.avsc", Bin))
        end}}
     || {Name, Bin} <- [
            {"index 99", <<198, 1, ?PAD/binary>>},
            {"negative index", <<1, ?PAD/binary>>}
        ]
    ].

%% Array whose items are a union, with a branch index that names no member.
array_union_branch_index_out_of_range_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% block count 1, branch index 9 (zigzag 18)
        Bin = <<2, 18, ?PAD/binary>>,
        ?assertRejected(decode_fast("robust_array_union.avsc", Bin))
    end}.

%% ----------------------- always-on: truncated / overlong input (#30)
%%
%% The decoder checks every read against the end of the input and returns
%% {error, Msg, Code} instead of reading past it (before: garbage terms, a
%% VM abort on a huge allocation, or an out-of-bounds read).

empty_input_test_() ->
    [
        {File, {timeout, ?TEST_TIMEOUT, fun() ->
            ?assertRejected(decode_fast(File, <<>>))
        end}}
     || File <- ["robust_scalars.avsc", "robust_string.avsc"]
    ].

truncation_at_every_offset_test_() ->
    [
        {File, {timeout, ?TEST_TIMEOUT, fun() -> truncation_sweep(Fixture) end}}
     || {File, _} = Fixture <- [fixture_all(), fixture_array_union()]
    ].

negative_string_length_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% zigzag(-2) = 3
        ?assertRejected(decode_fast("robust_string.avsc", <<3, ?PAD/binary>>))
    end}.

%% Length far past the buffer: used to hang or abort on the allocation.
string_length_past_end_of_buffer_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Len = <<128, 128, 128, 128, 128, 64>>,
        ?assertRejected(
            decode_fast("robust_string.avsc", <<Len/binary, ?PAD/binary>>)
        )
    end}.

%% Off by one: claims 5 bytes but only 4 remain / exactly 5 remain (valid).
string_length_exactly_at_the_end_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertEqual(
            {ok, #{<<"s">> => <<"abcde">>}},
            decode_fast("robust_string.avsc", <<10, "abcde">>)
        ),
        ?assertRejected(decode_fast("robust_string.avsc", <<10, "abcd">>))
    end}.

%% Fixed-width values: one byte short is rejected, the exact width is not.
truncated_float_and_double_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% robust_scalars is l:long i:int d:double f:float. With l = i = 1
        %% (one byte each) the layout is: l(1) i(1) d(8) f(4) = 14 bytes.
        Full = encode("robust_scalars.avsc",
                      #{<<"l">> => 1, <<"i">> => 1, <<"d">> => 2.5, <<"f">> => 1.5}),
        ?assertEqual(14, byte_size(Full)),
        ?assertMatch({ok, #{<<"d">> := 2.5, <<"f">> := 1.5}},
                     decode_fast("robust_scalars.avsc", Full)),
        %% one byte short of the float, then one byte short of the double
        ?assertRejected(decode_fast("robust_scalars.avsc", binary:part(Full, 0, 13))),
        ?assertRejected(decode_fast("robust_scalars.avsc", binary:part(Full, 0, 9)))
    end}.

truncated_boolean_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertEqual({ok, #{<<"v">> => true}},
                     decode_fast("robust_boolean.avsc", <<1>>)),
        ?assertRejected(decode_fast("robust_boolean.avsc", <<>>))
    end}.

truncated_fixed_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Term = #{<<"beforeField">> => 1, <<"fixedField">> => binary:copy(<<7>>, 16),
                 <<"afterField">> => 2},
        Full = encode("fixed_type.avsc", Term),
        ?assertEqual({ok, Term}, decode_fast("fixed_type.avsc", Full)),
        %% cut inside the 16 fixed bytes
        ?assertRejected(decode_fast("fixed_type.avsc", binary:part(Full, 0, 10)))
    end}.

%% A block-count of INT64_MIN cannot be negated; used to be undefined behaviour.
%% (The array case is also exercised, nondeterministically, by #40's cases.)
negative_block_count_with_int64_min_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% zigzag(INT64_MIN) = 2^64 - 1, followed by a block byte length
        Count = <<255, 255, 255, 255, 255, 255, 255, 255, 255, 1>>,
        ?assertRejected(decode_fast("robust_array.avsc", <<Count/binary, 0>>))
    end}.

%% int_decode/1 walks a bare varint stream: a trailing truncated varint must
%% raise badarg, not read past the end or throw a C++ exception out of the NIF.
int_decode_truncated_varint_test() ->
    ?assertEqual([1, 300], erlav_nif:int_decode(<<1, 172, 2>>)),
    ?assertError(badarg, erlav_nif:int_decode(<<1, 172>>)),
    ?assertError(badarg, erlav_nif:int_decode(<<128>>)).

%% --------------------- always-on: union branch index out of range (#38)
%%
%% A union branch index names a member of the union. It used to index the
%% schema's member vector with no range check (over-read / VM death), and the
%% array-of-union lookup inserted an empty entry into the shared, cached
%% schema for every unknown index. Valid indexes: ["null","long","string"]
%% -> 0..2; ["long","string"] -> 0..1; array<["null","string"]> -> 0..1.

zz(N) when N >= 0 -> N bsl 1;
zz(N) -> ((-N) bsl 1) - 1.

varint(N) when N < 128 -> <<N>>;
varint(N) -> <<1:1, (N band 127):7, (varint(N bsr 7))/binary>>.

idx(N) -> varint(zz(N)).

union_branch_index_out_of_range_test_() ->
    [
        {File ++ " index " ++ integer_to_list(N),
         {timeout, ?TEST_TIMEOUT, fun() ->
              ?assertRejected(
                  decode_fast(File, <<(idx(N))/binary, ?PAD/binary>>))
          end}}
     || {File, Bad} <- [
            {"robust_union.avsc", [3, 4, 100, -1, -2]},
            {"robust_union_nonnull.avsc", [2, 3, 100, -1]}
        ],
        N <- Bad
    ].

%% Same indexes but valid, so the range check is not simply rejecting everything.
union_branch_index_in_range_test() ->
    Id = erlav_nif:erlav_init(path("robust_union.avsc")),
    ?assertEqual(#{<<"u">> => undefined},
                 erlav_nif:erlav_decode_fast(Id, <<(idx(0))/binary, ?PAD/binary>>)),
    ?assertEqual(#{<<"u">> => 7},
                 erlav_nif:erlav_decode_fast(Id, <<(idx(1))/binary, (enc_long(7))/binary>>)),
    ?assertEqual(#{<<"u">> => <<"ab">>},
                 erlav_nif:erlav_decode_fast(Id, <<(idx(2))/binary, (idx(2))/binary, "ab">>)),
    Id2 = erlav_nif:erlav_init(path("robust_union_nonnull.avsc")),
    ?assertEqual(#{<<"u">> => 7},
                 erlav_nif:erlav_decode_fast(Id2, <<(idx(0))/binary, (enc_long(7))/binary>>)),
    ?assertEqual(#{<<"u">> => <<"a">>},
                 erlav_nif:erlav_decode_fast(Id2, <<(idx(1))/binary, (idx(1))/binary, "a">>)).

enc_long(N) -> idx(N).

%% The branch index is a 32-bit value on the wire. A varint wider than 32
%% bits used to be truncated, so 2^32+1 aliased to branch 1 and decoded as a
%% valid value (defeating any range check).
union_branch_index_wider_than_32_bits_test_() ->
    [
        {"index " ++ integer_to_list(N),
         {timeout, ?TEST_TIMEOUT, fun() ->
              ?assertRejected(
                  decode_fast("robust_union.avsc",
                              <<(idx(N))/binary, (enc_long(7))/binary, ?PAD/binary>>))
          end}}
     || N <- [(1 bsl 32) + 1, (1 bsl 32) + 2, 1 bsl 31, (1 bsl 40) + 1]
    ].

%% Array of union: same, for the per-element branch index (a 64-bit long on the
%% wire here, looked up in an int-keyed map).
array_union_branch_index_out_of_range_sweep_test_() ->
    [
        {"index " ++ integer_to_list(N),
         {timeout, ?TEST_TIMEOUT, fun() ->
              %% block count 1, branch index N, then a string "x"
              Bin = <<(idx(1))/binary, (idx(N))/binary, (idx(1))/binary, "x", 0, ?PAD/binary>>,
              ?assertRejected(decode_fast("robust_array_union.avsc", Bin))
          end}}
     || N <- [2, 3, 99, -1, 1 bsl 31, (1 bsl 32) + 1, (1 bsl 33) + 1, 1 bsl 62]
    ].

%% A bad index followed by a good payload on the same cached schema id, several
%% times over: the good payload must keep decoding. This is a smoke test only.
%% The #38 side effect it is named after (operator[] inserting an empty entry
%% into the shared schema's member map for every unknown index) is NOT
%% observable from Erlang: an unfixed build passes this test too, because a
%% repeated bad index fails the same way with or without the stray entry. The
%% fix for that is a code-level change (find() instead of operator[]).
bad_union_index_then_good_payload_on_same_schema_test() ->
    Id = erlav_nif:erlav_init(path("robust_array_union.avsc")),
    Good = erlav_nif:erlav_encode(Id, #{<<"a">> => [<<"a">>, undefined, <<"bcd">>]}),
    Bad = fun(N) -> <<(idx(1))/binary, (idx(N))/binary, (idx(1))/binary, "x", 0>> end,
    lists:foreach(
      fun(N) ->
              ?assertMatch({error, _, _}, erlav_nif:erlav_decode_fast(Id, Bad(N))),
              ?assertEqual(#{<<"a">> => [<<"a">>, undefined, <<"bcd">>]},
                           erlav_nif:erlav_decode_fast(Id, Good))
      end, [2, 99, 1000, -1, 99, 99]).

%% Enum indexes use the same 32-bit varint reader: same aliasing, same fix.
enum_index_wider_than_32_bits_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% enum.avsc has fewer than 100 symbols, so 2^32+1 -> 1 would be "valid"
        ?assertRejected(
            decode_fast("enum.avsc", <<(idx((1 bsl 32) + 1))/binary, ?PAD/binary>>))
    end}.

%% ------------------------ always-on: array / map block counts (#40)
%%
%% A block count read off the wire used to drive a loop with no bound at all:
%% 2^62 elements meant an unbounded allocate-and-decode cycle (hang, OOM, or a
%% VM abort). Now: a count whose items cannot fit in the remaining input is
%% rejected up front; the negative form's byte length must fit in the input;
%% and items that occupy no bytes at all (empty record, fixed(0)) -- the one
%% case the input length cannot bound -- draw on a fixed budget per decode.

block_count_cases() ->
    %% zigzag(2^62) ~ 2^63: a count no real payload could satisfy
    [{"~2^62", <<254, 255, 255, 255, 255, 255, 255, 255, 255, 1>>},
     %% INT64_MIN: negating it overflows
     {"INT64_MIN", <<255, 255, 255, 255, 255, 255, 255, 255, 255, 1>>},
     %% negative form: count -2^40, followed by a block byte length
     {"-2^40 with byte length", <<255, 255, 255, 255, 255, 31, 2>>}].

huge_block_count_test_() ->
    [
        {Kind ++ " block count " ++ Name,
         {timeout, ?TEST_TIMEOUT, fun() ->
              ?assertRejected(decode_fast(File, <<Count/binary, ?PAD/binary>>))
          end}}
     || {Kind, File} <- [{"array", "robust_array.avsc"}, {"map", "robust_map.avsc"}],
        {Name, Count} <- block_count_cases()
    ].

%% Exactly as many items as the remaining input holds is fine; one more is not.
%% array<double>: each item is 8 bytes. Terminator byte included in the input.
block_count_exactly_fits_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Items = binary:copy(<<0:64>>, 3),
        %% 3 items + terminator
        ?assertEqual({ok, #{<<"a">> => [0.0, 0.0, 0.0]}},
                     decode_fast("robust_double_array.avsc",
                                 <<(idx(3))/binary, Items/binary, 0>>)),
        %% claims 4 items but only 3 (24 bytes) + a 1-byte terminator are there
        ?assertRejected(decode_fast("robust_double_array.avsc",
                                    <<(idx(4))/binary, Items/binary, 0>>))
    end}.

%% The negative form carries the block's byte length; it must fit the input.
negative_block_byte_length_must_fit_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Items = binary:copy(<<0:64>>, 2),
        %% count -2, byte length 16 (correct), 2 doubles, terminator
        ?assertEqual({ok, #{<<"a">> => [0.0, 0.0]}},
                     decode_fast("robust_double_array.avsc",
                                 <<(idx(-2))/binary, (idx(16))/binary, Items/binary, 0>>)),
        %% byte length claims far more than the input holds
        ?assertRejected(decode_fast("robust_double_array.avsc",
                                    <<(idx(-2))/binary, (idx(1000000))/binary, Items/binary, 0>>)),
        %% a negative byte length is meaningless
        ?assertRejected(decode_fast("robust_double_array.avsc",
                                    <<(idx(-2))/binary, (idx(-5))/binary, Items/binary, 0>>))
    end}.

%% Zero-width items are not bounded by the input, so a fixed budget applies.
%% Below it: decodes. Above it: rejected, in a few ms, with no big allocation.
zero_width_items_within_budget_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        N = 1000,
        {ok, #{<<"a">> := L}} = decode_fast("robust_zero_width_array.avsc",
                                            <<(idx(N))/binary, 0>>),
        ?assertEqual(N, length(L)),
        {ok, #{<<"m">> := M}} = decode_fast("robust_zero_width_map.avsc",
                                            <<(idx(1))/binary, (idx(1))/binary, "k", 0>>),
        ?assertEqual(#{<<"k">> => #{}}, M)
    end}.

zero_width_items_over_budget_test_() ->
    [
        {File ++ " " ++ integer_to_list(Count),
         {timeout, ?TEST_TIMEOUT, fun() ->
              T0 = erlang:monotonic_time(millisecond),
              ?assertRejected(decode_fast(File, <<(idx(Count))/binary, 0>>)),
              %% rejected up front, not after building the list
              ?assert(erlang:monotonic_time(millisecond) - T0 < ?CALL_TIMEOUT)
          end}}
     || {File, Count} <- [{"robust_zero_width_array.avsc", 20000000},
                          {"robust_zero_width_array.avsc", 1 bsl 40},
                          {"robust_zero_width_array.avsc", (1 bsl 20) + 1}]
    ].

%% The budget is per decode call, not per array: several arrays that each fit
%% must not add up past it (nested arrays would otherwise multiply).
zero_width_budget_is_shared_across_arrays_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        %% 2000 inner arrays of 1000 empty records = 2,000,000 items > 2^20,
        %% with every individual block small.
        Inner = <<(idx(1000))/binary, 0>>,
        Outer = iolist_to_binary([idx(2000), binary:copy(Inner, 2000), 0]),
        ?assertRejected(decode_fast("robust_zero_width_nested.avsc", Outer))
    end}.

%% A previous over-budget decode must not poison the next one (the budget is
%% reset per call).
zero_width_budget_resets_between_decodes_test() ->
    Id = erlav_nif:erlav_init(path("robust_zero_width_array.avsc")),
    Big = <<(idx(1000000))/binary, 0>>,
    lists:foreach(
      fun(_) ->
              ?assertMatch(#{<<"a">> := _}, erlav_nif:erlav_decode_fast(Id, Big))
      end, lists:seq(1, 5)).

%% ------------------------------------- erlav_decode/2 (legacy, iterator based)
%%
%% Characterisation tests: they pin down what erlav_decode/2 does *today*.
%% It only decodes top-level long/int/float/double fields; it is not a
%% general decoder. If it is fixed or removed, update these.

legacy_decodes_numeric_scalars_and_matches_fast_test() ->
    File = "robust_scalars.avsc",
    Term = #{<<"l">> => 300, <<"i">> => -5, <<"d">> => 2.5, <<"f">> => 1.5},
    Bin = encode(File, Term),
    Id = erlav_nif:erlav_init(path(File)),
    ?assertEqual(Term, erlav_nif:erlav_decode(Id, Bin)),
    ?assertEqual(
        erlav_nif:erlav_decode_fast(Id, Bin), erlav_nif:erlav_decode(Id, Bin)
    ).

%% Nullable, array, map and nested-record fields are skipped without error or
%% warning: the result just lacks those keys.
legacy_silently_omits_non_scalar_fields_test() ->
    File = "robust_legacy_tail.avsc",
    Term = #{
        <<"l">> => 5,
        <<"n">> => 6,
        <<"al">> => [1, 2],
        <<"m">> => #{<<"k">> => 1}
    },
    Bin = encode(File, Term),
    Id = erlav_nif:erlav_init(path(File)),
    ?assertEqual(#{<<"l">> => 5}, erlav_nif:erlav_decode(Id, Bin)),
    ?assertEqual(Term, erlav_nif:erlav_decode_fast(Id, Bin)).

%% Because the skipped field's bytes are never consumed, a field *after* it
%% is read from the wrong position and comes back with a wrong value --
%% silently. (n = 5 occupies bytes 0-1; l = 9 is byte 2; the legacy decoder
%% reads l from byte 0.)
legacy_misdecodes_fields_after_a_skipped_field_test() ->
    File = "robust_legacy_desync.avsc",
    Term = #{<<"n">> => 5, <<"l">> => 9},
    Bin = encode(File, Term),
    Id = erlav_nif:erlav_init(path(File)),
    ?assertEqual(Term, erlav_nif:erlav_decode_fast(Id, Bin)),
    Legacy = erlav_nif:erlav_decode(Id, Bin),
    ?assertNotEqual(maps:get(<<"l">>, Term), maps:get(<<"l">>, Legacy)).

%% string / bytes / boolean are not handled by the iterator decode_scalar:
%% the NIF call raises badarg.
legacy_raises_badarg_on_string_bytes_boolean_fields_test_() ->
    [
        {File, fun() ->
            Bin = encode(File, Term),
            Id = erlav_nif:erlav_init(path(File)),
            ?assertEqual(Term, erlav_nif:erlav_decode_fast(Id, Bin)),
            ?assertError(badarg, erlav_nif:erlav_decode(Id, Bin))
        end}
     || {File, Term} <- [
            {"robust_string.avsc", #{<<"s">> => <<"hi">>}},
            {"robust_bytes.avsc", #{<<"v">> => <<1, 2>>}},
            {"robust_boolean.avsc", #{<<"v">> => true}}
        ]
    ].

legacy_unknown_schema_id_raises_badarg_test() ->
    ?assertError(badarg, erlav_nif:erlav_decode(999999999, <<1, 2, 3>>)).

legacy_non_integer_schema_id_raises_badarg_test() ->
    ?assertError(badarg, erlav_nif:erlav_decode(<<"x">>, <<1>>)).

%% Quirk shared with erlav_decode_fast/2: input that is not a binary yields an
%% empty map rather than badarg.
non_binary_input_returns_empty_map_test() ->
    Id = erlav_nif:erlav_init(path("robust_scalars.avsc")),
    ?assertEqual(#{}, erlav_nif:erlav_decode(Id, not_a_binary)),
    ?assertEqual(#{}, erlav_nif:erlav_decode_fast(Id, not_a_binary)).

legacy_empty_input_returns_error_tuple_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        ?assertRejected(decode(erlav_decode, "robust_scalars.avsc", <<>>))
    end}.

legacy_truncated_double_returns_error_tuple_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Full = encode("robust_scalars.avsc",
                      #{<<"l">> => 1, <<"i">> => 1, <<"d">> => 2.5, <<"f">> => 1.5}),
        %% 9 bytes: l(1) i(1) + 7 of the double's 8 bytes
        ?assertRejected(decode(erlav_decode, "robust_scalars.avsc",
                               binary:part(Full, 0, 9)))
    end}.

legacy_varint_overflow_returns_error_tuple_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Bin = <<(binary:copy(<<255>>, 12))/binary, 1>>,
        ?assertRejected(decode(erlav_decode, "robust_scalars.avsc", Bin))
    end}.

%% ---------------------------- opt-in: open bugs (none at present), see header

known_bug_test_() ->
    case os:getenv("ERLAV_KNOWN_BUGS") of
        "1" ->
            [
                {Name, {timeout, 300, Fun}}
             || {Name, Fun} <- known_bug_cases()
            ] ++ [{"cleanup", fun stop_peer/0}];
        _ ->
            io:format(
                user,
                "~n[decode_robustness_test] ~p known-bug tests skipped "
                "(none open); set ERLAV_KNOWN_BUGS=1 to run them~n",
                [length(known_bug_cases())]
            ),
            [{"cleanup", fun stop_peer/0}]
    end.

known_bug_cases() ->
    %% Each case asserts the *desired* behaviour: {error, _, _}, no crash.
    %% Empty since #30, #38 and #40 are fixed; add a case here for any new
    %% known-open bug, and delete this gate once none remain.
    [].

%% Every proper prefix of a valid encoding is truncated input and must be
%% rejected (erlavro_rejects_every_proper_prefix_test_ confirms that is a
%% sound expectation for these fixtures).
truncation_sweep({File, Term}) ->
    Bin = encode(File, Term),
    Results = [
        {N, decode_fast(File, binary:part(Bin, 0, N))}
     || N <- lists:seq(0, byte_size(Bin) - 1)
    ],
    Bad = [{N, summarize(R)} || {N, R} <- Results, not is_rejection(R)],
    ?assertEqual([], Bad).

is_rejection({ok, {error, _, _}}) -> true;
is_rejection(_) -> false.

summarize({ok, Term}) when is_map(Term) -> returned_garbage_map;
summarize(Other) -> Other.
