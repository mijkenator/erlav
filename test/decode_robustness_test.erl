%% Decoder robustness: malformed / truncated input, and erlav_decode/2
%% (the legacy iterator-based decoder) behaviour.
%%
%% Malformed input can crash the whole VM today (no bounds checking: #30,
%% #38, #40), which would take the eunit run down with it. So every
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

legacy_varint_overflow_returns_error_tuple_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        Bin = <<(binary:copy(<<255>>, 12))/binary, 1>>,
        ?assertRejected(decode(erlav_decode, "robust_scalars.avsc", Bin))
    end}.

%% ---------------------------- opt-in: open bugs (#30, #38, #40), see header

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
                "(#30 #38 #40); set ERLAV_KNOWN_BUGS=1 to run them~n",
                [length(known_bug_cases())]
            ),
            [{"cleanup", fun stop_peer/0}]
    end.

known_bug_cases() ->
    %% Each case asserts the *desired* behaviour: {error, _, _}, no crash.
    [
        {"#30 empty input", fun() ->
            ?assertRejected(decode_fast("robust_scalars.avsc", <<>>))
        end},
        {"#30 empty input, record with string", fun() ->
            ?assertRejected(decode_fast("robust_string.avsc", <<>>))
        end},
        {"#30 truncation at every offset: all field types", fun() ->
            truncation_sweep(fixture_all())
        end},
        {"#30 truncation at every offset: array of union", fun() ->
            truncation_sweep(fixture_array_union())
        end},
        {"#30 negative string length", fun() ->
            %% zigzag(-2) = 3
            ?assertRejected(
                decode_fast("robust_string.avsc", <<3, ?PAD/binary>>)
            )
        end},
        {"#30 string length 2^40 (far past the buffer)", fun() ->
            Len = <<128, 128, 128, 128, 128, 64>>,
            ?assertRejected(
                decode_fast("robust_string.avsc", <<Len/binary, ?PAD/binary>>)
            )
        end},
        {"#30 legacy erlav_decode/2 on empty input", fun() ->
            ?assertRejected(decode(erlav_decode, "robust_scalars.avsc", <<>>))
        end}
    ] ++
        [
            {"#38 union branch index " ++ Name, fun() ->
                ?assertRejected(
                    decode_fast("robust_union.avsc", <<Idx/binary, ?PAD/binary>>)
                )
            end}
         || {Name, Idx} <- [
                %% schema is ["null","long","string"]: valid indexes are 0..2
                {"3 (one past the last member)", <<6>>},
                {"100", <<200, 1>>},
                {"-1 (becomes 4294967295 as uint32)", <<1>>}
            ]
        ] ++
        [
            {"#40 " ++ Kind ++ " block count " ++ Name, fun() ->
                ?assertRejected(
                    decode_fast(File, <<Count/binary, ?PAD/binary>>)
                )
            end}
         || {Kind, File} <- [
                {"array", "robust_array.avsc"},
                {"map", "robust_map.avsc"}
            ],
            {Name, Count} <- [
                %% zigzag(2^62) ~ 2^63: a count no real payload could satisfy
                {"~2^62", <<254, 255, 255, 255, 255, 255, 255, 255, 255, 1>>},
                %% INT64_MIN: negating it overflows
                {"INT64_MIN", <<255, 255, 255, 255, 255, 255, 255, 255, 255, 1>>},
                %% negative form: count -2^40, followed by a block byte length
                {"-2^40 with byte length", <<255, 255, 255, 255, 255, 31, 2>>}
            ]
        ].

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
