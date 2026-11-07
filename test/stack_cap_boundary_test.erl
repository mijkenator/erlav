-module(stack_cap_boundary_test).

%% Several encode/decode paths keep a small fixed-size buffer on the stack and
%% fall back to a heap vector above a cap (see #28). A wrong cap check does not
%% fail loudly: a buffer sized for the wrong element width overflows the stack
%% silently. These tests drive each path just below, at, and just above its cap
%% and compare the *exact* encoded bytes (not only an encode/decode round trip,
%% which could hide an encoder bug that the decoder happens to undo).
%%
%% Caps (c_src/mkh_avro2.hh unless noted):
%%   encoderecord found/present arrays ........ 64 fields   (decode() too, in
%%                                                            mkh_avro_decoder.cc)
%%   encodearray element handle buffer ......... 384 elements
%%   encodearray varint scratch ................ 3840 bytes = 384 longs (10 B
%%                                                max each) or 768 ints (5 B)
%%   encodemap map<int>/map<long> entry scratch  256 bytes; the size check assumes a
%%                                                10-byte key-length varint, so it
%%                                                switches to heap at key > 241 (int) /
%%                                                > 236 (long). Real bytes written
%%                                                (2-byte key length) exceed 256 at
%%                                                key >= 245 (long)
%%   decode_map MapEntries (mkh_avro_decoder.cc) 16 entries
%%
%% Values are chosen to need the maximum varint width, since the scratch-size
%% checks are computed from the worst case.

-include_lib("eunit/include/eunit.hrl").

%% --- reference Avro encoding -------------------------------------------

zz(N) when N >= 0 -> N bsl 1;
zz(N) -> ((-N) bsl 1) - 1.

varint(N) when N < 128 -> <<N>>;
varint(N) -> <<1:1, (N band 127):7, (varint(N bsr 7))/binary>>.

enc(N) -> varint(zz(N)).

str(B) -> <<(enc(byte_size(B)))/binary, B/binary>>.

array(Vs) ->
    case Vs of
        [] -> <<0>>;
        _ -> iolist_to_binary([enc(length(Vs)), [enc(V) || V <- Vs], 0])
    end.

%% Single-entry (order-independent) or empty map<..>.
map1(none, _) -> <<0>>;
map1({K, V}, ValEnc) ->
    iolist_to_binary([enc(1), str(K), ValEnc(V), 0]).

roundtrip(SchemaId, Term, Expected) ->
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(Expected, Encoded),
    ?assertEqual(Term, erlav_nif:erlav_decode_fast(SchemaId, Encoded)).

%% --- records wider than the 64-field stack cap ---------------------------

wide_schema(K) ->
    Dir = filename:join(os:getenv("TMPDIR", "/tmp"), "erlav_stack_caps"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Path = filename:join(Dir, "wide" ++ integer_to_list(K) ++ ".avsc"),
    Fields = [iolist_to_binary(io_lib:format(
                "{\"name\": \"f~3..0w\", \"type\": \"int\"}", [I]))
              || I <- lists:seq(0, K - 1)],
    Json = iolist_to_binary(
             ["{\"type\": \"record\", \"name\": \"Wide", integer_to_list(K),
              "\", \"fields\": [", lists:join(<<", ">>, Fields), "]}"]),
    ok = file:write_file(Path, Json),
    erlav_nif:erlav_init(list_to_binary(Path)).

wide_term(K) ->
    maps:from_list([{iolist_to_binary(io_lib:format("f~3..0w", [I])), I + 1}
                    || I <- lists:seq(0, K - 1)]).

wide_records_test_() ->
    [{lists:flatten(io_lib:format("record with ~p fields", [K])),
      fun() ->
              Id = wide_schema(K),
              Term = wide_term(K),
              Expected = iolist_to_binary([enc(I + 1) || I <- lists:seq(0, K - 1)]),
              roundtrip(Id, Term, Expected)
      end}
     || K <- [3, 4, 63, 64, 65, 66, 70, 100, 128, 129, 200]].

%% Extra, unknown input keys must not disturb the found/present bookkeeping,
%% including right at the cap.
wide_record_with_unknown_keys_test() ->
    K = 65,
    Id = wide_schema(K),
    Term = wide_term(K),
    Noisy = maps:merge(Term, #{<<"zzz">> => 1, <<"f000x">> => 2, <<>> => 3}),
    Expected = iolist_to_binary([enc(I + 1) || I <- lists:seq(0, K - 1)]),
    ?assertEqual(Expected, erlav_nif:erlav_encode(Id, Noisy)).

%% --- arrays around 384 elements / 3840 scratch bytes ---------------------

long_vals(N) ->
    %% |v| >= 2^62 -> zigzag >= 2^63 -> 10-byte varint, alternating sign.
    [case I rem 2 of
         0 -> (1 bsl 62) + I;
         1 -> -(1 bsl 62) - I
     end || I <- lists:seq(1, N)].

int_vals(N) ->
    %% |v| >= 2^27 -> 5-byte varint, alternating sign.
    [case I rem 2 of
         0 -> 2000000000 - I;
         1 -> -2000000000 + I
     end || I <- lists:seq(1, N)].

array_lengths_test_() ->
    [{lists:flatten(io_lib:format("array<long>+array<int> of ~p elements", [N])),
      fun() ->
              Id = erlav_nif:erlav_init(<<"test/stack_cap_arrays.avsc">>),
              L = long_vals(N),
              I = int_vals(N),
              Term = #{<<"l">> => L, <<"i">> => I},
              roundtrip(Id, Term, <<(array(L))/binary, (array(I))/binary>>)
      end}
     || N <- [0, 1, 383, 384, 385, 386, 767, 768, 769, 770, 1000, 5000]].

%% Same arrays in the negative-block-count form, which encodes elements into a
%% separate scratch vector first (erlav_init/2 option; see #15).
negative_block_count_arrays_test_() ->
    [{lists:flatten(io_lib:format("negative block count, ~p elements", [N])),
      fun() ->
              Id = erlav_nif:erlav_init(<<"test/stack_cap_arrays.avsc">>,
                                        [use_negative_block_count]),
              L = long_vals(N),
              I = int_vals(N),
              Term = #{<<"l">> => L, <<"i">> => I},
              Encoded = erlav_nif:erlav_encode(Id, Term),
              ?assertEqual(Term, erlav_nif:erlav_decode_fast(Id, Encoded))
      end}
     || N <- [1, 384, 385, 768, 769]].

%% --- maps ---------------------------------------------------------------

key(Len) -> binary:copy(<<"k">>, Len).

map_entry_scratch_test_() ->
    [{lists:flatten(io_lib:format("map<~p> key of ~p bytes", [Type, KLen])),
      fun() ->
              Id = erlav_nif:erlav_init(<<"test/stack_cap_maps.avsc">>),
              K = key(KLen),
              {Field, V} = case Type of
                               int -> {<<"mi">>, -2000000000};   %% 5-byte varint
                               long -> {<<"ml">>, 1 bsl 62}      %% 10-byte varint
                           end,
              Others = [F || F <- [<<"mi">>, <<"ml">>, <<"ms">>], F =/= Field],
              Term = maps:from_list([{Field, #{K => V}}] ++ [{F, #{}} || F <- Others]),
              Parts = fun(F) when F =:= Field -> map1({K, V}, fun enc/1);
                         (_) -> <<0>>
                      end,
              Expected = iolist_to_binary([Parts(F) || F <- [<<"mi">>, <<"ml">>, <<"ms">>]]),
              roundtrip(Id, Term, Expected)
      end}
     || {Type, KLen} <- [{int, 240}, {int, 241}, {int, 242}, {int, 243},
                         {long, 235}, {long, 236}, {long, 237}, {long, 238},
                         %% Real bytes written = 2 (key length varint) + key + 10
                         %% (value) exceed the 256-byte scratch at key >= 245, so these
                         %% only stay in bounds if the size check adds the value width.
                         {long, 244}, {long, 245}, {long, 246}, {long, 247}]].

entry_size_int(K, V) -> byte_size(str(K)) + byte_size(enc(V)).
entry_size_str(K, V) -> byte_size(str(K)) + byte_size(str(V)).

map_entry_count_test_() ->
    [{lists:flatten(io_lib:format("map<int>+map<string> with ~p entries", [N])),
      fun() ->
              Id = erlav_nif:erlav_init(<<"test/stack_cap_maps.avsc">>),
              Seq = lists:seq(1, N),
              MI = maps:from_list([{integer_to_binary(I), I * 1000003} || I <- Seq]),
              MS = maps:from_list([{integer_to_binary(I), integer_to_binary(I * 7)} || I <- Seq]),
              Term = #{<<"mi">> => MI, <<"ml">> => #{}, <<"ms">> => MS},
              Encoded = erlav_nif:erlav_encode(Id, Term),
              %% Map iteration order is not specified, so check total size
              %% (header + entries + terminator) and the decoded content.
              Hdr = fun(0) -> 1; (Cnt) -> byte_size(enc(Cnt)) + 1 end,
              Size = Hdr(N) + lists:sum([entry_size_int(K, V) || {K, V} <- maps:to_list(MI)])
                   + 1
                   + Hdr(N) + lists:sum([entry_size_str(K, V) || {K, V} <- maps:to_list(MS)]),
              ?assertEqual(Size, byte_size(Encoded)),
              ?assertEqual(Term, erlav_nif:erlav_decode_fast(Id, Encoded))
      end}
     || N <- [0, 1, 15, 16, 17, 18, 33, 100]].
