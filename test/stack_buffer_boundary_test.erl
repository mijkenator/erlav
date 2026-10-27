-module(stack_buffer_boundary_test).

-include_lib("eunit/include/eunit.hrl").

%
% Issue #28: encodearray (element buffer, varint scratch), encoderecord
% (found/present arrays) and encodemap (per-entry scratch) each pick between
% an on-stack array and a heap vector based on a size cap. These tests pin
% the behaviour on both sides of every cap (cap-1, cap, cap+1), checking the
% output against erlavro's decoder, so any off-by-one in the stack-vs-heap
% decision shows up as a wrong or corrupted encode.
%
% Caps (c_src/mkh_avro2.hh):
%   encodearray element buffer   384 elements
%   encodearray varint scratch   3840 bytes: len * 10 (long), len * 5 (int)
%   encoderecord found/present   64 fields
%   encodemap entry scratch      256 bytes: 10 + key bytes + 10 (long) / 5 (int)
%

roundtrip(SchemaFile, Term) ->
    SchemaId = erlav_nif:erlav_init(list_to_binary(SchemaFile)),
    {ok, SchemaJSON} = file:read_file(SchemaFile),
    ErlavroDecoder = avro:make_simple_decoder(SchemaJSON, []),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(Term, norm(ErlavroDecoder(Encoded))),
    ?assertEqual(Term, erlav_nif:erlav_decode_fast(SchemaId, Encoded)).

% erlavro's decoder returns records/maps as proplists; turn them back into
% maps so they compare against the input term. Test data never contains an
% empty map or record, so an empty list is always an empty array.
norm([{K, _} | _] = L) when is_binary(K) ->
    maps:from_list([{Key, norm(V)} || {Key, V} <- L]);
norm(L) when is_list(L) -> [norm(E) || E <- L];
norm(V) -> V.

ints(N) -> [signed(I) * I * 1000003 || I <- lists:seq(1, N)].

signed(I) when I rem 2 =:= 0 -> 1;
signed(_) -> -1.

% ---------------------------------------------------------------------------
% encodearray: element buffer cap (384) and varint scratch cap (3840 bytes).
% ---------------------------------------------------------------------------

array_long_around_element_cap_test_() ->
    [{"len " ++ integer_to_list(N), fun() ->
        roundtrip("test/array_simple.avsc", #{<<"arrayField">> => ints(N)})
     end} || N <- [0, 1, 383, 384, 385, 1000]].

% array<int>: scratch is len * 5, so the scratch cap is crossed at 768
% elements, well after the 384-element buffer cap.
array_int_around_caps_test_() ->
    [{"len " ++ integer_to_list(N), fun() ->
        roundtrip("test/array_simple1.avsc", #{<<"arrayField">> => ints(N)})
     end} || N <- [383, 384, 385, 767, 768, 769, 2000]].

% ---------------------------------------------------------------------------
% encoderecord: found/present arrays cap (64 fields). More than 3 fields
% takes the single-pass path that owns those arrays.
% ---------------------------------------------------------------------------

record_around_field_cap_test_() ->
    [{integer_to_list(N) ++ " fields", fun() ->
        File = write_record_schema(N),
        try
            Term = maps:from_list(
                [{field_name(I), I * 7919} || I <- lists:seq(1, N)]),
            roundtrip(File, Term)
        after
            file:delete(File)
        end
     end} || N <- [4, 63, 64, 65, 130]].

% Same, with every other field omitted: exercises the "present" flags on
% both stack and heap storage. Fields are nullable so omission is legal.
record_around_field_cap_missing_fields_test_() ->
    [{integer_to_list(N) ++ " fields, half missing", fun() ->
        File = write_record_schema(N, nullable),
        try
            SchemaId = erlav_nif:erlav_init(list_to_binary(File)),
            Present = [I || I <- lists:seq(1, N), I rem 2 =:= 1],
            Term = maps:from_list([{field_name(I), I * 7919} || I <- Present]),
            Full = maps:merge(
                maps:from_list([{field_name(I), null}
                                || I <- lists:seq(1, N)]),
                Term),
            {ok, SchemaJSON} = file:read_file(File),
            Decoder = avro:make_simple_decoder(SchemaJSON, []),
            Encoded = erlav_nif:erlav_encode(SchemaId, Term),
            ?assertEqual(Full, norm(Decoder(Encoded)))
        after
            file:delete(File)
        end
     end} || N <- [63, 64, 65, 130]].

field_name(I) -> list_to_binary("f" ++ integer_to_list(I)).

write_record_schema(N) -> write_record_schema(N, plain).

write_record_schema(N, Kind) ->
    Type = case Kind of
        plain -> <<"\"long\"">>;
        nullable -> <<"[\"null\", \"long\"]">>
    end,
    Default = case Kind of
        plain -> <<>>;
        nullable -> <<", \"default\": null">>
    end,
    Fields = lists:join(<<",\n">>,
        [iolist_to_binary(["{\"name\": \"", field_name(I), "\", \"type\": ",
                           Type, Default, "}"])
         || I <- lists:seq(1, N)]),
    Json = iolist_to_binary(
        ["{\"type\": \"record\", \"name\": \"Wide\", \"namespace\": ",
         "\"erlav.test\", \"fields\": [", Fields, "]}"]),
    Unique = erlang:unique_integer([positive]),
    File = lists:flatten(
        io_lib:format("test/tmp_stack_boundary_~p_~p.avsc", [Unique, N])),
    ok = file:write_file(File, Json),
    File.

% ---------------------------------------------------------------------------
% encodemap: per-entry scratch cap (256 bytes). The worst-case entry size is
% 10 (key-length varint) + key bytes + 10 (long) / 5 (int) value varint, so
% the cap is crossed at key length 236 for map<long> and 241 for map<int>.
% ---------------------------------------------------------------------------

map_long_around_entry_cap_test_() ->
    [{"key len " ++ integer_to_list(K), fun() ->
        roundtrip("test/tschema_map.avsc",
                  #{<<"mapField">> => #{key(K) => -9223372036854775808}})
     end} || K <- [1, 235, 236, 237, 600]].

map_int_around_entry_cap_test_() ->
    [{"key len " ++ integer_to_list(K), fun() ->
        roundtrip("test/tschema_map_int.avsc",
                  #{<<"mapField">> => #{key(K) => -2147483648}})
     end} || K <- [1, 240, 241, 242, 600]].

% Entries alternating between stack-sized and heap-sized keys in one map, so
% the scratch buffer flips between the two on consecutive iterations.
map_mixed_entry_sizes_test() ->
    Map = maps:from_list(
        [{key(K), I} || {I, K} <- lists:zip(
            lists:seq(1, 8), [5, 400, 7, 236, 237, 3, 900, 235])]),
    roundtrip("test/tschema_map.avsc", #{<<"mapField">> => Map}).

key(K) -> list_to_binary(lists:duplicate(K, $k)).
