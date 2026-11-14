-module(stack_buffer_boundary_test).

-include_lib("eunit/include/eunit.hrl").

%
% Complements stack_cap_boundary_test (which pins every cap at cap-1/cap/cap+1
% with exact bytes) with two cases it does not cover, both about reusing the
% StackOrHeapBuffer state rather than about a single size:
%
%   - encoderecord found/present with some fields omitted, at and around the
%     64-field cap: the "present" flags must be right on stack and heap storage.
%   - encodemap entry scratch flipping between stack-sized and heap-sized
%     entries on consecutive iterations of one map.
%

% erlavro's decoder returns records/maps as proplists; turn them back into
% maps so they compare against the input term. Test data never contains an
% empty map or record, so an empty list is always an empty array.
norm([{K, _} | _] = L) when is_binary(K) ->
    maps:from_list([{Key, norm(V)} || {Key, V} <- L]);
norm(L) when is_list(L) -> [norm(E) || E <- L];
norm(V) -> V.

field_name(I) -> list_to_binary("f" ++ integer_to_list(I)).

% Nullable fields so that omitting them is legal.
write_nullable_record_schema(N) ->
    Fields = lists:join(<<",\n">>,
        [iolist_to_binary(["{\"name\": \"", field_name(I),
                           "\", \"type\": [\"null\", \"long\"], ",
                           "\"default\": null}"])
         || I <- lists:seq(1, N)]),
    Json = iolist_to_binary(
        ["{\"type\": \"record\", \"name\": \"Wide\", \"namespace\": ",
         "\"erlav.test\", \"fields\": [", Fields, "]}"]),
    Unique = erlang:unique_integer([positive]),
    File = lists:flatten(
        io_lib:format("test/tmp_stack_boundary_~p_~p.avsc", [Unique, N])),
    ok = file:write_file(File, Json),
    File.

record_around_field_cap_missing_fields_test_() ->
    [{integer_to_list(N) ++ " fields, half missing", fun() ->
        File = write_nullable_record_schema(N),
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

key(K) -> list_to_binary(lists:duplicate(K, $k)).

% Entries alternating between stack-sized and heap-sized keys in one map, so
% the scratch buffer flips between the two on consecutive iterations.
map_mixed_entry_sizes_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_map.avsc">>),
    Map = maps:from_list(
        [{key(K), I} || {I, K} <- lists:zip(
            lists:seq(1, 8), [5, 400, 7, 236, 237, 3, 900, 235])]),
    Term = #{<<"mapField">> => Map},
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(Term, erlav_nif:erlav_decode_fast(SchemaId, Encoded)).
