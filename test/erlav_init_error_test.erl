-module(erlav_init_error_test).

-include_lib("eunit/include/eunit.hrl").

%% Regression tests for two fixes to erlav_init/1 (c_src/erlav_nif.cpp):
%%
%% 1. erlav_init_nif previously called mkh_avro2::read_schema (JSON parse +
%%    SchemaItem tree build) with no try/catch at all, unlike
%%    erlav_encode_nif right below it. A malformed .avsc file threw a raw
%%    C++ exception straight across the NIF boundary -- undefined behavior
%%    that can crash the whole BEAM process, not just fail the call. This
%%    is now wrapped the same way erlav_encode_nif already is.
%%
%% 2. erlav_init/1 previously had no consistent failure contract: success
%%    returned a plain integer (schema id >= 1), and its one existing
%%    failure case (enif_inspect_binary failing on the input) also
%%    returned a plain integer (0) -- indistinguishable from a real id by
%%    any caller that only checks "is it an integer". Now every failure
%%    path returns {error, Msg, Code}, matching erlav_safe_encode/2's
%%    existing contract, so callers can branch on the return shape instead
%%    of just inspecting the type.
%%
%% These write scratch .avsc files under test/ (not /tmp) so relative
%% paths resolve the same way whether run via `rebar3 eunit` or a plain
%% `erl` shell from the repo root, matching every other schema-file-using
%% test in this suite.

-define(SCRATCH_DIR, "test").

scratch_path(Name) ->
    Unique = erlang:unique_integer([positive]),
    filename:join(?SCRATCH_DIR, io_lib:format("tmp_init_err_~p_~s", [Unique, Name])).

write_scratch(Name, Content) ->
    Path = scratch_path(Name),
    ok = file:write_file(Path, Content),
    Path.

with_scratch_file(Name, Content, Fun) ->
    Path = write_scratch(Name, Content),
    try
        Fun(list_to_binary(Path))
    after
        file:delete(Path)
    end.

%% --- Non-binary argument: was the bare-int-0 sentinel, now {error, _, _} ---

non_binary_argument_test() ->
    ?assertMatch({error, _, _}, erlav_nif:erlav_init(not_a_binary)).

non_binary_argument_error_message_test() ->
    {error, Msg, _Code} = erlav_nif:erlav_init(not_a_binary),
    %% erlang:list_to_binary/enif_make_string route through the same
    %% ERL_NIF_LATIN1 string path as every other error tuple in this NIF --
    %% assert it's a real diagnostic string, not e.g. an empty binary.
    ?assert(is_binary(Msg) orelse is_list(Msg)),
    ?assert(erlang:iolist_size(Msg) > 0).

%% --- Malformed JSON: used to throw a raw C++ exception across the NIF
%% boundary (undefined behavior, could crash the BEAM); now caught and
%% converted to {error, Msg, Code}. Simply completing this test at all
%% (the eunit run doesn't die) is itself part of what's being verified. ---

malformed_json_test() ->
    with_scratch_file("malformed.avsc", <<"{ this is not valid json">>, fun(Path) ->
        Ret = erlav_nif:erlav_init(Path),
        ?assertMatch({error, _, _}, Ret)
    end).

truncated_json_test() ->
    with_scratch_file("truncated.avsc", <<"{\"type\": \"record\"">>, fun(Path) ->
        ?assertMatch({error, _, _}, erlav_nif:erlav_init(Path))
    end).

%% --- Valid JSON that isn't a valid Avro schema (missing required keys the
%% parser assumes are present, e.g. "namespace"/"fields") -- also used to
%% throw uncaught (a json::type_error on a null field access), now caught. ---

valid_json_invalid_schema_test() ->
    with_scratch_file("no_fields.avsc", <<"{\"type\": \"record\", \"name\": \"X\"}">>, fun(Path) ->
        ?assertMatch({error, _, _}, erlav_nif:erlav_init(Path))
    end).

empty_json_object_test() ->
    with_scratch_file("empty.avsc", <<"{}">>, fun(Path) ->
        ?assertMatch({error, _, _}, erlav_nif:erlav_init(Path))
    end).

%% --- Nonexistent file: std::ifstream silently opens in a failed state, so
%% json::parse sees an empty stream and throws a parse_error -- also
%% previously uncaught. ---

nonexistent_file_test() ->
    Ret = erlav_nif:erlav_init(<<"test/this_file_does_not_exist_12345.avsc">>),
    ?assertMatch({error, _, _}, Ret).

%% --- A failed init must not corrupt the registry for that filename: fix
%% #1's try/catch only stops the crash. The map-ordering fix (register the
%% filename -> id mapping *after* read_schema succeeds, not before) is what
%% makes a retry against the same filename actually work once the
%% underlying file is corrected -- verify both parts together. ---

retry_after_fixing_malformed_schema_test() ->
    Path = scratch_path("retry.avsc"),
    Bin = list_to_binary(Path),
    ok = file:write_file(Path, <<"{ not json at all">>),
    try
        ?assertMatch({error, _, _}, erlav_nif:erlav_init(Bin)),
        {ok, Valid} = file:read_file("test/single_int.avsc"),
        ok = file:write_file(Path, Valid),
        SchemaId = erlav_nif:erlav_init(Bin),
        ?assert(is_integer(SchemaId)),
        ?assert(SchemaId > 0),
        %% And the id is fully usable, not just "not a bare 0/error tuple".
        Encoded = erlav_nif:erlav_encode(SchemaId, #{<<"f">> => 5}),
        Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(5, maps:get(<<"f">>, Decoded))
    after
        file:delete(Path)
    end.

%% --- A schema that fails to parse must not consume a schema id that a
%% subsequently-registered *valid* schema then can't get -- i.e. failed
%% attempts should not leave gaps that shift numbering for other callers.
%% (This is a behavioral guarantee of the "register only after success"
%% ordering fix, not a hard contract erlav_init/1 ever promised elsewhere.)

failed_init_does_not_break_other_schemas_test() ->
    BadPath = scratch_path("breaks_nothing.avsc"),
    ok = file:write_file(BadPath, <<"{ not json">>),
    try
        ?assertMatch({error, _, _}, erlav_nif:erlav_init(list_to_binary(BadPath))),
        %% A completely unrelated, already-well-formed schema must still
        %% init and work normally after a prior failed attempt.
        SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
        ?assert(is_integer(SchemaId)),
        Encoded = erlav_nif:erlav_encode(SchemaId, #{<<"f">> => 12345}),
        Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        ?assertEqual(12345, maps:get(<<"f">>, Decoded))
    after
        file:delete(BadPath)
    end.

%% --- Successful init is unaffected: same-file idempotency and distinct
%% ids for distinct schemas must still hold with the new try/catch +
%% register-after-success ordering in place. ---

valid_schema_still_returns_plain_integer_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/single_int.avsc">>),
    ?assert(is_integer(SchemaId)),
    ?assert(SchemaId > 0).

valid_schema_idempotent_after_fix_test() ->
    Id1 = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    Id2 = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
    ?assertEqual(Id1, Id2).
