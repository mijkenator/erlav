-module(concurrency_test).

-include_lib("eunit/include/eunit.hrl").

%% Regression tests for the c_src/erlav_nif.cpp thread-safety fix:
%% erlav_schema_map / erlav_encoders_map were plain std::maps with no
%% synchronization, and erlav_encode_nif/erlav_decode_nif(_fast) looked up
%% a schema via std::map::operator[] -- which itself inserts a node for an
%% unknown key, turning every "read" into a potential concurrent write.
%% BEAM runs NIFs from many scheduler threads at once, so two encode/decode
%% calls (or an encode racing an erlav_init) hitting the same map
%% concurrently was a real data race, not a theoretical one (reproduced
%% directly with ThreadSanitizer against this exact access pattern).
%%
%% The fix: erlav_init_nif takes a std::unique_lock (the only writer);
%% erlav_encode_nif/erlav_decode_nif/erlav_decode_nif_fast look the schema
%% up via a new find_schema/1 helper (std::map::find -- a true read) under
%% a std::shared_lock, and return badarg for an unknown id instead of
%% falling through to encode/decode with a null SchemaItem*.
%%
%% These tests can't observe a data race directly, but they hammer the
%% exact concurrent-access patterns that used to race (concurrent
%% erlav_init/1 registering distinct new schemas, concurrent encode/decode
%% against already-registered schemas, and both together) from many
%% processes at once. Under the old code this class of test would be
%% expected to occasionally corrupt the map or crash the whole BEAM process
%% (taking the eunit run down with it, not just failing an assertion); a
%% clean, repeatable pass here is the regression signal.

-define(NUM_WORKERS, 32).

%% Runs Fun(Item) in its own process for every Item, in parallel, and
%% collects the results back in the original order. A Fun that throws
%% reports {error, Class, Reason} for that item instead of crashing the
%% test process (or, under the old code, the whole VM).
pmap(Fun, Items) ->
    Parent = self(),
    Tagged = [{make_ref(), Item} || Item <- Items],
    lists:foreach(
        fun({Ref, Item}) ->
            spawn(fun() ->
                Result =
                    try
                        {ok, Fun(Item)}
                    catch
                        Class:Reason -> {error, Class, Reason}
                    end,
                Parent ! {Ref, Result}
            end)
        end,
        Tagged
    ),
    [receive {Ref, Result} -> Result end || {Ref, _Item} <- Tagged].

%% Copies test/single_int.avsc content to N distinct temp filenames so each
%% one is guaranteed to be a brand-new key in erlav_schema_map (the map is
%% keyed by the filename binary passed to erlav_init/1, not by schema
%% content) -- concurrently erlav_init-ing these is exactly the concurrent
%% insert pattern that used to race.
make_temp_schemas(N) ->
    {ok, Content} = file:read_file("test/single_int.avsc"),
    Unique = erlang:unique_integer([positive]),
    Files = [
        lists:flatten(
            io_lib:format("test/tmp_concurrency_~p_~p.avsc", [Unique, I])
        )
        || I <- lists:seq(1, N)
    ],
    [ok = file:write_file(F, Content) || F <- Files],
    Files.

cleanup_temp_schemas(Files) ->
    lists:foreach(fun(F) -> file:delete(F) end, Files).

%% --- Concurrent erlav_init/1 on distinct new schemas (concurrent writers) ---

concurrent_init_distinct_schemas_test_() ->
    {timeout, 30, fun() ->
        Files = make_temp_schemas(?NUM_WORKERS),
        try
            Results = pmap(
                fun(F) -> erlav_nif:erlav_init(list_to_binary(F)) end,
                Files
            ),
            %% No worker crashed or threw.
            ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)),
            Ids = [Id || {ok, Id} <- Results],
            %% Every id is a real schema id (erlav_init_nif's failure
            %% sentinel is 0; real ids start at 1).
            ?assert(lists:all(fun(Id) -> is_integer(Id) andalso Id > 0 end, Ids)),
            %% Every file was a brand-new key, so every id must be unique --
            %% a corrupted map under concurrent insert would manifest as
            %% duplicate/dropped ids here.
            ?assertEqual(length(Ids), length(lists:usort(Ids))),
            ?assertEqual(?NUM_WORKERS, length(Ids)),
            %% Spot-check that the registrations are actually usable, not
            %% just non-crashing.
            lists:foreach(
                fun(Id) ->
                    Encoded = erlav_nif:erlav_encode(Id, #{<<"f">> => 42}),
                    Decoded = erlav_nif:erlav_decode_fast(Id, Encoded),
                    ?assertEqual(42, maps:get(<<"f">>, Decoded))
                end,
                Ids
            )
        after
            cleanup_temp_schemas(Files)
        end
    end}.

%% --- Concurrent erlav_init/1 on the *same* new schema (idempotency under a race) ---

concurrent_init_same_schema_is_idempotent_test_() ->
    {timeout, 30, fun() ->
        [File] = make_temp_schemas(1),
        try
            Bin = list_to_binary(File),
            Results = pmap(fun(_) -> erlav_nif:erlav_init(Bin) end,
                            lists:seq(1, ?NUM_WORKERS)),
            ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)),
            Ids = [Id || {ok, Id} <- Results],
            %% Whichever worker's insert "wins" the race, every concurrent
            %% caller must observe the same single id for the same schema
            %% file -- not a mix of ids from a corrupted/duplicated insert.
            ?assertEqual(1, length(lists:usort(Ids)))
        after
            cleanup_temp_schemas([File])
        end
    end}.

%% --- Concurrent encode/decode against an already-registered schema (concurrent readers) ---

concurrent_encode_decode_same_schema_test_() ->
    {timeout, 30, fun() ->
        SchemaId = erlav_nif:erlav_init(<<"test/single_long.avsc">>),
        Values = lists:seq(1, ?NUM_WORKERS),
        Results = pmap(
            fun(V) ->
                Term = #{<<"f">> => V},
                Encoded = erlav_nif:erlav_encode(SchemaId, Term),
                Decoded = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
                maps:get(<<"f">>, Decoded)
            end,
            Values
        ),
        ?assertEqual(
            [{ok, V} || V <- Values],
            Results
        )
    end}.

%% --- Mixed workload: concurrent init of new schemas + encode/decode of an
%% existing schema + encode calls with an invalid id, all at once. This is
%% the pattern most likely to have corrupted the map or crashed the whole
%% NIF under the old unsynchronized code: a writer (init) and readers
%% (encode/decode, including ones that hit the "unknown id" badarg path)
%% hitting erlav_encoders_map/erlav_schema_map simultaneously.

concurrent_mixed_init_encode_decode_test_() ->
    {timeout, 30, fun() ->
        KnownSchemaId = erlav_nif:erlav_init(<<"test/single_double.avsc">>),
        NewFiles = make_temp_schemas(?NUM_WORKERS),
        try
            InitJobs = [{init, F} || F <- NewFiles],
            EncodeJobs = [{encode, V} || V <- lists:seq(1, ?NUM_WORKERS)],
            BadJobs = [{bad, N} || N <- lists:seq(1, ?NUM_WORKERS)],
            Jobs = InitJobs ++ EncodeJobs ++ BadJobs,
            Results = pmap(
                fun
                    ({init, F}) ->
                        erlav_nif:erlav_init(list_to_binary(F));
                    ({encode, V}) ->
                        Term = #{<<"f">> => V * 1.5},
                        Encoded = erlav_nif:erlav_encode(KnownSchemaId, Term),
                        Decoded = erlav_nif:erlav_decode_fast(KnownSchemaId, Encoded),
                        abs(maps:get(<<"f">>, Decoded) - V * 1.5) < 0.00001;
                    ({bad, N}) ->
                        %% Deliberately unknown id: must fail with badarg,
                        %% not corrupt the map out from under the other
                        %% concurrent init/encode jobs.
                        erlav_nif:erlav_encode(1000000000 + N, #{<<"f">> => 1})
                end,
                Jobs
            ),
            %% init/encode jobs succeed with the expected shape...
            {InitResults, Rest1} = lists:split(length(InitJobs), Results),
            {EncodeResults, BadResults} = lists:split(length(EncodeJobs), Rest1),
            ?assert(lists:all(fun({ok, Id}) -> is_integer(Id) andalso Id > 0 end,
                               InitResults)),
            ?assertEqual(lists:duplicate(length(EncodeJobs), {ok, true}), EncodeResults),
            %% ...and the deliberately-bad ids consistently raise badarg,
            %% same as they do outside a concurrent workload.
            ?assert(
                lists:all(
                    fun({error, error, badarg}) -> true; (_) -> false end,
                    BadResults
                )
            )
        after
            cleanup_temp_schemas(NewFiles)
        end
    end}.
