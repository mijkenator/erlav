-module(erlav_perf).

-export([ 
    erlav_perf_tst/1, 
    erlav_perf_tst2/1, 
    erlav_perf_tst2/3, 
    erlav_perf_string/2, 
    erlav_perf_integer/2,
    erlav_perf_strings/3,
    map_perf_tst1/3,
    map_perf_tst2/3,
    array_int_perf_tst/3,
    array_str_perf_tst/3,
    array_map_perf_tst/3,
    all_tests/3,
    all_tests/0
]).

erlav_perf_tst(Num) ->
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    L = lists:seq(1, Num),
    {ok, [Term1]} = file:consult("test/opnrtb_perf.data"),

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(_) ->
        iolist_to_binary(Encoder(Term1))
    end, L),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),


    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(_) ->
        erlav_nif:erlav_encode(SchemaId1, Term1)
    end, L),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav2 encoding time: ~p microseconds ~n", [T3]),

    {Total1/Num, Total3/Num}.

erlav_perf_tst2(Num, _, _) -> erlav_perf_tst2(Num).
erlav_perf_tst2(Num) ->
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    {ok, [Term1]} = file:consult("test/opnrtb_perf.data"),
    Terms = [ randomize(Term1) || _ <- lists:seq(1, Num)],

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(Term1r) ->
        iolist_to_binary(Encoder(Term1r))
                  end, Terms),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(Term1r) ->
        erlav_nif:erlav_encode(SchemaId1, Term1r)
                  end, Terms),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav2 encoding time: ~p microseconds ~n", [T3]),
    
    {true, Total1/Num, Total3/Num}.

randomize(Map) when is_map(Map) ->
    maps:filtermap(fun
            (_, V) when is_binary(V)  -> {true, base64:encode(crypto:strong_rand_bytes(100))}; 
            (_, V) when is_integer(V) -> {true, rand:uniform(999999)};
            (_, V) when is_map(V)     -> {true, randomize(V)};
            (_, V) when is_list(V)    -> {true, [ randomize(Vi) || Vi <- V]};
            (_, _) -> true
    end, Map);
randomize(S) when is_binary(S) -> base64:encode(crypto:strong_rand_bytes(100));
randomize(I) when is_integer(I) -> rand:uniform(9999999);
randomize(V) -> V.


erlav_perf_string(Num, StrLen) ->
    {ok, SchemaJSON1} = file:read_file("test/string.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(<<"test/string.avsc">>),

    [St1 | _] = Strings = [ base64:encode(crypto:strong_rand_bytes(StrLen)) || _ <- lists:seq(1, Num)],

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(S) ->
        iolist_to_binary(Encoder(#{ <<"stringField">> => S }))
    end, Strings),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(S) ->
        erlav_nif:erlav_encode(SchemaId1, #{ <<"stringField">> => S })
    end, Strings),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav2 encoding time: ~p microseconds ~n", [T3]),
    
    TestMap = #{ <<"stringField">> => St1 },
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = RetAvro1 =:= RetAvro2,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [IsSame, RetAvro2, RetAvro1]),

    {IsSame, Total1/Num, Total3/Num}.

erlav_perf_integer(Num, Type) ->
    Schema = case Type of
        null -> "test/integer.avsc";
        _ -> "test/integer_null.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),

    Ints = [ [rand:uniform(9999999) || _ <- lists:seq(1,6) ] || _ <- lists:seq(1, Num)],

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun([I1, I2, I3, I4, I5, I6]) ->
        iolist_to_binary(Encoder(#{ 
            <<"intField1">> => I1, 
            <<"intField2">> => I2, 
            <<"intField3">> => I3, 
            <<"intField4">> => I4, 
            <<"intField5">> => I5, 
            <<"intField6">> => I6
        }))
    end, Ints),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun([I1, I2, I3, I4, I5, I6]) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"intField1">> => I1, 
            <<"intField2">> => I2, 
            <<"intField3">> => I3, 
            <<"intField4">> => I4, 
            <<"intField5">> => I5, 
            <<"intField6">> => I6
        })
    end, Ints),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    {Total1/Num,  Total3/Num}.

erlav_perf_strings(Num, StrLen, Type) ->
    Schema = case Type of
        null -> "test/perfstr.avsc";
        _ -> "test/perfstr_null.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),

    Strings = [ [base64:encode(crypto:strong_rand_bytes(StrLen)) || _ <- lists:seq(1, 6)] || _ <- lists:seq(1, Num)],

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun([S1,S2,S3,S4,S5,S6]) ->
        iolist_to_binary(Encoder(#{ 
            <<"stringField1">> => S1,
            <<"stringField2">> => S2,
            <<"stringField3">> => S3,
            <<"stringField4">> => S4,
            <<"stringField5">> => S5,
            <<"stringField6">> => S6
        }))
    end, Strings),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun([S1,S2,S3,S4,S5,S6]) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"stringField1">> => S1,
            <<"stringField2">> => S2,
            <<"stringField3">> => S3,
            <<"stringField4">> => S4,
            <<"stringField5">> => S5,
            <<"stringField6">> => S6
        })
    end, Strings),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    [ [ St1, St2, St3, St4, St5, St6 ] | _ ] = Strings,
    TestMap = #{ 
        <<"stringField1">> => St1,
        <<"stringField2">> => St2,
        <<"stringField3">> => St3,
        <<"stringField4">> => St4,
        <<"stringField5">> => St5,
        <<"stringField6">> => St6
    },
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = RetAvro1 =:= RetAvro2,
    io:format("Test Term: ~p ~n", [TestMap]),
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/Num, Total3/Num}.


% map of ints perf test
map_perf_tst1(NumIterations, _StrLen, IsNullable) ->
    Schema = case IsNullable of
        null -> "test/map_int_null.avsc";
        _    -> "test/tschema_map.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),
    
    Ints = [ [rand:uniform(9999999) || _ <- lists:seq(1,100) ] || _ <- lists:seq(1, NumIterations)],
    Strings = [ [base64:encode(crypto:strong_rand_bytes(20)) || _ <- lists:seq(1, 100)] || _ <- lists:seq(1, NumIterations)],
    Maps = [ maps:from_list(lists:zip(Keys, Vals)) || {Keys, Vals} <- lists:zip(Strings, Ints)],
    [ Map1 | _ ] = Maps,


    io:format("Started .....~p ~n", [Map1]),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        iolist_to_binary(Encoder(#{ 
            <<"mapField">> => M1
        }))
    end, Maps),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"mapField">> => M1
        })
    end, Maps),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    TestMap = #{ 
        <<"mapField">> => Map1
    },
    io:format("Test Term: ~p ~n", [TestMap]),
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = case RetAvro1 of
        <<>> -> false;
        _ ->
            RetMap = tst_utils:to_map(Decoder(RetAvro1)),
            tst_utils:compare_maps(TestMap, RetMap)
    end,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/NumIterations, Total3/NumIterations}.

% map of strings perf test
map_perf_tst2(NumIterations, _StrLen, IsNullable) ->
    Schema = case IsNullable of
        null -> "test/map_str_null.avsc";
        _    -> "test/tschema_map_str.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),
    
    StringKeys = [ [base64:encode(crypto:strong_rand_bytes(20)) || _ <- lists:seq(1, 100)] || _ <- lists:seq(1, NumIterations)],
    StringVals = [ [base64:encode(crypto:strong_rand_bytes(100)) || _ <- lists:seq(1, 100)] || _ <- lists:seq(1, NumIterations)],
    Maps = [ maps:from_list(lists:zip(Keys, Vals)) || {Keys, Vals} <- lists:zip(StringKeys, StringVals)],
    [ Map1 | _ ] = Maps,


    io:format("Started .....~p ~n", [Map1]),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        iolist_to_binary(Encoder(#{ 
            <<"mapField">> => M1
        }))
    end, Maps),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"mapField">> => M1
        })
    end, Maps),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    TestMap = #{ 
        <<"mapField">> => Map1
    },
    io:format("Test Term: ~p ~n", [TestMap]),
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = case RetAvro1 of
        <<>> -> false;
        _ ->
            RetMap = tst_utils:to_map(Decoder(RetAvro1)),
            tst_utils:compare_maps(TestMap, RetMap)
    end,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/NumIterations, Total3/NumIterations}.

% array of int perf test
array_int_perf_tst(NumIterations, _StrLen, IsNullable) ->
    Schema = case IsNullable of
        null -> "test/tschema_array_null.avsc";
        _    -> "test/tschema_array.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),
    
    Arrays = [ [rand:uniform(9999999) || _ <- lists:seq(1,100) ] || _ <- lists:seq(1, NumIterations)],
    [ Arr1 | _ ] = Arrays,


    io:format("Started .....~p ~n", [Arr1]),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        iolist_to_binary(Encoder(#{ 
            <<"arrayField">> => M1
        }))
    end, Arrays),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"arrayField">> => M1
        })
    end, Arrays),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    TestMap = #{ 
        <<"arrayField">> => Arr1
    },
    io:format("Test Term: ~p ~n", [TestMap]),
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = case RetAvro1 of
        <<>> -> false;
        _ ->
            RetMap = tst_utils:to_map(Decoder(RetAvro1)),
            tst_utils:compare_maps(TestMap, RetMap)
    end,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/NumIterations, Total3/NumIterations}.

% array of strings perf test
array_str_perf_tst(NumIterations, _StrLen, IsNullable) ->
    Schema = case IsNullable of
        null -> "test/tschema_array_null_str.avsc";
        _    -> "test/tschema_array_str.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),
    
    Arrays = [ [ base64:encode(crypto:strong_rand_bytes(100)) || _ <- lists:seq(1,100) ] || _ <- lists:seq(1, NumIterations)],
    [ Arr1 | _ ] = Arrays,

    io:format("Started .....~p ~n", [Arr1]),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        iolist_to_binary(Encoder(#{ 
            <<"arrayField">> => M1
        }))
    end, Arrays),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"arrayField">> => M1
        })
    end, Arrays),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    TestMap = #{ 
        <<"arrayField">> => Arr1
    },
    io:format("Test Term: ~p ~n", [TestMap]),
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = case RetAvro1 of
        <<>> -> false;
        _ ->
            RetMap = tst_utils:to_map(Decoder(RetAvro1)),
            tst_utils:compare_maps(TestMap, RetMap)
    end,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/NumIterations,  Total3/NumIterations}.

% array of maps perf test
array_map_perf_tst(NumIterations, _StrLen, IsNullable) ->
    Schema = case IsNullable of
        null -> "test/array_map.avsc";
        _    -> "test/array_map.avsc"
    end,
    {ok, SchemaJSON1} = file:read_file(Schema),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId1 = erlav_nif:erlav_init(list_to_binary(Schema)),
    
    MakeMap = fun() ->
      Keys = [base64:encode(crypto:strong_rand_bytes(20)) || _ <- lists:seq(1, 20)],
      Vals = [base64:encode(crypto:strong_rand_bytes(200)) || _ <- lists:seq(1, 20)],
      maps:from_list(lists:zip(Keys, Vals))
    end,

    Arrays = [ [ MakeMap() || _ <- lists:seq(1,10) ] || _ <- lists:seq(1, NumIterations)],
    [ Arr1 | _ ] = Arrays,


    io:format("Started .....~p ~n", [Arr1]),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        iolist_to_binary(Encoder(#{ 
            <<"arrayField">> => M1
        }))
    end, Arrays),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T3 = erlang:system_time(microsecond),
    lists:foreach(fun(M1) ->
        erlav_nif:erlav_encode(SchemaId1, #{ 
            <<"arrayField">> => M1
        })
    end, Arrays),
    Total3 = erlang:system_time(microsecond) - T3,
    io:format("Erlav encoding time: ~p microseconds ~n", [T3]),

    TestMap = #{ 
        <<"arrayField">> => Arr1
    },
    io:format("Test Term: ~p ~n", [TestMap]),
    RetAvro1 = erlav_nif:erlav_encode(SchemaId1, TestMap),
    RetAvro2 = iolist_to_binary(Encoder(TestMap)),
    IsSame = case RetAvro1 of
        <<>> -> false;
        _ ->
            RetMap = tst_utils:to_map(Decoder(RetAvro1)),
            tst_utils:compare_maps(TestMap, RetMap)
    end,
    io:format("Same ret: ~p ~n ~p ~n ~p ~n", [RetAvro2, RetAvro1, IsSame]),

    {IsSame, Total1/NumIterations, Total3/NumIterations}.
% array of arrays
% map for arrays

% all perf test run
all_tests() -> all_tests(10000, 50, null).

all_tests(NumIterations, StrLen, IsNullable) ->
    Funs = [erlav_perf_tst2, map_perf_tst1, map_perf_tst2, array_int_perf_tst, array_str_perf_tst, array_map_perf_tst],
    Report = lists:map(fun(FName) -> 
        {IsSame, ErlTime, CppTime2} = apply(erlav_perf, FName, [NumIterations, StrLen, IsNullable]),
        io:format("~p, equal: ~p, erltime: ~p, cpptime2: ~p ~n~n", [FName, IsSame, ErlTime,  CppTime2]),
        {IsSame, ErlTime,  CppTime2}
    end, Funs),
    Ret = lists:zip(Funs, Report),
    lists:foreach(fun({Test, {IsSameR, ErlTimeR,  CppTime2R}}) ->
        io:format("~p, equal: ~p, erltime: ~p,  cpptime2: ~p  ~n~n", [Test, IsSameR, ErlTimeR,  CppTime2R])
    end, Ret).

