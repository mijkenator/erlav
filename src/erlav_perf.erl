-module(erlav_perf).

-export([ erlav_perf_tst/1 ]).

erlav_perf_tst(Num) ->
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Encoder  = avro:make_simple_encoder(SchemaJSON1, []),
    _Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    SchemaId = erlav_nif:create_encoder(<<"test/opnrtb_test1.avsc">>),
    L = lists:seq(1, Num),
    {ok, [Term1]} = file:consult("test/opnrtb_perf.data"),

    io:format("Started ..... ~n", []),

    T1 = erlang:system_time(microsecond),
    lists:foreach(fun(_) ->
        iolist_to_binary(Encoder(Term1))
    end, L),
    Total1 = erlang:system_time(microsecond) - T1,
    io:format("Erlavro encoding time: ~p microseconds ~n", [T1]),

    T2 = erlang:system_time(microsecond),

    lists:foreach(fun(_) ->
        erlav_nif:do_encode(SchemaId, Term1)
    end, L),
    Total2 = erlang:system_time(microsecond) - T2,
    io:format("Erlav encoding time: ~p microseconds ~n", [T2]),

    {Total1/Num, Total2/Num}.
