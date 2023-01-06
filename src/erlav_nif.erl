-module(erlav_nif).

-export([add/2, encode/2, etest/0, etest/1]).

-nifs([add/2, encode/2]).

-on_load(init/0).

-define(APPNAME, erlav_nif).
-define(LIBNAME, erlav_nif).

init() ->
    SoName =
        case code:priv_dir(?APPNAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, ?LIBNAME]);
                    _ ->
                        filename:join([priv, ?LIBNAME])
                end;
            Dir ->
                filename:join(Dir, ?LIBNAME)
        end,
    erlang:load_nif(SoName, 0).

add(_A, _B) ->
    not_loaded(?LINE).

encode(_A, _B) ->
    not_loaded(?LINE).
    
not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

etest() ->
    {ok, SchemaJSON} = file:read_file("priv/tschema2.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    Term = #{
        <<"intField">> => 1,
        <<"longField">> => 2
    },
    iolist_to_binary(Encoder(Term)).

etest(2) ->
    Term = #{
        <<"intField">> => 1,
        <<"longField">> => 2
    },
    encode(7, Term);
etest(1) ->
    EA1 = [iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"int">>,[]},X})) 
           ||  X <- lists:seq(1,10)],
    io:format("Erlavro INT ~p ~n", [EA1]),
    EA2 = [ encode(1, X) || X <- lists:seq(1,10)],
    io:format("Erlav INT ~p ~n", [EA2]),
    EA3 = [iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"long">>,[]},X})) 
           ||  X <- lists:seq(9223372036854775707, 9223372036854775717)],
    io:format("Erlavro LONG ~p ~n", [EA3]),
    EA4 = [ encode(2, X) || X <- lists:seq(9223372036854775707, 9223372036854775717)],
    io:format("Erlav LONG ~p ~n", [EA4]),


    EA5 = [iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"float">>,[]},X})) 
           ||  X <- [1.0, 1.234, 3.456, 567.89] ],
    io:format("Erlavro Float ~p ~n", [EA5]),
    EA6 = [encode(3, X) ||  X <- [1.0, 1.234, 3.456, 567.89] ],
    io:format("Erlav Float ~p ~n", [EA6]),
    
    EA7 = [iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"double">>,[]},X})) 
           ||  X <- [1.0, 1.234, 3.456, 567.89] ],
    io:format("Erlavro double ~p ~n", [EA7]),
    EA8 = [encode(4, X) ||  X <- [1.0, 1.234, 3.456, 567.89] ],
    io:format("Erlav double ~p ~n", [EA8]),
    
    EA9 = [iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"boolean">>,[]},X}))
          || X <- [true, false] ],
    io:format("Erlavro boolean ~p ~n", [EA9]),
    EA10 = [encode(5, X) ||  X <- [true, false] ],
    io:format("Erlav boolean ~p ~n", [EA10]),

    S1 = iolist_to_binary(avro_binary_encoder:encode_value({avro_value,{avro_primitive_type,<<"string">>,[]}, <<"qwertyasdfgzxcv">>})),
    io:format("Erlavro string ~p ~n", [S1]),
    S2 = encode(6, <<"qwertyasdfgzxcv">>),
    io:format("Erlav string ~p ~n", [S2]),

    ok.
