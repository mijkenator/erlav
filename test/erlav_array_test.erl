-module(erlav_array_test).

-include_lib("eunit/include/eunit.hrl").

arrayofrec_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_array_ofrecs.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                                   #{
                                        <<"rec1field">> => 1,
                                        <<"rec2field">> => <<"koko">>,
                                        <<"rec3field">> => 2,
                                        <<"rec4field">> => 112233,
                                        <<"rec5field">> => true,
                                        <<"rec6field">> => 11.22,
                                        <<"rec7field">> => 33.44
                                    }
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_array_ofrecs.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [M1|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"koko">>,  proplists:get_value(<<"rec2field">>, M1)),
    ?assertEqual(112233,  proplists:get_value(<<"rec4field">>, M1)),
    ?assertEqual(true,  proplists:get_value(<<"rec5field">>, M1)),
    ?assertEqual(11.22,  proplists:get_value(<<"rec6field">>, M1)).

arrayofmaps_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_map.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                                   #{
                                        <<"rec1field">> => <<"lalalal">>,
                                        <<"rec2field">> => <<"koko">>
                                    }
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/array_map.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [M1|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"koko">>,  proplists:get_value(<<"rec2field">>, M1)),
    ?assertEqual(<<"lalalal">>,  proplists:get_value(<<"rec1field">>, M1)).

arrayofarrays_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                    [1,2,3], [4,5], [6], [7,8,9,10]
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/array_array.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [A1, A2, A3, A4] = maps:get(<<"arrayField">>, M),
    ?assertEqual([1,2,3], A1),
    ?assertEqual([4,5], A2),
    ?assertEqual([6], A3),
    ?assertEqual([7,8,9,10], A4).

arrayofarrays_null_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_null_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                    [1,2,3], [4,5], [6], [7,8,9,10]
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/array_null_array.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [A1, A2, A3, A4] = maps:get(<<"arrayField">>, M),
    ?assertEqual([1,2,3], A1),
    ?assertEqual([4,5], A2),
    ?assertEqual([6], A3),
    ?assertEqual([7,8,9,10], A4).

array_null_ofmaps_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_map_null.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                                   #{
                                        <<"rec1field">> => <<"lalalal">>,
                                        <<"rec2field">> => <<"koko">>
                                    }
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/array_map_null.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [M1|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"koko">>,  proplists:get_value(<<"rec2field">>, M1)),
    ?assertEqual(<<"lalalal">>,  proplists:get_value(<<"rec1field">>, M1)).

arrayofrec_null_test() ->
    {ok, SchemaJSON1} = file:read_file("test/tschema_array_ofrecs_nullable.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
              <<"arrayField">> => [
                                   #{
                                        <<"rec1field">> => 1112244,
                                        <<"rec3field">> => 244567
                                    },
                                   #{
                                        <<"rec1field">> => 11122442,
                                        <<"rec3field">> => 2445672
                                    },
                                   #{
                                        <<"rec1field">> => 11122443,
                                        <<"rec3field">> => 2445673
                                    }
                ]
    },
    SchemaId = erlav_nif:create_encoder(<<"test/tschema_array_ofrecs_nullable.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [M1,_,M3|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(1112244,  proplists:get_value(<<"rec1field">>, M1)),
    ?assertEqual(244567,  proplists:get_value(<<"rec3field">>, M1)),
    ?assertEqual(11122443,  proplists:get_value(<<"rec1field">>, M3)),
    ?assertEqual(2445673,  proplists:get_value(<<"rec3field">>, M3)).

array_complex1_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_arraymaps.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
             <<"arrayField">> => [
                                  [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}]
                ]
            },
    SchemaId = erlav_nif:create_encoder(<<"test/array_arraymaps.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [[M1, M2]|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"m1value">>,  proplists:get_value(<<"m1key">>, M1)),
    ?assertEqual(<<"m2value">>,  proplists:get_value(<<"m2key">>, M2)).

array_complex2_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_null_arraymaps.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
             <<"arrayField">> => [
                                  [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}]
                ]
            },
    SchemaId = erlav_nif:create_encoder(<<"test/array_null_arraymaps.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [[M1, M2]|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"m1value">>,  proplists:get_value(<<"m1key">>, M1)),
    ?assertEqual(<<"m2value">>,  proplists:get_value(<<"m2key">>, M2)).

array_complex3_test() ->
    {ok, SchemaJSON1} = file:read_file("test/array_null_recursive_array.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term1 = #{
             <<"arrayField">> => [
                                  [
                                    [#{<<"m1key">> => <<"m1value">>}, #{<<"m2key">> => <<"m2value">>}],
                                    [#{<<"m21key">> => <<"m21value">>}, #{<<"m22key">> => <<"m22value">>}]
                                  ]
                ]
            },
    SchemaId = erlav_nif:create_encoder(<<"test/array_null_recursive_array.avsc">>),
    Re2 = erlav_nif:do_encode(SchemaId, Term1),
    M = maps:from_list(Decoder(Re2)),
    [[ [M1, M2], [M21, M22]]|_] = maps:get(<<"arrayField">>, M),
    ?assertEqual(<<"m1value">>,  proplists:get_value(<<"m1key">>, M1)),
    ?assertEqual(<<"m2value">>,  proplists:get_value(<<"m2key">>, M2)),
    ?assertEqual(<<"m21value">>,  proplists:get_value(<<"m21key">>, M21)),
    ?assertEqual(<<"m22value">>,  proplists:get_value(<<"m22key">>, M22)).

