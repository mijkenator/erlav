-module(encoder_decoder).

-include_lib("eunit/include/eunit.hrl").

main_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    {ok, Terms} = file:consult("test/opnrtb_test1.data"),

    lists:foreach(fun(Term1) -> 
        %?debugFmt("=============== struct : ===================  ~n", []),
        %?debugFmt("~p ~n", [Term1]),
        Event = maps:get(<<"event">>, Term1),
        case erlav_nif:erlav_safe_encode(SchemaId, Term1) of
            {error, _,_} = Error ->
                ?debugFmt("~p -> encode error  ~p ~n", [Event, Error]),
                ?assert(true == false);
            Encoded ->
                %Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term1),
                M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
                tst_utils:compare_maps_extra_fields(Term1, M),
                ?debugFmt("~p -> encode ok  ~n", [Event])
        end
        % ?debugFmt("=============== /struct  ===================  ~n~n", [])
    end, Terms).

dec_enc_test() ->
    ?debugFmt("=============== openRTB test 1 ===================  ~n ~n", []),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    ?debugFmt("=============== openRTB test 1 :1: ===================  ~n ~n", []),
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    ?debugFmt("=============== openRTB test 1 :2: ===================  ~n ~n", []),
    {ok, Terms} = file:consult("test/opnrtb_test1.data"),

    lists:foreach(fun(Term1) -> 
        Event = maps:get(<<"event">>, Term1),
        case erlav_nif:erlav_safe_encode(SchemaId, Term1) of
            {error, _,_} = Error ->
                ?debugFmt("~p -> encode error  ~p ~n", [Event, Error]),
                ?assert(true == false);
            Re2 ->
                M = tst_utils:to_map(Decoder(Re2)),
                tst_utils:compare_maps_extra_fields(Term1, M),
                ?debugFmt("~p -> encode ok  ~n", [Event])
        end
    end, Terms).

ev7_debug_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    Term = #{
                <<"event">> => <<"event7">>,
                <<"sampling">> => <<"sampling3">>,
                <<"timestamp">> => 111.222,
                <<"seller_request_cpm">> => 1111,
                <<"seller_request_fee_cpm">> => 2222,
                <<"seller_trader_fee_cpm">> => 3333,
                <<"maxtime">> => 4444,
                <<"filtered_app_compatible_result">> => #{
                    <<"facr_key2">> => [777777, 888],
                    <<"facr_key1">> => [55555, 66666]
                },
                <<"filtered_ssp_result">> => #{
                    <<"fsr_key2">> => [3333, 44444],
                    <<"fsr_key1">> => [1111, 22222]
                }
            },
    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded: ~p ~n", [M]),
    tst_utils:compare_maps(Term, M),
    ok.

ev16_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    {ok, Terms} = file:consult("test/opnrtb_test1.data"),
    {ok, SchemaJSON1} = file:read_file("test/opnrtb_test1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = lists:nth(16, Terms),

    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),

    M0 = tst_utils:to_map(Decoder(Encoded)),
    ?debugFmt("Decoded erlavro: ~p ~n", [M0]),

    %M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    %?debugFmt("Decoded erlav: ~p ~n", [M]),
    %tst_utils:compare_maps(Term, M),
    ok.

crash_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/ir.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/ir.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
       <<"bidder_bid_price">> => 1000000,
       <<"bidder_id">> => 1,
       <<"buyer_id">> => 187,
       <<"advertiser_id">> => 187,
       <<"creative_group_lookup_id">> => <<"a59984">>,
       <<"dsp_id">> => 1,
       <<"deal_id">> => <<"1">>,
       <<"flight_id">> => 246135
    },

    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),

    M0 = tst_utils:to_map(Decoder(Encoded)),
    ?debugFmt("Decoded erlavro: ~p ~n", [M0]),

    M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded erlav: ~p ~n", [M]),
    %tst_utils:compare_maps(Term, M),
    ok.

crash2_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/ir1.avsc">>),
    {ok, SchemaJSON1} = file:read_file("test/ir1.avsc"),
    Decoder  = avro:make_simple_decoder(SchemaJSON1, []),
    Term = #{
       <<"bidder_bid_price">> => 3,
       <<"creative_group_lookup_id">> => <<"aaaaa">>
    },

    Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),

    M0 = tst_utils:to_map(Decoder(Encoded)),
    ?debugFmt("Decoded erlavro: ~p ~n", [M0]),

    M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("Decoded erlav: ~p ~n", [M]),
    %tst_utils:compare_maps(Term, M),
    ok.
