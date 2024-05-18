-module(encoder_decoder).

-include_lib("eunit/include/eunit.hrl").

main_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
    {ok, Terms} = file:consult("test/opnrtb_test1.data"),

    lists:foreach(fun(Term1) -> 
        ?debugFmt("=============== struct : ===================  ~n", []),
        ?debugFmt("~p ~n", [Term1]),
        Encoded = erlav_nif:erlav_safe_encode(SchemaId, Term1),
        M = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
        tst_utils:compare_maps_extra_fields(Term1, M),
        ?debugFmt("=============== /struct  ===================  ~n~n", [])
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

