-module(varint_test).

-include_lib("eunit/include/eunit.hrl").


integer_test() ->
    % Re1 = erlav_nif:int_decode(<<180,141,172,122,2,226,18,228,40>>),
    Re1 = erlav_nif:int_decode(<<218,134,150,61,1,177,9,178,20>>),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    % expected [128287578,1,1201,2610]
    
    Re2 = erlav_nif:int_encode([128287578,1,1201,2610]),
    ?debugFmt("encode result: ~p ~n", [Re2]),
    ok.


