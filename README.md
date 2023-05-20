erlav
=====

An OTP library

Build
-----

    $ rebar3 compile
    $ rebar3 eunit

    valgrind --tool=callgrind --callgrind-out-file=valg/file.out $ERL_TOP/bin/cerl -valgrind -pa _build/default/lib/*/ebin
