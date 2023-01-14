erlav
=====

An OTP library

Build
-----

    $ rebar3 compile

    valgrind --tool=callgrind --callgrind-out-file=valg/file.out $ERL_TOP/bin/cerl -valgrind -pa _build/default/lib/*/ebin
