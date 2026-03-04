erlav
=====

High performance avro encoding/decoding NIF-based library

Build
-----

    $ rebar3 compile
    $ rebar3 eunit

Usage
-----

```erlang
SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>),
Term = #{
        <<"intField">> => 789,
        <<"longField">> => 2989898111,
        <<"doubleField">> => 11.2345,
        <<"floatField">> => 23.12,
        <<"boolField">> => true,
        <<"stringField">> => <<"asdadasdasdasd3453534dfgdgd123456789">>,
        <<"bytesField">> => <<1,99,57,127,0,56>>
},

% Encode Term to avro format
Ret = erlav_nif:erlav_encode(SchemaId, Term),

% Decode data in avro format to erlang term
DecodedTerm = erlav_nif:erlav_decode_fast(SchemaId, Ret)

```


Performance
-----

run erlang shell

```bash
erl -pa _build/default/lib/*/ebin
```

run performance test ( it will generate report at the end )

```erlang
erlav_perf:all_tests().
```

```bash

+------------------------+---------+--------------+--------------+----------+
| Test                   | Equal   | Erlavro us   | Erlav us     | Speedup  |
+------------------------+---------+--------------+--------------+----------+
| erlav_perf_tst2        | true    |       846.50 |        94.80 |    8.93x |
| erlav_perf_tst3        | true    |      1803.85 |       172.40 |   10.46x |
| map_perf_tst1          | true    |       688.17 |       231.59 |    2.97x |
| map_perf_tst2          | true    |       207.22 |        45.85 |    4.52x |
| array_int_perf_tst     | true    |        23.54 |         4.10 |    5.74x |
| array_str_perf_tst     | true    |       181.06 |        31.90 |    5.68x |
| array_map_perf_tst     | true    |       512.89 |       119.24 |    4.30x |
+------------------------+---------+--------------+--------------+----------+
```
