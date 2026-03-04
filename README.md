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
| erlav_perf_tst2        | true    |       857.25 |        94.21 |    9.10x |
| erlav_perf_tst3        | true    |      1798.55 |       177.38 |   10.14x |
| map_perf_tst1          | true    |       842.50 |       222.81 |    3.78x |
| map_perf_tst2          | true    |       217.18 |        46.58 |    4.66x |
| array_int_perf_tst     | true    |        34.83 |         3.81 |    9.13x |
| array_str_perf_tst     | true    |      4012.83 |      1028.46 |    3.90x |
| array_map_perf_tst     | true    |       448.40 |       105.78 |    4.24x |
+------------------------+---------+--------------+--------------+----------+

```
