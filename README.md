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
| erlav_perf_tst2        | true    |       861.20 |        69.90 |   12.32x |
| erlav_perf_tst3        | true    |      1778.01 |       143.50 |   12.39x |
| map_perf_tst1          | true    |       149.29 |        25.80 |    5.79x |
| map_perf_tst2          | true    |       181.71 |        54.56 |    3.33x |
| array_int_perf_tst     | true    |        23.29 |         3.74 |    6.23x |
| array_str_perf_tst     | true    |       171.41 |        29.74 |    5.76x |
| array_map_perf_tst     | true    |       443.22 |       143.79 |    3.08x |
+------------------------+---------+--------------+--------------+----------+

```
