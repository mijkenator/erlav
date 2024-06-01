erlav
=====

High performance avro encoding NIF-based library

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

| Test name          | erlavro time | erlav time |
|--------------------|-----------------|---------------|
| erlav_perf_tst2    | 746.0727 | 142.3787 |
| erlav_perf_tst3 | 1591.4327 | 250.2485 |
| map_perf_tst1 | 1069.9354 |  331.2828  |
| map_perf_tst2 | 217.3632 | 70.7547  |
| array_int_perf_tst |  20.8804 |  3.9581 |
| array_str_perf_tst |  152.1964 | 33.9999 |
| array_map_perf_tst |  363.2405 |  120.0964 |


