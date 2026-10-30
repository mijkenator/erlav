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

### Negative block-count encoding

Avro allows array/map blocks to be encoded either as a plain positive item
count, or as a negative count followed by the block's byte length (used by
readers to skip a block without decoding it). `erlav_decode`/
`erlav_decode_fast` always understand both forms, so bytes produced by other
Avro implementations (e.g. `erlavro`, which emits the negative form by
default) decode correctly out of the box.

`erlav_encode` emits the plain positive-count form by default. To make it
emit the negative form instead -- e.g. to match `erlavro`'s byte output, or
for interop with a reader that expects it -- pass
`use_negative_block_count` when initializing the schema:

```erlang
SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>, [use_negative_block_count]),

% Every array/map field encoded with this SchemaId now uses the negative
% block-count form; decoding is unaffected either way.
Ret = erlav_nif:erlav_encode(SchemaId, Term),
DecodedTerm = erlav_nif:erlav_decode_fast(SchemaId, Ret)
```

Plain `erlav_init/1` (no options) keeps emitting today's positive-count
form, unchanged.

### Decoding long strings without copying: `decode_sub_binaries`

By default `erlav_decode_fast/2` copies every decoded string and `bytes` value
into a new binary. For values of 65 bytes or more the BEAM has to allocate a
reference-counted binary for each copy, which is most of the decode time for
string-heavy messages. Pass `decode_sub_binaries` to return those values as
sub-binaries of the input instead (no copy, no allocation):

```erlang
SchemaId = erlav_nif:erlav_init(<<"priv/tschema2.avsc">>, [decode_sub_binaries]),
Decoded = erlav_nif:erlav_decode_fast(SchemaId, Binary)
```

Measured on `array<string>` (100 strings per array): strings of 65 bytes or
more decode about 4-5x faster (136 bytes: ~17 -> ~4 us; 2000 bytes: ~25 -> ~5 us),
and the decode time stays flat as strings get longer. Strings under 65 bytes are
unchanged, because they are still copied (a heap binary is cheaper than a
sub-binary reference). This is close to a best case, since the benchmark is
almost pure string decoding; messages that mix many other field types gain
less. The decoded values compare equal either way.

**The trade-off: memory.** A sub-binary keeps the *entire input binary* alive for
as long as any decoded value is referenced. If you keep one 136-byte string out
of a 1.4 MB message, the process holds on to the whole 1.4 MB until that string
is dropped. If your code stores slices of decoded messages long-term (a cache,
a long-lived process state), either leave this option off or copy what you keep
with `binary:copy/1`. (`erlavro`'s decoder behaves like the option: it returns
sub-binaries.) It is off by default for this reason.

Options are part of a schema's identity: the same file initialised with
different options gets a different `SchemaId`. `decode_sub_binaries` affects
`erlav_decode_fast/2` only; the legacy `erlav_decode/2` does not decode strings.


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
