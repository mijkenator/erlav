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

Measured on `array<string>` (100 strings per array), time per
`erlav_decode_fast/2` call (range over two runs):

| String length | Copy (default) | `decode_sub_binaries` | Speedup |
|---|---|---|---|
| 32 B | 4.7-5.2 us | 4.8-5.4 us | none (both copy) |
| 64 B | 6.6-6.8 us | 6.7-7.0 us | none (both copy) |
| 65 B | 16-17 us | 4.0-4.2 us | 3.9-4.2x |
| 136 B | 16.5-18.6 us | ~3.9 us | 4.3-4.7x |
| 500 B | 17.5-19 us | ~4.0 us | 4.4-4.8x |
| 2000 B | 25-26 us | 4.7 us | 5.3-5.7x |

Strings of 65 bytes or more decode about 4-5x faster, and the decode time stays
flat as strings get longer. Strings under 65 bytes are unchanged, because they
are still copied (a heap binary is cheaper than a sub-binary reference). This
is close to a best case, since the benchmark is almost pure string decoding;
messages that mix many other field types gain less. The decoded values compare
equal either way.

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

### Decoding only some fields: `erlav_projection` / `erlav_decode_projected`

If you read a handful of fields out of a large record, building the whole Erlang
map is wasted work. Compile a *projection* once, then decode with it:

```erlang
SchemaId = erlav_nif:erlav_init(<<"test/opnrtb_test1.avsc">>),
{ok, ProjId} = erlav_nif:erlav_projection(SchemaId, [
    <<"event">>,                                      % the field's whole value
    {<<"bid_request">>, [                             % only these sub-fields
        {<<"imp">>, [{<<"banner">>, [<<"w">>]}]},     % of every element of an array of records
        {<<"user">>, [<<"id">>]}
    ]}
]),
#{<<"event">> := _, <<"bid_request">> := #{<<"imp">> := [#{<<"banner">> := #{<<"w">> := _}} | _]}} =
    erlav_nif:erlav_decode_projected(ProjId, Binary)
```

A spec is a list of `<<"field">>` (the whole value) and `{<<"field">>, SubSpec}`
(only the listed fields of a record, of every record in an array of records, or
of a nullable one of those). The result is a map with exactly the selected keys,
equal to picking those keys out of `erlav_decode_fast/2`'s result, including
`undefined` for a null record or an absent nullable field. A projection belongs
to one `SchemaId`; compiling the same selection twice returns the same
`ProjId`.

`erlav_projection/2` checks the spec against the schema and returns
`{error, Message, Code}` for an unknown field (112), a field listed twice (113),
a sub-spec on something that is not a record, an array of records or a nullable
one of those (115, e.g. a scalar, a map, a multi-member union), an empty
sub-spec (116), or a malformed spec (110, 111).

Avro records carry no field offsets, so a projection cannot jump to a field. It
*skips* the fields it does not want (walking their bytes, building no terms) and
*stops* after the last one it wants. Arrays and maps written with negative block
counts (`use_negative_block_count`, and what `erlavro` emits) are skipped in one
step. Measured on the 900-byte OpenRTB fixture (full `erlav_decode_fast/2`:
~41 us):

| Selection | Projected decode | Speedup |
|---|---|---|
| 1 early top-level field | ~1.3 us | ~30x |
| `bid_request.id` | ~1.4-1.7 us | ~25-30x |
| `event` + `bid_request.imp[].banner.w` + `bid_request.user.id` | ~3.0-3.6 us | ~11-14x |
| 3 spread top-level fields, the last one near the end | 3.5-5.3 us | 8-12x |
| 1 field near the end | 3.2-5.1 us | 8-13x |
| all 13 top-level fields | ~41-42 us | none (same work as a full decode) |

(Ranges cover two runs, with and without negative block counts; the
negative-count encoding skips large arrays/maps faster.) The savings come from
not building terms, so they are biggest on wide records with large unwanted
parts and smallest when you select most of the record or skip only small
scalars. Measure on your own schema.

**Differences from a full decode to be aware of:**

- Bytes after the last selected field are **not validated**. A truncated or
  corrupt tail that `erlav_decode_fast/2` rejects can still decode successfully
  here. Everything up to and including the last selected field is checked, and
  skipped fields are checked as far as skipping them needs (a bad length, a
  truncated value), not fully.
- Strings and `bytes` are always copied, even if the schema was initialised with
  `decode_sub_binaries`: a projection usually keeps a few fields of a large
  message, and a sub-binary would keep the whole input alive for them.

The design is in [docs/design-partial-decode.md](docs/design-partial-decode.md).


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
