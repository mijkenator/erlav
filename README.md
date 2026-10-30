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

#### Examples

These use `test/projection_types.avsc`; `Bin` is an encoded message whose full
decode (`erlav_decode_fast/2`) contains, among other fields:

```erlang
#{<<"fInt">> => 7,
  <<"fNullRec">> => #{<<"x">> => 3,
                      <<"inner">> => #{<<"y">> => 4, <<"z">> => <<"zz">>},
                      <<"w">> => <<"ww">>},
  <<"fArrRec">> => [#{<<"id">> => 1, <<"tag">> => <<"t1">>},
                    #{<<"id">> => 2, <<"tag">> => <<"t2">>, <<"note">> => <<"n">>}],
  <<"fLast">> => <<"last">>,
  ...}
```

A small helper keeps the examples short:

```erlang
SchemaId = erlav_nif:erlav_init(<<"test/projection_types.avsc">>),
Decode = fun(Spec) ->
    {ok, ProjId} = erlav_nif:erlav_projection(SchemaId, Spec),
    erlav_nif:erlav_decode_projected(ProjId, Bin)
end.
```

(In real code, compile each projection once, at start-up or in a `persistent_term`,
and keep the `ProjId`. Only `erlav_decode_projected/2` belongs on the hot path.)

Top-level fields. Fields you skip are never built, whatever their type:

```erlang
1> Decode([<<"fInt">>, <<"fLast">>]).
#{<<"fInt">> => 7, <<"fLast">> => <<"last">>}
```

A field inside a nested record. A bare name takes the whole value; a tuple takes
only the listed sub-fields, at any depth:

```erlang
2> Decode([{<<"fNullRec">>, [<<"x">>, {<<"inner">>, [<<"z">>]}]}]).
#{<<"fNullRec">> => #{<<"inner">> => #{<<"z">> => <<"zz">>}, <<"x">> => 3}}
```

An array of records. The sub-spec applies to every element, and a nullable field
that is absent comes back as `undefined`, as in a full decode:

```erlang
3> Decode([{<<"fArrRec">>, [<<"id">>, <<"note">>]}]).
#{<<"fArrRec">> => [#{<<"id">> => 1, <<"note">> => undefined},
                    #{<<"id">> => 2, <<"note">> => <<"n">>}]}
```

A null nested record. If the message has `fNullRec` set to null, the whole
sub-spec collapses to `undefined`, as in a full decode:

```erlang
4> {ok, P} = erlav_nif:erlav_projection(SchemaId, [{<<"fNullRec">>, [<<"x">>]}]),
   erlav_nif:erlav_decode_projected(P, BinWithNullRec).
#{<<"fNullRec">> => undefined}
```

The same selection always gives the same id, so compiling twice is harmless:

```erlang
5> {ok, P} =:= erlav_nif:erlav_projection(SchemaId, [{<<"fNullRec">>, [<<"x">>]}]).
true
```

A bad spec fails when you compile it, not on every decode:

```erlang
6> erlav_nif:erlav_projection(SchemaId, [<<"nope">>]).
{error,"erlav_projection: unknown field 'nope' in record 'ProjTypes'",112}

7> erlav_nif:erlav_projection(SchemaId, [{<<"fInt">>, [<<"x">>]}]).
{error,"erlav_projection: field 'fInt' is not a record, an array of records, or a nullable one of those, so it cannot have a sub-spec",115}
```

The result matches a full decode followed by picking the same keys, which is a
handy check when you adopt a projection:

```erlang
Full = erlav_nif:erlav_decode_fast(SchemaId, Bin),
Part = Decode([<<"fInt">>, <<"fLast">>]),
Part = maps:with([<<"fInt">>, <<"fLast">>], Full).
```

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

#### Example: a big schema (OpenRTB), a couple of fields

`test/opnrtb_test1.avsc` is a bid request/response schema with ~425 named
fields once nested records are counted. Real consumers usually want a few of
them: a routing key, an id, a device field. Time per call against a full
`erlav_decode_fast/2`, on two messages: the 900-byte fixture
(`test/opnrtb_perf.data`) and the same message with 25 `imp` entries (about
2.8 KB; the first `imp` repeated with distinct ids, so a stand-in for a larger
real request). Ranges cover two runs.

Messages with **negative block counts** (what `erlavro` writes, or `erlav` with
`use_negative_block_count`):

| What you need | Spec | 900 B message | 25 imps (2.8 KB) |
|---|---|---|---|
| Full decode | (all fields) | ~43 us | ~175 us |
| Routing keys | `[<<"event">>, {<<"bid_request">>, [<<"id">>]}]` | ~1.6 us (**~27x**) | ~1.6 us (**~110x**) |
| Event + device | `[<<"event">>, {<<"bid_request">>, [{<<"device">>, [<<"ip">>, <<"ua">>]}]}]` | ~2.0 us (**~21x**) | ~2.1 us (**~85x**) |
| Three ids | `[{<<"bid_request">>, [{<<"user">>, [<<"id">>]}, {<<"app">>, [<<"id">>]}, {<<"site">>, [<<"id">>]}]}]` | ~2.9 us (**~15x**) | 2.9-3.4 us (**~50-60x**) |
| A field near the end | `[<<"maxtime">>]` | ~3.1-3.7 us (**~11-14x**) | ~3.6 us (**~49x**) |
| Fields of every `imp` | `[{<<"bid_request">>, [{<<"imp">>, [<<"id">>, {<<"banner">>, [<<"w">>, <<"h">>]}]}]}]` | ~2.2 us (**~19x**) | ~15-16 us (**~11x**) |

Messages with **positive block counts** (`erlav`'s default output):

| What you need | 900 B message | 25 imps (2.8 KB) |
|---|---|---|
| Full decode | ~44 us | ~179 us |
| Routing keys | ~1.6 us (**~27x**) | ~1.5 us (**~115x**) |
| Event + device | 2.6-3.0 us (**~15-17x**) | 9.6-10.3 us (**~17-19x**) |
| Three ids | 3.8-4.1 us (**~11x**) | 11.7-12.4 us (**~14-15x**) |
| A field near the end | ~5.3 us (**~8.5x**) | 12-14.6 us (**~12-15x**) |
| Fields of every `imp` | ~2.3 us (**~19x**) | 16-17 us (**~10-11x**) |

How to read this:

- **The win grows with the message, not with the schema.** A selection that
  stops early costs the same whether the message is 900 bytes or 2.8 KB
  ("Routing keys": ~1.6 us in both), while the full decode gets 4x slower. That
  is where the ~100x comes from; it is a ratio against a long full decode, not a
  claim that anything got 100x cheaper than before.
- **Block counts matter for big arrays and maps.** With negative counts an
  unwanted array is skipped in one step, so "A field near the end" stays at
  ~3.6 us for 25 imps. With positive counts the skipper has to walk every item
  of the unwanted arrays (still without building terms), so it grows to
  12-15 us. If you control the writer, `use_negative_block_count` makes
  selective decoding of large messages several times faster.
- **Selecting inside a big array is the slowest case.** "Fields of every `imp`"
  has to visit every element, so it scales with the number of `imp`s
  (~11x faster than a full decode, not ~100x).
- **These are best cases for "a couple of fields".** They time `erlav_decode_fast/2`
  against `erlav_decode_projected/2` in a tight loop on one machine (Apple
  clang, OTP 26), results already in memory, no message parsing or I/O. If you
  select most of a record the gain shrinks to nothing (all 13 top-level fields:
  ~1x). Measure your own schema and messages before relying on the numbers.

**Differences from a full decode to be aware of:**

- Bytes after the last selected field are **not validated**. A truncated or
  corrupt tail that `erlav_decode_fast/2` rejects can still decode successfully
  here. Everything up to and including the last selected field is checked, and
  skipped fields are checked as far as skipping them needs (a bad length, a
  truncated value), not fully.
- Strings and `bytes` are always copied, even if the schema was initialised with
  `decode_sub_binaries`: a projection usually keeps a few fields of a large
  message, and a sub-binary would keep the whole input alive for them.
- A schema the full decoder cannot decode, such as an array whose `items` is a
  bare `"array"` with no item type, fails with `{error, _, 17}` instead of
  returning a value read from the wrong offset.

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
