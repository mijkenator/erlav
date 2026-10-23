# Design proposal: partial (selective) decode

Status: **proposal, no implementation**. Review of the current decoder plus a
design for decoding only caller-selected fields.

## 1. Motivation

Typical consumers of a large, deeply nested Avro record (the OpenRTB test schema has 13 top-level fields and ~425 `"name"` entries once nested records are counted)
read a handful of them: routing keys, a timestamp, a couple of ids. Today
`erlav_decode/2` and `erlav_decode_fast/2` always materialise the whole value
as an Erlang map. The cost is paid whether or not the caller looks at the
result.

Baseline measured once on one developer machine (Apple clang 15, `erlav_decode_fast`, 100k
iterations): `test/opnrtb_test1.avsc` with `test/opnrtb_perf.data`
(900 bytes encoded, 13 top-level fields, deeply nested) takes **~124 µs per decode**, versus
~14.5 µs to *encode* the same term. Decode being ~8x slower than encode on
this schema is not explained by wire work (it is the same 900 bytes), so most
of the time is probably term construction, see 2.3. This is a hypothesis to
confirm by profiling in phase 0, not a finding.

## 2. Review of the current decoder

### 2.1 Structure

- Entry points: `erlav_decode_nif` (iterator over a *copy* of the binary into
  a `std::vector<uint8_t>`) and `erlav_decode_nif_fast` (raw `uint8_t*` over
  the binary's own bytes) in `c_src/erlav_nif.cpp`. Both call
  `mkh_avro2::decode()`; the iterator variant is the legacy path.
- `decode()` (`mkh_avro_decoder.cc:90`) is the record decoder. It loops over
  `si->childItems` and, per field, builds the key binary, dispatches on
  `obj_type`, and `enif_make_map_put`s the value. The seven `obj_type` branches
  are copy-pasted and differ only in which `decode_*` they call;
  `decodevalue()` (line 192) already contains that same switch.
- Value decoders: `decode_scalar`, `decode_union`, `decode_array`,
  `decode_map`, `decode_enum`, `decode_fixed`, `decode_record` (which just
  calls `decode()`).
- Schema is a `SchemaItem` tree built once at `erlav_init`; decode is pure
  pointer-walking over it and shared read-only across schedulers under a
  `shared_lock` on the registry.

### 2.2 Properties that matter for selective decode

1. **Strictly sequential, no offsets.** Avro records have no field lengths or
   offsets on the wire. Field *k* can only be reached by consuming fields
   0..k-1. So partial decode cannot jump; it can only **skip** (walk the bytes
   without building terms) and **stop early**. The savings come from not
   building terms, not from not reading bytes.
2. **Skipping cost depends on type:**
   - fixed-width (`float` 4, `double` 8, `boolean` 1, `fixed` N, `null` 0):
     constant.
   - varint (`int`, `long`, `enum`): scan until a byte `< 0x80`.
   - `string`/`bytes`: read the length varint, advance. No copy.
   - union: read branch index, skip that branch.
   - array/map: a block with a **negative count** carries its byte length
     (Avro spec; per the comments in `decode_block_count`, erlavro writes this form for essentially every array/map, and `erlav_init/2` with
     `use_negative_block_count` writes it), so the whole block can be skipped
     in O(1). The default positive-count form has no such shortcut and every
     item must be walked (O(1) per item if items are fixed-width).
   - record: skip each field in turn.
3. **Output is always a full-shape map.** `decode()` emits an entry for every
   schema field, including absent nullable ones (`undefined`). A partial
   result needs a defined shape (4.3).
4. **Union handling is reader-side only.** `decode_union` indexes
   `childItems[branch]` (or `[branch-1]` when nullable); skip must follow the
   same indexing.
5. **Errors:** failures surface as `{error, Msg, Code}` via the catch blocks in
   the NIF; the new entry points should use the same contract.

### 2.3 Observations that bear on the design (and are worth fixing regardless)

- **No bounds checking** against the input length: #30 (confirmed heap
  overflow), #38 (unchecked union index), #40 (unbounded block count).
  New skip code must not add a second unchecked reader (see 5).
- **`enif_make_map_put` per field** returns a new map each time. For flatmaps
  this is likely O(n) per put, so a record with n fields is O(n^2) to build.
  Building from arrays in one call (`enif_make_map_from_arrays`, if available
  at our minimum OTP; to be verified) would help full *and* partial decode.
  Hypothesis, to be measured in phase 0.
- **Key binaries are re-created on every decode** (`enif_make_new_binary` per
  field per call). The encoder avoids this with `cached_keys`; the decoder
  cannot reuse process-independent terms directly, but it should not be doing
  more than one allocation per key.
- `decode_array` still has an unimplemented multi-type branch that silently
  returns `undefined` and desyncs the cursor (#41). Skip must not inherit that:
  an unsupported shape must be an error, not a guess.

## 3. Goals and non-goals

Goals
- Decode a caller-specified subset of fields, including nested ones, with cost
  roughly proportional to bytes *walked* plus terms *built for selected
  fields*, not terms for the whole schema.
- Result for selected fields identical to the full decode (oracle test, 7).
- No change to the behaviour or performance of `erlav_decode/2`,
  `erlav_decode_fast/2`.
- Safe on malformed input from day one.

Non-goals (first version)
- Random access / lazy decoding of the same binary multiple times (see 4.4-C).
- Selecting individual array elements by index.
- Writer/reader schema resolution beyond "skip fields the reader does not want".

## 4. Proposed design

### 4.1 Selection spec

A spec is a nested description of wanted fields:

```erlang
%% Wanted: top-level `event` and `timestamp`, plus `user.id` and, for every
%% element of the `imp` array, its `id` and `banner.w`.
[<<"event">>,
 <<"timestamp">>,
 {<<"user">>,  [<<"id">>]},
 {<<"imp">>,   [<<"id">>, {<<"banner">>, [<<"w">>]}]}].
```

- A bare name selects the field with its **whole** value.
- `{Name, SubSpec}` selects only listed sub-fields; it applies through a
  nullable union (if the branch is null the value is `undefined`) and to each
  element of an array of records.
- For map-typed fields, `{Name, {keys, [Key]}}` is a possible later extension;
  v1 treats a map as a whole value.

### 4.2 API: compile once, decode many (recommended)

```erlang
-spec erlav_projection(SchemaId, Spec) -> {ok, ProjId} | {error, Msg, Code}.
-spec erlav_decode_projected(ProjId, Bin) -> map() | {error, Msg, Code}.
```

`erlav_projection/2` validates the spec against the schema (unknown field name
is an error at compile time, not silently ignored at decode time) and builds
an immutable `Projection` tree stored in a registry parallel to
`erlav_encoders_map`, behind the same `shared_mutex` pattern. Decoding then
does no name lookup and no spec parsing.

`Projection` mirrors the record structure it applies to:

```
struct Projection {            // one per record SchemaItem on the path
    std::vector<uint8_t>     want;      // per field: 0 skip, 1 take whole, 2 recurse
    std::vector<Projection*> sub;       // for want==2
    int                      last_needed; // index of last wanted field
    bool                     tail_needed; // does anything *after* this record still need reading
};
```

`last_needed` / `tail_needed` drive early exit (4.5). This is conceptually
Avro's reader-schema resolution (a reader that omits writer fields skips
them), just expressed as a projection over the writer's `SchemaItem` tree.

### 4.3 Result shape

- Result is a map containing **exactly the selected keys**, nested like the
  spec. Selected-but-absent nullable fields are present as `undefined`, same
  as full decode. Unselected fields are absent.
- Invariant: `decode_projected(P, B) =:= project(Spec, decode_fast(S, B))`.
  This is the oracle for tests.
- Optional later output mode: a tuple of values in spec order, which avoids
  building and key-hashing maps entirely.

### 4.4 Alternatives considered

| | Idea | Verdict |
|---|---|---|
| A | Pass the field list on every call: `erlav_decode_fields(Id, Bin, Fields)` | Simple, fine for flat top-level selection, but re-resolves names every call and cannot cleanly express nesting. Could be a thin wrapper over B. |
| **B** | **Precompiled projection handle (above)** | **Recommended.** Matches the existing `erlav_init` lifecycle, zero per-call spec cost. |
| C | Lazy decode: `erlav_open(Bin) -> Ref`, then `erlav_get(Ref, Path)` | Best when callers read many different paths from one binary, but needs an offset index or repeated skipping, plus an `enif_resource` lifetime. Revisit if usage shows it is needed. |
| D | Derive a *reader schema* from the writer schema and run the regular decoder | Same idea as B, but the decoder still needs a skip path for writer fields absent from the reader, so it does not remove the work. |

### 4.5 Decode algorithm

New `project()` next to `decode()` in `mkh_avro_decoder.cc`, pointer-based
only (the iterator path is legacy and is not extended):

```
project(env, si, proj, p, end):
    for i in 0 .. proj.last_needed:
        switch proj.want[i]:
            skip:    skipvalue(si.child[i], p, end)
            whole:   put(key[i], decodevalue(si.child[i], p, end))
            recurse: put(key[i], project_value(si.child[i], proj.sub[i], p, end))
    if proj.tail_needed:
        for i in last_needed+1 .. nfields-1: skipvalue(...)
    // else: stop; the rest of the record is never touched
```

`skipvalue(si, p, end)` is the new piece, per-type per 2.2(2):
fixed-width advances a constant; varint scans; string/bytes read length and
advance; union reads the branch and recurses; record skips each field;
array/map use the block header, jumping by the encoded byte length for
negative-count blocks and iterating otherwise.

Early exit: once the last wanted field of the top-level record is consumed,
return. Inside a nested record, remaining fields must only be skipped when an
enclosing record still has wanted fields after it (`tail_needed`).

Cost model. Per call: (bytes walked for skipped fields) + (terms built only
for wanted fields). Largest wins: wide records with large unwanted arrays,
maps, or strings (skipped by length or block header, not copied), and wanted
fields early in the record (early exit). Smallest win: wanted fields near the
end of a record of small scalars, where skipping still touches every byte but
avoids term construction.

Prerequisite refactor: collapse `decode()`'s seven copy-pasted branches onto a
single per-field dispatch (`decodevalue`) so `project()` and `decode()` share
one value-decoding switch and cannot drift apart.

### 4.6 Semantics to document

- Bytes after the last wanted field are **not validated** when early exit
  applies. A projected decode of a truncated/corrupt tail can succeed where a
  full decode would error.
- Corruption inside *skipped* fields is detected only as far as skipping
  needs (e.g. a bad length), not fully.
- A projected decode never returns fields that were not asked for.

## 5. Safety

Skipping is exactly the kind of code that turns a bad length into a wild
pointer. The new code takes an `end` pointer from the start and checks every
advance (varint length, string length, fixed size, block byte-length jump,
block count against remaining bytes). That duplicates what #30/#38/#40 want
for the existing decoder; ideally the shared primitives (`read_varint(p,end)`
and friends) land first, or in the same effort, so there is one bounds-checked
reader rather than two. A negative-count block's byte length must be validated
against `end` before jumping. Unsupported shapes (#41) throw.

## 6. Concurrency and lifecycle

`Projection` is immutable after construction. Registry access follows the
existing `find_schema` pattern (`shared_lock` on read, `unique_lock` on the
rare create). A projection holds a pointer to its `SchemaItem` tree; schemas
are never freed today, so there is no dangling-pointer risk, but this
assumption should be stated next to the registry.

## 7. Test plan

1. **Oracle test.** For every schema+data pair under `test/` and a set of
   generated specs (single field, several fields, nested, all fields, none),
   assert `decode_projected(P, B) =:= project(Spec, decode_fast(S, B))`.
2. **Wire variants.** Run both positive-count and negative-count
   (`use_negative_block_count`) array/map encodings through skip, including
   multi-block arrays and empty arrays/maps.
3. **Type coverage.** Skip over every type: all scalars, `null`, enum, fixed,
   unions (each branch), nested records, arrays of records, maps with union
   values (#32), nullable records.
4. **Spec validation.** Unknown field, selecting a sub-spec on a scalar,
   duplicate entries: all errors at `erlav_projection/2`.
5. **Malformed input.** Truncation at every offset and random byte flips must
   return `{error,_,_}` and never crash (run under ASan/UBSan once, as in #30).
6. **Concurrency.** Many processes decoding with the same `ProjId` while new
   projections are being created.
7. **Regression guard.** `erlav_decode/2` and `erlav_decode_fast/2` perf
   unchanged (A/B build, as in the existing `docs/*-perf.md` docs).

## 8. Performance plan

Acceptance numbers are set after phase 0; no speedup is promised here.
Benchmark on `opnrtb_test1.avsc` (wide, 900 B) and a synthetic record with a
large unwanted array/map/string, for: 1 early field, 3 spread fields, 1 late
field, 1 nested field, and all fields (must not be slower than full decode by
more than noise). Compare against the full-decode baseline in section 1.

## 9. Phased plan

0. **Baseline and prerequisites.** Profile full decode on the OpenRTB fixture
   (confirm or refute the map-build hypothesis in 2.3); collapse `decode()`
   branches onto `decodevalue`; agree with #30/#38/#40 on a shared
   bounds-checked reader.
1. `skipvalue` for all types + `erlav_projection/2` / `erlav_decode_projected/2`
   for **top-level** fields, with oracle tests.
2. Nested records, nullable unions, arrays of records.
3. Early exit (`last_needed`/`tail_needed`) and negative-block jump.
4. Optional: map key selection, tuple output mode.

## 10. Open questions

- Is selecting by **path into arrays** (every element) enough, or do callers
  need element indexes?
- Preferred output when nothing is selected inside a nullable record that is
  null: `undefined` (matches full decode) vs. key omitted.
- Do callers want a values-in-spec-order tuple instead of a map (faster, no
  key hashing)?
- Minimum supported OTP: is `enif_make_map_from_arrays` available there?
- Should `erlav_projection/2` accept dotted-binary paths (`<<"user.id">>`) as
  sugar for the nested form?
