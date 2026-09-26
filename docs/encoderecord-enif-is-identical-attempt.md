# Why the `enif_is_identical` fast-path for `encoderecord`'s key lookup is a dead end

## Background

Issue #23 profiled `erlav_nif:erlav_encode/2` on top of PR #21's single-pass
`encoderecord` fix and found the NIF-boundary/BEAM-term-marshaling cost
(~26.4% of in-NIF time) as the next-largest chunk after `encoderecord`'s own
dispatch. Within that, `field_index_by_name`'s hash lookup (added by PR #21
to replace per-field `enif_get_map_value` scans, see #20) accounted for
~4.5%, and binary key comparison (`eq`/`memcmp`) for another ~4.4%.

The issue's highest-confidence suggestion (option 1) was to short-circuit
that cost: in `encoderecord`'s single-pass scan
(`c_src/mkh_avro2.hh:363-383`), compare each input map key against the
schema's `cached_keys` (`c_src/schema_item.hh:31`, built once at
schema-init time) with `enif_is_identical`, on the theory that a caller
building a map with a literal key (e.g. `#{<<"event">> => ...}`) might reuse
the exact same allocated `ERL_NIF_TERM` the schema already cached — turning
the hash-lookup-plus-`memcmp` path into a cheap pointer-equality check for
that common case, falling back to `field_index_by_name` only when it misses.

## What was checked before writing any code

Before prototyping, two things needed confirming: what `enif_is_identical`
actually costs, and whether the specific term-identity scenario the issue
describes can ever occur given how this codebase allocates its cached keys.

### `enif_is_identical`'s real cost

Per the [Erlang docs](https://www.erlang.org/doc/apps/erts/erl_nif.html),
`enif_is_identical` corresponds to `=:=` — full structural equality, not
reference identity. Looking at the ERTS source
(`erts/emulator/beam/erl_nif.c`) confirms the implementation is a direct
call-through:

```c
int enif_is_identical(Eterm lhs, Eterm rhs)
{
    return EQ(lhs,rhs);
}
```

`EQ` (`erts/emulator/beam/erl_utils.h`) does have a pointer-equality fast
path before falling back to deep comparison:

```c
#define EQ(x,y) (((x) == (y)) || (is_not_both_immed((x),(y)) && eq((x),(y))))
```

So the mechanism the issue is relying on is real *if* `x == y` can fire.
When it doesn't, `eq()` (`erts/emulator/beam/utils.c`) falls through to a
size check plus `erts_cmp_bits()` — a `memcmp`-class byte comparison for
binaries. That's not free: it's the same class of cost `field_index_by_name`
already avoids by hashing once instead of comparing bytes per candidate.

### Can `x == y` ever fire for `cached_keys` vs. an input map key?

No — and not just "rarely," but structurally never, given how this codebase
builds `cached_keys`. `SchemaItem::init_keys()`
(`c_src/schema_item.hh:70-73`) allocates a dedicated `ErlNifEnv` at
schema-init time, entirely separate from any calling process's heap:

```cpp
void init_keys() {
    key_env = enif_alloc_env(); // Root object owns the environment
    init_keys_with_env(key_env);
}
```

Every field-name binary in `cached_keys` is built once, in this schema-owned
environment, from the schema JSON — independent of any encode call. Every
`erlav_encode` call, by contrast, receives a fresh per-call `env` tied to the
calling BEAM process. A map key term the caller passes in is either a heap
binary living on that process's heap, or (for a source-level literal like
`<<"event">>`) a pointer into that *module's* literal pool — either way, a
completely different allocation from `key_env`'s independently-constructed
binary. There is no code path by which an input map's key term and
`cached_keys`' term can ever be the same heap object, for any caller or
calling pattern.

## Conclusion

`x == y` cannot fire between `cached_keys` and an input map key, so the
`enif_is_identical` fast path degrades unconditionally to `eq()`'s
structural byte comparison — real `memcmp`-class cost, potentially paid
against multiple candidate keys per map entry in a naive per-field
comparison loop, which is worse than the current single hash lookup via
`field_index_by_name`, not better. Unlike the `array<string>` batch-write
attempt (`docs/array-string-encoding-optimization-attempt.md`), which needed
end-to-end benchmarking to disprove a plausible-looking win, this one is
ruled out by the allocation pattern alone — no prototype or benchmark was
implemented, since there is no calling pattern under which it could win.

No change was made to `encoderecord`'s key-lookup path as a result of this
investigation. `field_index_by_name`'s hash lookup (PR #21) remains the
right mechanism for this cost.

## Related

- Issue #23: NIF-boundary/BEAM-term-marshaling cost, option 1 (the proposal
  this document evaluates).
- PR #21: single-pass `encoderecord` fix that introduced
  `field_index_by_name`.
- Issue #20: original map-lookup-dominance finding PR #21 addresses.
- `docs/array-string-encoding-optimization-attempt.md`: the precedent this
  investigation follows for "measure before trusting a profiling-shaped
  win" — here the disproof came from allocation semantics rather than
  benchmarking, but the discipline (don't merge on the strength of the
  argument alone) is the same.
