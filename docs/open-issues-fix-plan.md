# Open issues: review and fix plan

Snapshot as of 2026-09-28. 12 open issues, reviewed and grouped below by
impact and root cause, with a proposed fix order.

## Groups

### Crashes from ordinary (non-malicious) usage

Hit by just using a valid, spec-compliant schema shape -- no adversarial
input needed.

- **#44** -- `decode_array`'s complex-array-of-one-type branch mis-decodes/
  crashes on `array<map>`, `array<fixed>`, `array<enum>`. Confirmed VM
  crash on `array<map>` (`binary_alloc: Cannot allocate ... bytes`);
  `array<fixed>`/`array<enum>` silently decode every element as `#{}`
  instead of the real value. Root cause: this branch calls the
  record-specific `decode()` instead of the generic `decodevalue()`
  dispatcher, unlike `encodearray`'s matching branch, which already calls
  `encodevalue` correctly.
- **#32** -- `decode_map`/`encodemap` VM crash on `map<union>` values
  (e.g. `{"type": "map", "values": ["null", "string"]}`), triggered by
  spec-compliant bytes from another Avro implementation (e.g. `erlavro`).
  Root cause: the per-value union discriminator byte is never read/written
  by the "map of scalars" fast path.

### Crashes/corruption from malformed or adversarial input

Require a bad/malicious payload, not just an unusual valid schema.

- **#30** -- no bounds checking against the input buffer's length anywhere
  in the decode path. Confirmed heap-buffer-overflow under AddressSanitizer.
  This is the root-cause issue underlying #38 and #40 below.
- **#38** -- `decode_union`/`decode_array` unchecked union-branch index:
  OOB vector access (undefined behavior, not a catchable exception) plus
  persistent corruption of the shared, cached schema's internal lookup map.
- **#40** -- `decode_array`/`decode_map` block count has no upper-bound
  sanity check: a huge/corrupted count drives the decode loop into an
  effectively unbounded allocate-and-read cycle (DoS).

### Silent correctness bugs

No crash, but wrong or lost data.

- **#41** -- `decode_array`'s "complex array multiple types" branch is
  unimplemented: silently returns `undefined` and desyncs the read cursor
  for everything decoded after it, instead of raising a catchable error.
- **#45** -- a bare `"type": "null"` record field (not part of a union)
  can never be encoded -- `"null"` isn't in the scalars table and
  `encode_field_or_default`/`encodescalar` have no fallback for it. Notably,
  this is the exact shape used by `priv/tschema1.avsc`'s `nullField`, the
  repo's own bundled reference schema.
- **#13** -- union record-vs-map disambiguation: when a union contains
  both a `record` and a `map` member, the encoder can't tell them apart
  (both are Erlang maps) and silently picks the wrong one, dropping data
  with no error. Pre-existing, not a recent regression.

### Code quality / tech debt (not bugs, no urgency)

- **#25** -- migrate C++17 -> C++20 to fix `encoderecord`'s per-lookup
  `std::string` allocation.
- **#27** -- `SchemaItem::field_index_by_name` is a 5th ad-hoc name->index
  lookup structure, inconsistent with `array_multi_type_child_index`.
- **#28** -- `mkh_avro2.hh`: third independent hand-rolled stack-array/
  heap-fallback pattern (`encoderecord`'s found/present arrays).
- **#29** -- `encodemap` and `encoderecord` duplicate the same
  map-iteration skeleton (low priority, not clearly worth factoring).

## Proposed fix order

1. **#44, then #32** -- fix first. Both are VM-crashing bugs reachable by
   any user hitting a common, valid schema shape (`array<map>`,
   `map<union>`) -- no attacker required. Both have a clearly identified,
   scoped root cause (wrong dispatch function in one case, missing
   union-discriminator handling in the other), so each should be a small,
   contained fix. Highest real-world impact per unit of effort.

2. **#30** -- fix next. The most severe issue from a pure security
   standpoint (arbitrary malformed/truncated input -> memory corruption,
   confirmed under ASan), but it only bites when decoding untrusted/
   corrupted input rather than ordinary valid schemas, so it's ranked
   below the two crashes above. Also the bigger effort: threading an
   end-of-buffer bound through every decode primitive.

3. **#38 + #40** -- bundle with #30's PR. Both need the exact same
   infrastructure change (the end-of-buffer bound #30 introduces), so
   fixing them in the same pass avoids threading that bound through the
   decode call chain twice.

4. **#41** -- quick, isolated fix once the above lands (throw instead of
   silently returning wrong data + desyncing the cursor).

5. **#45** -- quick, isolated fix (give bare-`null` fields an encode path).

6. **#13** -- larger design problem (real union disambiguation, not a
   quick patch), lowest urgency among the bug-labeled issues since it's a
   long-standing known limitation rather than a recent regression. Tackle
   after the crash/correctness backlog above is clear.

7. **Tech-debt issues (#25/#27/#28/#29)** -- defer indefinitely; pick up
   opportunistically alongside other work in the same files, not worth a
   dedicated pass.
