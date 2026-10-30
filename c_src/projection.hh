#ifndef ERLAV_PROJECTION_HH
#define ERLAV_PROJECTION_HH

#include <erl_nif.h>
#include <cstdint>
#include <string>
#include <vector>

#ifndef SI_H
#define SI_H
#include "schema_item.hh"
#endif

namespace mkh_avro2 {

// Partial (selective) decode, see docs/design-partial-decode.md (#53).
//
// A Projection is compiled once from a caller's field spec (erlav_projection/2)
// and then used by every erlav_decode_projected/2 call. It mirrors ONE record
// SchemaItem on the path to the selected fields; nested records the spec
// descends into get their own Projection in `sub`. It is immutable after
// compile_projection() returns, so schedulers share it without locking.
struct Projection {
    // Indexed like the record's childItems.
    static constexpr uint8_t SKIP = 0;    // not selected: walk the bytes only
    static constexpr uint8_t WHOLE = 1;   // selected with its whole value
    static constexpr uint8_t RECURSE = 2; // selected, with a sub-spec
    std::vector<uint8_t> want;
    std::vector<Projection*> sub; // non-null exactly where want == RECURSE

    // Number of leading fields project() has to walk: one past the last
    // selected field (0 when nothing is selected).
    size_t scan_end = 0;
    // Number of selected fields (size of the result map).
    size_t nwanted = 0;
    // True when bytes after the last selected field are still needed by
    // someone, so the rest of the record must be skipped rather than left
    // unread: a later field of an enclosing record is selected, or this record
    // is an array element (the next element follows). False lets decoding stop
    // right after the last selected field (early exit).
    bool tail_needed = false;

    Projection() = default;
    Projection(const Projection&) = delete;
    Projection& operator=(const Projection&) = delete;
    ~Projection() {
        for (auto* p : sub) delete p;
    }
};

// Same test decode() applies to each record field: a field with no resolvable
// type consumes no bytes and gets no key. Skipping and projecting must agree
// with it exactly or the cursor drifts.
inline bool field_is_decoded(const SchemaItem* f) {
    if (f->obj_type == 0 && f->scalar_type < 0) return false;
    if (f->obj_type < 0 || f->obj_type > 6) return false;
    return true;
}

// Compiles `spec` (a list of <<"field">> | {<<"field">>, SubSpec}) against the
// root record `root`. Throws mkh_avro::AvroException (codes 110-115) when the
// spec does not fit the schema. The result is owned by the caller.
Projection* compile_projection(ErlNifEnv* env, SchemaItem* root, ERL_NIF_TERM spec);

// Canonical text form of the resolved selection (field positions, not names),
// used as part of the registry key so equal specs share one projection id.
std::string projection_canonical_form(const Projection& p);

}

#endif
