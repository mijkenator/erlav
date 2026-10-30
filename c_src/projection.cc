#include "projection.hh"

#include <memory>

#include "avro_exceptions.hh"

namespace mkh_avro2 {

// Error codes for a spec that does not fit the schema (all raised at
// erlav_projection/2 time, never while decoding). 100-103 are erlav_init's.
static constexpr uint32_t SPEC_NOT_LIST = 110;
static constexpr uint32_t SPEC_BAD_ELEMENT = 111;
static constexpr uint32_t SPEC_UNKNOWN_FIELD = 112;
static constexpr uint32_t SPEC_DUPLICATE_FIELD = 113;
static constexpr uint32_t SPEC_FIELD_NOT_ON_WIRE = 114;
static constexpr uint32_t SPEC_BAD_SUBSPEC_TARGET = 115;
static constexpr uint32_t SPEC_EMPTY_SUBSPEC = 116;

[[noreturn]] static void spec_error(const std::string& msg, uint32_t code) {
    throw mkh_avro::AvroException("erlav_projection: " + msg, code);
}

// What a sub-spec ends up applying to, found by looking through the wrappers
// project_value() knows how to walk: a record, a union with a single non-null
// member, and an array whose items are a record. Anything else (a scalar, a
// multi-member union, a map, an array of unions or arrays, ...) is rejected
// here so a projection can never decode differently from the full decoder.
struct SubTarget {
    SchemaItem* record = nullptr;
    bool in_array = false; // more elements follow each decoded record
};

static bool find_array_record(SchemaItem* arr, SubTarget& out) {
    if (arr->obj_field != "complex" || arr->array_type != 0 || arr->childItems.size() != 1) {
        return false;
    }
    SchemaItem* item = arr->childItems[0];
    if (item->obj_type != 3) return false;
    out.record = item;
    out.in_array = true;
    return true;
}

static bool find_sub_target(SchemaItem* f, SubTarget& out) {
    if (f->obj_type == 3) {
        out.record = f;
        return true;
    }
    if (f->obj_type == 2) {
        return find_array_record(f, out);
    }
    if (f->obj_type == 1 && f->childItems.size() == 1) {
        SchemaItem* member = f->childItems[0];
        if (member->obj_type == 3) {
            out.record = member;
            return true;
        }
        if (member->obj_type == 2) {
            return find_array_record(member, out);
        }
    }
    return false;
}

static std::string binary_to_string(const ErlNifBinary& b) {
    return std::string(reinterpret_cast<const char*>(b.data), b.size);
}

static Projection* compile_record(ErlNifEnv* env, SchemaItem* rec, ERL_NIF_TERM spec);

static void compile_element(ErlNifEnv* env, SchemaItem* rec, Projection* p, ERL_NIF_TERM elem) {
    ErlNifBinary name;
    ERL_NIF_TERM sub_spec = 0;
    bool has_sub = false;

    if (enif_inspect_binary(env, elem, &name)) {
        // bare name: the whole value
    } else {
        int arity;
        const ERL_NIF_TERM* tup;
        if (!enif_get_tuple(env, elem, &arity, &tup) || arity != 2 ||
            !enif_inspect_binary(env, tup[0], &name)) {
            spec_error("each spec element must be <<\"field\">> or {<<\"field\">>, SubSpec}",
                       SPEC_BAD_ELEMENT);
        }
        sub_spec = tup[1];
        has_sub = true;
    }

    const std::string fname = binary_to_string(name);
    const size_t idx = rec->find_field_index(fname.data(), fname.size());
    if (idx == SIZE_MAX) {
        spec_error("unknown field '" + fname + "' in record '" + rec->obj_name + "'",
                   SPEC_UNKNOWN_FIELD);
    }
    SchemaItem* f = rec->childItems[idx];
    if (!field_is_decoded(f)) {
        // Full decode emits no key for such a field, so there is nothing to select.
        spec_error("field '" + fname + "' has no decodable type", SPEC_FIELD_NOT_ON_WIRE);
    }
    if (p->want[idx] != Projection::SKIP) {
        spec_error("field '" + fname + "' selected more than once", SPEC_DUPLICATE_FIELD);
    }

    if (!has_sub) {
        p->want[idx] = Projection::WHOLE;
    } else {
        SubTarget target;
        if (!find_sub_target(f, target)) {
            spec_error("field '" + fname + "' is not a record, an array of records, or a "
                       "nullable one of those, so it cannot have a sub-spec",
                       SPEC_BAD_SUBSPEC_TARGET);
        }
        if (enif_is_empty_list(env, sub_spec)) {
            spec_error("field '" + fname + "' has an empty sub-spec; use the bare field name "
                       "to select the whole value, or leave the field out",
                       SPEC_EMPTY_SUBSPEC);
        }
        Projection* sub = compile_record(env, target.record, sub_spec);
        // Until finalize() runs, tail_needed only carries what this level
        // alone knows: array elements are always followed by more bytes.
        sub->tail_needed = target.in_array;
        p->want[idx] = Projection::RECURSE;
        p->sub[idx] = sub;
    }
    p->nwanted++;
    if (idx + 1 > p->scan_end) p->scan_end = idx + 1;
}

static Projection* compile_record(ErlNifEnv* env, SchemaItem* rec, ERL_NIF_TERM spec) {
    if (!enif_is_list(env, spec)) {
        spec_error("spec must be a list", SPEC_NOT_LIST);
    }
    std::unique_ptr<Projection> p(new Projection());
    p->want.assign(rec->childItems.size(), Projection::SKIP);
    p->sub.assign(rec->childItems.size(), nullptr);

    ERL_NIF_TERM list = spec, head;
    while (enif_get_list_cell(env, list, &head, &list)) {
        compile_element(env, rec, p.get(), head);
    }
    if (!enif_is_empty_list(env, list)) {
        spec_error("spec must be a proper list", SPEC_NOT_LIST);
    }
    return p.release();
}

// Top-down pass that settles tail_needed (see projection.hh). A nested record
// may stop early only if nothing after it is wanted: it is not an array
// element, the enclosing record has no later selected field, and the
// enclosing record may itself stop early.
static void finalize(Projection* p, bool incoming_tail) {
    p->tail_needed = p->tail_needed || incoming_tail;
    for (size_t i = 0; i < p->sub.size(); i++) {
        if (p->sub[i] != nullptr) {
            finalize(p->sub[i], (i + 1 < p->scan_end) || p->tail_needed);
        }
    }
}

Projection* compile_projection(ErlNifEnv* env, SchemaItem* root, ERL_NIF_TERM spec) {
    std::unique_ptr<Projection> p(compile_record(env, root, spec));
    finalize(p.get(), false);
    return p.release();
}

std::string projection_canonical_form(const Projection& p) {
    std::string out;
    for (size_t i = 0; i < p.want.size(); i++) {
        if (p.want[i] == Projection::SKIP) continue;
        if (!out.empty()) out += ',';
        out += std::to_string(i);
        if (p.want[i] == Projection::RECURSE) {
            out += '(' + projection_canonical_form(*p.sub[i]) + ')';
        }
    }
    return out;
}

}
