#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

#include "avro_exceptions.hh"
#include "include/json.hpp"
#include "mkh_avro_decoder.hh"

#ifndef SI_H
#define SI_H
#include "schema_item.hh"
#endif

using json = nlohmann::json;

namespace mkh_avro2 {


extern const std::vector<std::string> scalars;

SchemaItem* read_schema(std::string);
ERL_NIF_TERM encode(ErlNifEnv*, SchemaItem*, const ERL_NIF_TERM*);
int encodevalue(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encodescalar(int, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encodeunion(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encodearray(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encoderecord(SchemaItem* si,
                 ErlNifEnv*,
                 const ERL_NIF_TERM*,
                 std::vector<uint8_t>*);
int encodemap(SchemaItem* si, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_int(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_int(ErlNifEnv*, int, std::vector<uint8_t>*);
int encode_long(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_long_fast(ErlNifEnv*, int64_t, std::vector<uint8_t>*);
int encode_float(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_double(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_string(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_bytes(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_boolean(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int enif_get_bool(ErlNifEnv*, ERL_NIF_TERM*, bool*);
int encodeenum(SchemaItem* si,
               ErlNifEnv*,
               ERL_NIF_TERM*,
               std::vector<uint8_t>*);
size_t encodeInt32(int32_t, std::array<uint8_t, 5>& output) noexcept;
size_t encodeVarint(int64_t, std::array<uint8_t, 10>& output) noexcept;
size_t encodeVarint(int64_t, std::vector<uint8_t>&) noexcept;

ERL_NIF_TERM
encode(ErlNifEnv* env, SchemaItem* si, const ERL_NIF_TERM* input) {
    ERL_NIF_TERM binary;
    ErlNifBinary retbin;
    std::vector<uint8_t> retv;

    retv.reserve(10000);

    if (!enif_is_map(env, *input)) {
        return enif_make_badarg(env);
    }

    encoderecord(si, env, input, &retv);

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}

int
encodevalue(SchemaItem* si,
            ErlNifEnv* env,
            ERL_NIF_TERM* val,
            std::vector<uint8_t>* ret) {
    switch (si->obj_type) {
        case 0:
            return encodescalar(si->scalar_type, env, val, ret);
        case 1:
            return encodeunion(si, env, val, ret);
        case 2:
            return encodearray(si, env, val, ret);
        case 3:
            return encoderecord(si, env, val, ret);
        case 4:
            return encodemap(si, env, val, ret);
        case 5:
            return encodeenum(si, env, val, ret);
        default:
            std::cout << "ENCODE VALUE!!!\n\r";
    }
    return 0;
}

int
encodeenum(SchemaItem* si,
           ErlNifEnv* env,
           ERL_NIF_TERM* input,
           std::vector<uint8_t>* ret) {
    ErlNifBinary sbin;
    std::string enumval;

    if (!enif_inspect_binary(env, *input, &sbin)) {
        return 11;
    }

    enumval.assign((const char*) sbin.data, sbin.size);
    try {
        auto ind = si->array_multi_type.at(enumval);
        return encode_int(env, ind, ret);
    } catch (const std::out_of_range& oor) {
        throw mkh_avro::AvroException(
            "Rec:" + si->obj_name + " enum bad value:" + enumval, 11);
    }
}

int
encodemap(SchemaItem* si,
          ErlNifEnv* env,
          ERL_NIF_TERM* input,
          std::vector<uint8_t>* ret) {
    ErlNifMapIterator iter;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary sbin;
    size_t map_size;

    if (!enif_is_map(env, *input)) {
        return 10;
    }

    if (!enif_get_map_size(env, *input, &map_size)) {
        return 10;
    }

    if (!enif_map_iterator_create(
            env, *input, &iter, ERL_NIF_MAP_ITERATOR_HEAD)) {
        return 10;
    }

    encode_long_fast(env, static_cast<int64_t>(map_size), ret);

    if (si->obj_field != "complex") { // map of scalar types
        auto st = get_scalar_type(si->obj_field);
        do {
            if (!enif_map_iterator_get_pair(env, &iter, &key, &val)) {
                continue;
            }
            if (!enif_inspect_binary(env, key, &sbin)) {
                continue;
            }
            // encode key: length-prefixed string
            encode_long_fast(env, static_cast<int64_t>(sbin.size), ret);
            ret->insert(ret->end(), sbin.data, sbin.data + sbin.size);
            // encode value
            encodescalar(st, env, &val, ret);
        } while (enif_map_iterator_next(env, &iter));
    } else { // map of complex types
        do {
            if (!enif_map_iterator_get_pair(env, &iter, &key, &val)) {
                continue;
            }
            if (!enif_inspect_binary(env, key, &sbin)) {
                continue;
            }
            // encode key: length-prefixed string
            encode_long_fast(env, static_cast<int64_t>(sbin.size), ret);
            ret->insert(ret->end(), sbin.data, sbin.data + sbin.size);
            // encode value
            encodevalue(si->childItems[0], env, &val, ret);
        } while (enif_map_iterator_next(env, &iter));
    }

    enif_map_iterator_destroy(env, &iter);

    if (map_size > 0) {
        ret->push_back(0);
    }
    return 0;
}

int
encoderecord(SchemaItem* si,
             ErlNifEnv* env,
             const ERL_NIF_TERM* input,
             std::vector<uint8_t>* ret) {
    ERL_NIF_TERM val;

    if (!enif_is_map(env, *input)) {
        return 9;
    }

    auto nfields = si->childItems.size();
    // Use pre-built keys from schema init (process-independent env)
    const auto& keys = si->cached_keys;

    for (size_t i = 0; i < nfields; i++) {
        auto* it = si->childItems[i];
        if (enif_get_map_value(env, *input, keys[i], &val)) {
            int encodeCode = encodevalue(it, env, &val, ret);
            if (encodeCode != 0) {
                throw mkh_avro::AvroException("Rec:" + si->obj_name +
                                                  " field:" + it->obj_name,
                                              encodeCode);
            }
        } else if (it->is_nullable == 1) {
            ret->push_back(0);
        } else if (it->obj_type == 2) {
            ret->push_back(0);
        }
    }
    return 0;
}

int
encodearray(SchemaItem* si,
            ErlNifEnv* env,
            ERL_NIF_TERM* val,
            std::vector<uint8_t>* ret) {
    unsigned int len;
    ERL_NIF_TERM elem;
    if (enif_is_list(env, *val)) {
        enif_get_list_length(env, *val, &len);
        encode_long_fast(env, len, ret);
        if (si->obj_field != "complex") {
            auto st = get_scalar_type(si->obj_field);
            for (uint32_t i = 0; i < len; i++) {
                if (enif_get_list_cell(env, *val, &elem, val)) {
                    if(encodescalar(st, env, &elem, ret) > 0) {
                        return 8; // encode scalar failed
                    }
                }
            }
        } else if ((si->obj_field == "complex") && si->array_type == 1) {
            //std::cout << "complex A1 \r\n";
            for (uint32_t i = 0; i < len; i++) {
                if (enif_get_list_cell(env, *val, &elem, val)) {
                    if (enif_is_binary(env, elem)) {
                        // string or enum union member -- both carry a
                        // binary value on the Erlang side, so try string
                        // first (existing behavior) and fall back to enum.
                        bool encoded_ok = false;
                        if (si->array_multi_type.count("string")) {
                            int typeindex = si->array_multi_type.at("string");
                            auto saved_size = ret->size();
                            encode_int(env, typeindex, ret);
                            if (encode_string(env, &elem, ret) == 0) {
                                encoded_ok = true;
                            } else {
                                ret->resize(saved_size);
                            }
                        }
                        if (!encoded_ok && si->array_multi_type.count("enum")) {
                            int typeindex = si->array_multi_type.at("enum");
                            int child_idx =
                                si->array_multi_type_child_index.at("enum");
                            auto saved_size = ret->size();
                            encode_int(env, typeindex, ret);
                            try {
                                if (encodeenum(si->childItems[child_idx],
                                               env,
                                               &elem,
                                               ret) == 0) {
                                    encoded_ok = true;
                                } else {
                                    ret->resize(saved_size);
                                }
                            } catch (const mkh_avro::AvroException&) {
                                ret->resize(saved_size);
                            }
                        }
                        if (!encoded_ok) {
                            return 8;
                        }
                    } else if (enif_is_number(env, elem)) {
                        long i64;
                        double dbl;
                        //std::cout << "complex A1 1.0.0 \r\n";
                        if (enif_get_int64(env, elem, &i64)) {
                            // longs
                            try {
                                int typeindex = si->array_multi_type.at("long");
                                encode_int(env, typeindex, ret);
                                encode_long(env, &elem, ret);
                            } catch (...){
                                return 8;
                            }
                        } else if (enif_get_double(env, elem, &dbl)) {
                            try {
                                int typeindex = si->array_multi_type.at("double");
                                encode_int(env, typeindex, ret);
                                encode_double(env, &elem, ret);
                            } catch (...){
                                return 8;
                            }
                        } else {
                            return 8;
                        }
                    } else if (enif_is_list(env, elem)) {
                        int typeindex = si->array_multi_type.at("array");
                        int child_idx = si->array_multi_type_child_index.at("array");
                        encode_int(env, typeindex, ret);
                        encodearray(si->childItems[child_idx], env, &elem, ret);
                    } else if (enif_is_map(env, elem)) {
                        // record or map union member
                        bool encoded_ok = false;
                        for (const auto& ttype : {"record", "map"}) {
                            if (!si->array_multi_type.count(ttype)) {
                                continue;
                            }
                            int typeindex = si->array_multi_type.at(ttype);
                            int child_idx =
                                si->array_multi_type_child_index.at(ttype);
                            auto saved_size = ret->size();
                            encode_int(env, typeindex, ret);
                            if (encodevalue(
                                    si->childItems[child_idx], env, &elem, ret) ==
                                0) {
                                encoded_ok = true;
                                break;
                            }
                            ret->resize(saved_size);
                        }
                        if (!encoded_ok) {
                            return 8;
                        }
                    } else if (enif_is_atom(env, elem) &&
                               si->array_multi_type.count("null")) {
                        // null union member -- only `undefined` is accepted;
                        // any other atom (true/false/anything else) is not
                        // a valid Avro value here.
                        char atom[16];
                        if (enif_get_atom(
                                env, elem, atom, sizeof(atom), ERL_NIF_LATIN1) &&
                            std::strcmp(atom, "undefined") == 0) {
                            int typeindex = si->array_multi_type.at("null");
                            encode_int(env, typeindex, ret);
                        } else {
                            return 8;
                        }
                    } else {
                        return 8;
                    }
                } else {
                    return 8;
                }
            }
        } else {
            // complex array - no support for union types yet
            for (uint32_t i = 0; i < len; i++) {
                if (enif_get_list_cell(env, *val, &elem, val)) {
                    encodevalue(si->childItems[0], env, &elem, ret);
                }
            }
        }
        if(len > 0){
            ret->push_back(0);
        }
        // std::cout << "close array \r\n";
        return 0;
    }
    return 8;
}

int
encodeunion(SchemaItem* si,
            ErlNifEnv* env,
            ERL_NIF_TERM* val,
            std::vector<uint8_t>* ret) {
    std::array<uint8_t, 5> output;
    // assume null in union always first item
    if (si->childItems.size() == 1) {
        ret->push_back(1 + si->is_nullable);
        return encodevalue(si->childItems[0], env, val, ret);
    } else {
        ret->push_back(0); // reserve first for type index
        auto union_index = ret->size() - 1;
        for (auto iter = si->childItems.begin(); iter != si->childItems.end();
             ++iter) {
            int index = std::distance(si->childItems.begin(), iter);
            auto saved_size = ret->size();
            auto ret_code = encodevalue(*iter, env, val, ret);
            if (ret_code == 0) {
                encodeInt32(index + si->is_nullable, output);
                ret->at(union_index) = output[0];
                return ret_code;
            }
            // Roll back partial bytes from the failed branch
            ret->resize(saved_size);
        }
        if (si->is_nullable == 1) {
            return 0;
        }
        return 7;
    }
    return 0;
}

int
encodescalar(int scalar_type,
             ErlNifEnv* env,
             ERL_NIF_TERM* val,
             std::vector<uint8_t>* ret) {
    switch (scalar_type) {
        case 0:
            return encode_int(env, val, ret);
        case 1:
            return encode_long(env, val, ret);
        case 2:
            return encode_double(env, val, ret);
        case 3:
            return encode_float(env, val, ret);
        case 4:
            return encode_boolean(env, val, ret);
        case 5:
            return encode_string(env, val, ret);
        case 6:
            return encode_bytes(env, val, ret);
    }
    return 1;
}

// Registry of named record/enum definitions found anywhere in the schema,
// keyed by both their bare name and their namespace-qualified fullname.
// Avro resolves a bare type-name reference against the enclosing namespace
// first, falling back to the bare name (schemas without namespaces, or a
// reference already given as a fullname).
typedef std::map<std::string, json> NamedTypeRegistry;

std::string
child_namespace(const json& node, const std::string& enclosing_ns) {
    if (node.is_object() && node.contains("namespace") &&
        node["namespace"].is_string()) {
        return node["namespace"];
    }
    return enclosing_ns;
}

// Recursively walk the whole schema and record every record/enum
// definition (object with "type":"record"/"enum" and a "name") under its
// bare name and, if a namespace is in scope, its fullname too.
void
collect_named_types(json& node,
                     const std::string& enclosing_ns,
                     NamedTypeRegistry& registry) {
    if (node.is_object()) {
        if (node.contains("type") && node["type"].is_string() &&
            (node["type"] == "record" || node["type"] == "enum") &&
            node.contains("name") && node["name"].is_string()) {
            std::string ns = child_namespace(node, enclosing_ns);
            std::string name = node["name"];
            registry[name] = node;
            if (!ns.empty()) {
                registry[ns + "." + name] = node;
            }
        }
        std::string next_ns = child_namespace(node, enclosing_ns);
        for (auto& item : node.items()) {
            collect_named_types(item.value(), next_ns, registry);
        }
    } else if (node.is_array()) {
        for (auto& item : node) {
            collect_named_types(item, enclosing_ns, registry);
        }
    }
}

bool
is_container_keyword(const std::string& name) {
    return name == "null" || name == "array" || name == "map" ||
           name == "record" || name == "enum";
}

// Look up a possibly-bare type name against the enclosing namespace first
// (Avro fullname resolution), then as a bare/unqualified name. Returns
// nullptr if nothing matches (left to the existing "unknown type"
// handling further down the pipeline).
const json*
lookup_named_type(const std::string& name,
                   const std::string& enclosing_ns,
                   const NamedTypeRegistry& registry) {
    if (is_scalar(name) || is_container_keyword(name)) {
        return nullptr;
    }
    std::string fullname = enclosing_ns.empty() ? name : enclosing_ns + "." + name;
    auto found = registry.find(fullname);
    if (found == registry.end()) {
        found = registry.find(name);
    }
    return found == registry.end() ? nullptr : &found->second;
}

// Forward declaration -- resolve_type_slot and resolve_named_type_refs
// are mutually recursive (a "type"/"items"/"values" slot may itself be an
// inline record with its own nested "fields"/"type" slots).
void resolve_named_type_refs(json& node,
                              const std::string& enclosing_ns,
                              NamedTypeRegistry& registry,
                              std::set<std::string>& active);

// Rewrite the "type"/"items"/"values" slot of a schema object in place:
// a bare string reference is replaced with the resolved definition (and
// the substituted definition is itself recursed into, in case it carries
// further named-type references), each string member of a union array is
// resolved individually, and inline object definitions are recursed into.
// Only these three keys ever hold type information in an Avro schema --
// other keys ("name", "doc", "default", "aliases", "symbols", ...) are
// left untouched so a field whose own name happens to collide with a type
// name is never mistaken for a type reference.
//
// `active` tracks type names currently being substituted along the
// current recursion path, so a self-/mutually-recursive Avro type (e.g. a
// linked-list-style record referencing its own name) is expanded once and
// then left as the bare name rather than recursing forever -- matching
// how encode/decode already treat a still-unresolved name (SchemaItem
// falls back to treating it as an unknown scalar rather than crashing).
void
resolve_type_slot(json& slot,
                   const std::string& enclosing_ns,
                   NamedTypeRegistry& registry,
                   std::set<std::string>& active) {
    if (slot.is_string()) {
        std::string name = slot.get<std::string>();
        if (active.count(name) == 0) {
            const json* resolved =
                lookup_named_type(name, enclosing_ns, registry);
            if (resolved != nullptr) {
                slot = *resolved;
                active.insert(name);
                resolve_named_type_refs(slot, enclosing_ns, registry, active);
                active.erase(name);
            }
        }
    } else if (slot.is_array()) {
        for (auto& item : slot) {
            resolve_type_slot(item, enclosing_ns, registry, active);
        }
    } else if (slot.is_object()) {
        resolve_named_type_refs(slot, enclosing_ns, registry, active);
    }
}

// Walk a schema object/record definition, resolving named-type references
// in its "type"/"items"/"values" slots and recursing into its "fields"
// (record) list, wherever such a definition appears (top-level schema,
// nested record, array items, map values, union member, ...).
void
resolve_named_type_refs(json& node,
                         const std::string& enclosing_ns,
                         NamedTypeRegistry& registry,
                         std::set<std::string>& active) {
    if (!node.is_object()) {
        return;
    }
    std::string next_ns = child_namespace(node, enclosing_ns);
    for (const auto& key : {"type", "items", "values"}) {
        if (node.contains(key)) {
            resolve_type_slot(node[key], next_ns, registry, active);
        }
    }
    if (node.contains("fields") && node["fields"].is_array()) {
        for (auto& field : node["fields"]) {
            resolve_named_type_refs(field, next_ns, registry, active);
        }
    }
}

void
resolve_user_types(json& data) {
    NamedTypeRegistry registry;
    std::string ns =
        data.contains("namespace") ? data["namespace"].get<std::string>() : "";
    collect_named_types(data, ns, registry);
    std::set<std::string> active;
    resolve_named_type_refs(data, ns, registry, active);
}

SchemaItem*
read_schema(std::string schemaName) {
    SchemaItem* si;
    std::ifstream f(schemaName);
    json data = json::parse(f);
    resolve_user_types(data);
    si = new SchemaItem(data["name"], data["fields"], 3);
    si->init_keys();
    return si;
}

// ------ primitive encoders

uint64_t
encodeZigzag64(int64_t input) noexcept {
    // cppcheck-suppress shiftTooManyBitsSigned
    return ((input << 1) ^ (input >> 63));
}

int64_t
decodeZigzag64(uint64_t input) noexcept {
    return static_cast<int64_t>(
        ((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

uint32_t
encodeZigzag32(int32_t input) noexcept {
    // cppcheck-suppress shiftTooManyBitsSigned
    return ((input << 1) ^ (input >> 31));
}

int32_t
decodeZigzag32(uint32_t input) noexcept {
    return static_cast<int32_t>(
        ((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

size_t
encodeInt64(int64_t input, std::array<uint8_t, 10>& output) noexcept {
    auto val = encodeZigzag64(input);

    // put values in an array of bytes with variable length encoding
    const int mask = 0x7F;
    auto v = val & mask;
    size_t bytesOut = 0;
    while (val >>= 7) {
        output[bytesOut++] = (v | 0x80);
        v = val & mask;
    }

    output[bytesOut++] = v;
    return bytesOut;
}

size_t
encodeVarint(int64_t val, std::array<uint8_t, 10>& output) noexcept {
    // put values in an array of bytes with variable length encoding
    const int mask = 0x7F;
    auto v = val & mask;
    size_t bytesOut = 0;
    while (val >>= 7) {
        output[bytesOut++] = (v | 0x80);
        v = val & mask;
    }

    output[bytesOut++] = v;
    return bytesOut;
}

size_t
encodeVarint(int64_t val, std::vector<uint8_t>& ret) noexcept {
    const int mask = 0x7F;
    auto v = val & mask;
    while (val >>= 7) {
        ret.push_back( (v| 0x80) );
        v = val & mask;
    }

    ret.push_back(v);
    return 1;
}

size_t
encodeInt32(int32_t input, std::array<uint8_t, 5>& output) noexcept {
    auto val = encodeZigzag32(input);

    // put values in an array of bytes with variable length encoding
    const int mask = 0x7F;
    auto v = val & mask;
    size_t bytesOut = 0;
    while (val >>= 7) {
        output[bytesOut++] = (v | 0x80);
        v = val & mask;
    }

    output[bytesOut++] = v;
    return bytesOut;
}

int
encode_int(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    std::array<uint8_t, 5> output;
    int32_t i32;

    if (!enif_get_int(env, *input, &i32)) {
        return 1;
    }
    auto len = encodeInt32(i32, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int
encode_int(ErlNifEnv* env, int input, std::vector<uint8_t>* ret) {
    std::array<uint8_t, 5> output;
    int32_t i32;

    i32 = input;
    auto len = encodeInt32(i32, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int
encode_long(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    std::array<uint8_t, 10> output;
    long i64;

    if (!enif_get_int64(env, *input, &i64)) {
        return 2;
    }
    auto len = encodeInt64(i64, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int
encode_long_fast(ErlNifEnv* env, int64_t input, std::vector<uint8_t>* ret) {
    std::array<uint8_t, 10> output;
    auto len = encodeInt64(input, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int
encode_float(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    float f;
    double dbl;
    long i64;

    if (!enif_get_double(env, *input, &dbl)) {
        if (!enif_get_int64(env, *input, &i64)) {
            return 3;
        }
        dbl = i64;
    }

    f = dbl;
    auto len = sizeof(float);
    const auto* p = reinterpret_cast<const uint8_t*>(&f);

    ret->insert(ret->end(), p, p + len);
    return 0;
}

int
encode_double(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    double dbl;
    long i64;

    if (!enif_get_double(env, *input, &dbl)) {
        if (!enif_get_int64(env, *input, &i64)) {
            return 4;
        }
        dbl = i64;
    }

    auto len = sizeof(double);
    const auto* p = reinterpret_cast<const uint8_t*>(&dbl);
    ret->insert(ret->end(), p, p + len);
    return 0;
}

inline int
encode_binary_data(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    ErlNifBinary sbin;

    if (!enif_inspect_binary(env, *input, &sbin)) {
        return 5;
    }

    auto len = sbin.size;
    auto offset = ret->size();

    if (len < 64) {
        // Fast path: zigzag of non-negative len = len * 2, fits in 1 byte
        ret->resize(offset + 1 + len);
        ret->data()[offset] = static_cast<uint8_t>(len << 1);
        memcpy(ret->data() + offset + 1, sbin.data, len);
    } else {
        std::array<uint8_t, 10> output;
        auto len2 = encodeInt64(len, output);
        ret->resize(offset + len2 + len);
        memcpy(ret->data() + offset, output.data(), len2);
        memcpy(ret->data() + offset + len2, sbin.data, len);
    }

    return 0;
}

int
encode_string(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    return encode_binary_data(env, input, ret);
}

int
encode_bytes(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    return encode_binary_data(env, input, ret);
}

int
encode_boolean(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret) {
    bool bl;

    if (!enif_get_bool(env, input, &bl)) {
        return 6;
    }

    ret->push_back(bl ? 1 : 0);
    return 0;
}

inline int
enif_get_bool(ErlNifEnv* env, ERL_NIF_TERM* term, bool* cond) {
    char atom[256];
    int len;
    if ((len = enif_get_atom(env, *term, atom, sizeof(atom), ERL_NIF_LATIN1)) ==
        0) {
        return false;
    }

    *cond = (std::strcmp(atom, "false") != 0 && std::strcmp(atom, "nil") != 0);

    return true;
}
} // namespace mkh_avro2
