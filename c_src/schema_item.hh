#include <stdexcept>
#include <vector>
#include <map>
#include <unordered_map>
#include <iostream>
#include <erl_nif.h>
#include "include/json.hpp"

using json = nlohmann::json;

namespace mkh_avro2 {

bool is_scalar(std::string);
int get_scalar_type(std::string);

struct SchemaItem {
    int obj_type = -1;
    int array_type = -1;
    int scalar_type = -1;
    int obj_simple_type = 0;
    int is_nullable = 0;
    std::string obj_name;
    std::vector<SchemaItem*> childItems;
    std::string obj_field = "complex";
    std::map<std::string, int> array_multi_type;
    std::map<int, std::string> array_multi_type_reverse;
    // For non-scalar union members (array/map/record/enum), maps the
    // member's type name to its position in childItems.
    std::map<std::string, int> array_multi_type_child_index;
    ErlNifEnv* key_env = nullptr; // Only set on root object for cleanup
    std::vector<ERL_NIF_TERM> cached_keys;
    // Maps a record field's name to its position in childItems/cached_keys.
    // Built alongside cached_keys (see init_keys_with_env) so encoderecord
    // can look up "is this input-map key one of my fields, and if so
    // which" in O(1) instead of comparing against every field name in
    // turn -- lets a single pass over the input map's entries replace the
    // previous nfields separate enif_get_map_value scans (each an
    // independent O(map_size) walk of Erlang's flatmap representation).
    std::unordered_map<std::string, size_t> field_index_by_name;

    // Destructor to clean up the shared environment if this is the root object
    ~SchemaItem() {
        if (key_env != nullptr) {
            enif_free_env(key_env);
        }
    }

    // Build binary key terms once in a process-independent env.
    // Called after schema construction; recurses into all children.
    void init_keys() {
        key_env = enif_alloc_env(); // Root object owns the environment
        init_keys_with_env(key_env);
    }

    // Internal recursive function that uses a shared environment
    void init_keys_with_env(ErlNifEnv* shared_env) {
        if (obj_type == 3 && !childItems.empty()) {
            cached_keys.resize(childItems.size());
            field_index_by_name.reserve(childItems.size());
            for (size_t i = 0; i < childItems.size(); i++) {
                const auto& name = childItems[i]->obj_name;
                auto len = name.size();
                unsigned char* data =
                    enif_make_new_binary(shared_env, len, &cached_keys[i]);
                memcpy(data, name.c_str(), len);
                // A duplicate field name would silently collide here,
                // leaving field_index_by_name pointing at only the last
                // occurrence -- encoderecord's single-pass path (see
                // mkh_avro2.hh) would then treat every earlier
                // same-named field as absent from the input map, and for
                // a required non-nullable non-array field that means no
                // bytes get emitted for it at all, corrupting the
                // encoded output. Avro requires field names to be unique
                // within a record, so reject this at schema-init time
                // instead of miscompiling silently at encode time.
                if (!field_index_by_name.emplace(name, i).second) {
                    throw std::runtime_error(
                        "Duplicate field name '" + name + "' in record '" +
                        obj_name + "'");
                }
            }
        }
        for (auto* child : childItems) {
            child->init_keys_with_env(shared_env);
        }
    }

    void set_undefined_obj_type(int ot) {
        if (obj_type == -1) {
            obj_type = ot;
        }
    }

    void set_array_multi_types(json atypes) {
        int index = 0;
        for (auto it : atypes) {
            if (it.is_string()) {
                array_multi_type[it] = index;
                array_multi_type_reverse[index] = it;
            } else if (it.is_object() && it.contains("type") &&
                       it["type"].is_string()) {
                // Non-scalar union member: array, map, record or enum.
                // Keyed by its Avro type keyword, matching the existing
                // "array" convention -- a union is expected to contain at
                // most one member of each such kind.
                std::string ttype = it["type"];
                array_multi_type[ttype] = index;
                array_multi_type_reverse[index] = ttype;
                array_multi_type_child_index[ttype] = childItems.size();
                childItems.push_back(read_object_type(it));
            }
            index++;
        }
    }

    SchemaItem* read_object_type(json otype) {
        SchemaItem* intsi;
        if (otype["type"] == "array") {
            intsi = new SchemaItem("array", otype["items"], 2);
        } else if (otype["type"] == "map") {
            intsi = new SchemaItem("map", otype["values"], 4);
        } else if (otype["type"] == "record") {
            intsi = new SchemaItem(otype["name"], otype["fields"], 3);
        } else if (otype["type"] == "enum") {
            intsi = new SchemaItem(otype["name"], otype["symbols"], 5);
        } else {
            if (otype["type"].is_string() && is_scalar(otype["type"])) {
                // record field with scalar type
                intsi = new SchemaItem(otype["name"], otype["type"], 0);
            } else if (otype["type"].is_object() &&
                       otype["type"].contains("type") &&
                       otype["type"]["type"] == "array") {
                // array of simple types
                intsi =
                    new SchemaItem(otype["name"], otype["type"]["items"], 2);
            } else if (otype["type"].is_object() &&
                       otype["type"].contains("type") &&
                       otype["type"]["type"] == "map") {
                intsi =
                    new SchemaItem(otype["name"], otype["type"]["values"], 4);
            } else if (otype["type"].is_object() &&
                       otype["type"].contains("type") &&
                       otype["type"]["type"] == "record") {
                intsi =
                    new SchemaItem(otype["name"], otype["type"]["fields"], 3);
            } else if (otype["type"].is_object() &&
                       otype["type"].contains("type") &&
                       otype["type"]["type"] == "enum") {
                std::cout << "ENUM"
                          << "\r\n";
                intsi =
                    new SchemaItem(otype["name"], otype["type"]["symbols"], 5);
            } else {
                intsi = new SchemaItem(otype["name"], otype["type"]);
            }
        }
        return intsi;
    }

    std::vector<SchemaItem*> read_internal_types(json utypes) {
        std::vector<SchemaItem*> retv;
        SchemaItem* intsi;
        if (utypes.is_array()) {
            for (auto it : utypes) {
                if (it.is_string() && "null" == it) {
                    is_nullable = 1;
                } else if (it.is_string()) {
                    obj_field = it;
                    // Keep scalar_type in lockstep with obj_field so callers
                    // can trust si->scalar_type directly instead of calling
                    // get_scalar_type(si->obj_field) (a linear scan) every
                    // encode. -1 for non-scalar obj_field values (e.g.
                    // "array"), matching get_scalar_type's "not found".
                    scalar_type = get_scalar_type(it);
                    intsi = new SchemaItem("union_member", it, 0);
                    retv.push_back(intsi);
                } else if (it.is_object()) {
                    retv.push_back(read_object_type(it));
                } else {
                    throw std::runtime_error("Not implemented 5");
                }
            }
        } else if (utypes.is_object()) {
            retv.push_back(read_object_type(utypes));
        } else {
            throw std::runtime_error("Not implemented 7");
        }
        return retv;
    }

    SchemaItem(std::string name, json jtypes, int ot) {
        obj_name = name;
        obj_type = ot;
        if (jtypes.is_string()) {
            // scalar types
            set_undefined_obj_type(0);
            obj_field = jtypes;
            array_type = 0;
            scalar_type = get_scalar_type(jtypes);
        } else if ((2 == ot) && (jtypes.is_array())) {
            // array of multiple types
            array_type = 1;
            set_undefined_obj_type(0);
            obj_field = "complex";
            set_array_multi_types(jtypes);
            scalar_type = -1;
        } else if ((5 == ot) && (jtypes.is_array())) { // enum
            set_array_multi_types(
                jtypes); // enum use array_multi_type to store possible values
            obj_field = "complex";
            scalar_type = -1;
        } else {
            array_type = 0;
            childItems = read_internal_types(jtypes);
        }
    }
    SchemaItem(std::string name, json jtypes) {
        obj_name = name;
        if (jtypes.is_string()) {
            // scalar types
            set_undefined_obj_type(0);
            obj_field = jtypes;
            if ("map" == jtypes) {
                obj_type = 4;
            } else if ("array" == jtypes) {
                obj_type = 2;
            } else if ("record" == jtypes) {
                obj_type = 3;
            }
        } else if (jtypes.is_array()) { // union type
            obj_type = 1;
            childItems = read_internal_types(jtypes);
        } else {
            childItems = read_internal_types(jtypes);
        }
    }
};

}
