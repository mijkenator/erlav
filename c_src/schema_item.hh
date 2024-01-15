#include <stdexcept>
#include <vector>
#include <map>
#include <iostream>
#include "include/json.hpp"

using json = nlohmann::json;

namespace mkh_avro2 {

typedef std::vector<std::string> MJpaths;
typedef std::map<std::string, MJpaths> MJpatch;


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
            } else if (it.is_object() && it["type"] == "array") {
                array_multi_type["array"] = index;
                childItems = read_internal_types(it);
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
