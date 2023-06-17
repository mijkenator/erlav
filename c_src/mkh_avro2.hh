#include <iostream>
#include <fstream>
#include <vector>
#include <stdexcept>
#include "include/json.hpp"
using json = nlohmann::json;

namespace mkh_avro2 {

const std::vector<std::string> scalars{"int", "long", "double", "float", "boolean", "string"};
bool is_scalar(std::string ttype){
    return std::find(scalars.begin(), scalars.end(), ttype) != scalars.end();
}

typedef struct SchemaItem {
    int obj_type = -1;
    int obj_simple_type = 0;
    int is_nullable = 0;
    std::string obj_name;
    std::vector<SchemaItem *> childItems;
    std::string obj_field = "complex";

    void set_undefined_obj_type(int ot){
        if(obj_type == -1){
            obj_type = ot;
        }
    }

    SchemaItem * read_object_type(json otype){
        SchemaItem * intsi;
        if(otype["type"] == "array"){
            intsi = new SchemaItem("array", otype["items"], 2);
        }else if(otype["type"] == "map"){
            intsi = new SchemaItem("map", otype["values"], 4);
        }else if(otype["type"] == "record"){
            intsi = new SchemaItem("record", otype["fields"], 3);
        }else{
            if(otype["type"].is_string() && is_scalar(otype["type"])){
                // record field with scalar type
                intsi = new SchemaItem(otype["name"], otype["type"], 0);
            }else if(otype["type"].is_object() && otype["type"].contains("type") && otype["type"]["type"] == "array"){ 
            // array of simple types
                intsi = new SchemaItem(otype["name"], otype["type"]["items"], 2);
            }else if(otype["type"].is_object() && otype["type"].contains("type") && otype["type"]["type"] == "map"){
                intsi = new SchemaItem(otype["name"], otype["type"]["values"], 4);
            }else if(otype["type"].is_object() && otype["type"].contains("type") && otype["type"]["type"] == "record"){
                intsi = new SchemaItem(otype["name"], otype["type"]["fields"], 3);
            }else{
                    intsi = new SchemaItem(otype["name"], otype["type"]);
            }
        }
        return intsi;
    }

    std::vector<SchemaItem *> read_internal_types(json utypes){
        std::vector<SchemaItem *> retv;
        SchemaItem * intsi;
        if(utypes.is_array()){
            for (auto it : utypes){
                if(it.is_string() && "null" == it){
                    std::cout << "Nullable!!!" << '\n' << '\r';
                    is_nullable = 1;
                }else if(it.is_string()){ 
                    std::cout << "simple type!!! -> " << it <<'\n' << '\r';
                    obj_field = it;
                    intsi = new SchemaItem("union_member", it, 0);
                    retv.push_back(intsi);
                }else if(it.is_object()){
                    retv.push_back(read_object_type(it));
                }else{
                    throw std::runtime_error("Not implemented 5");
                }
            }
        }else if(utypes.is_object()){
            retv.push_back(read_object_type(utypes));
        }else{
            throw std::runtime_error("Not implemented 7");
        }
        return retv;
    }

    SchemaItem(std::string name, json jtypes, int ot){
        obj_name = name;
        obj_type = ot;
        std::cout << "SI constructor 3args" << '\n' << '\r';
        if(jtypes.is_string()){
            // scalar types
            set_undefined_obj_type(0);
            std::cout << "\t" << "SI constructor simlpe type " << jtypes << '\n' << '\r';
            obj_field = jtypes;
        }else{
            childItems = read_internal_types(jtypes);
        }
    }
    SchemaItem(std::string name, json jtypes){
        obj_name = name;
        std::cout << "SI constructor 2args" << '\n' << '\r';
        if(jtypes.is_string()){
            // scalar types
            set_undefined_obj_type(0);
            std::cout << "\t" << "SI constructor simlpe type " << jtypes << '\n' << '\r';
            obj_field = jtypes;
            if("map" == jtypes){
                obj_type = 4;
            }else if("array" == jtypes){
                obj_type = 2;
            }else if("record" == jtypes){
                obj_type = 3;
            }
        }else if(jtypes.is_array()){ // union type
            obj_type = 1;
            childItems = read_internal_types(jtypes);
        }else{
            childItems = read_internal_types(jtypes);
        }
    }
} SchemaItem;


SchemaItem*  read_schema(std::string);
ERL_NIF_TERM encode(ErlNifEnv*, SchemaItem*, const ERL_NIF_TERM*);


ERL_NIF_TERM encode(ErlNifEnv* env, SchemaItem* si, const ERL_NIF_TERM* input){
    ERL_NIF_TERM binary;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary bin;
    ErlNifBinary retbin;
    int len;
    std::vector<uint8_t> retv;
    std::vector<uint8_t> rv;

    retv.reserve(10000);
    rv.reserve(1000);

    if(!enif_is_map(env, *input)){
    	return enif_make_badarg(env);
    }

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}



SchemaItem*  read_schema(std::string schemaName){
    SchemaItem* si;
    std::ifstream f(schemaName);
    json data = json::parse(f);

    si = new SchemaItem(data["name"], data["fields"], 3);
    return si;
}

}
