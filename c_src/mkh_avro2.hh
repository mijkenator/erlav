#include <iostream>
#include <fstream>
#include <vector>
#include <stdexcept>
#include "include/json.hpp"
#include "avro_exceptions.hh"
using json = nlohmann::json;

namespace mkh_avro2 {

typedef std::vector<std::string> MJpaths;
typedef std::map<std::string, MJpaths> MJpatch;

const std::vector<std::string> scalars{"int", "long", "double", "float", "boolean", "string", "bytes"};
bool is_scalar(std::string ttype){
    return std::find(scalars.begin(), scalars.end(), ttype) != scalars.end();
}
int get_scalar_type(std::string ntype){
    auto it = find(scalars.begin(), scalars.end(), ntype);
    if (it != scalars.end()){
        int index = it - scalars.begin();
        return index;
    }else{
        return -1;
    } 
}

typedef struct SchemaItem {
    int obj_type = -1;
    int scalar_type = -1;
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
            intsi = new SchemaItem(otype["name"], otype["fields"], 3);
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
                    is_nullable = 1;
                }else if(it.is_string()){ 
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
        if(jtypes.is_string()){
            // scalar types
            set_undefined_obj_type(0);
            obj_field = jtypes;
            scalar_type = get_scalar_type(jtypes);
        }else{
            childItems = read_internal_types(jtypes);
        }
    }
    SchemaItem(std::string name, json jtypes){
        obj_name = name;
        if(jtypes.is_string()){
            // scalar types
            set_undefined_obj_type(0);
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
int encodevalue(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encodescalar(int, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*); 
int encodeunion(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*); 
int encodearray(SchemaItem*, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encoderecord(SchemaItem* si, ErlNifEnv*, const ERL_NIF_TERM*, std::vector<uint8_t>*);
int encodemap(SchemaItem* si, ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_int(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_long(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_long_fast(ErlNifEnv*, int64_t, std::vector<uint8_t>*);
int encode_float(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_double(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_string(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_bytes(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int encode_boolean(ErlNifEnv*, ERL_NIF_TERM*, std::vector<uint8_t>*);
int enif_get_bool(ErlNifEnv*, ERL_NIF_TERM*, bool*);
size_t encodeInt32(int32_t, std::array<uint8_t, 5> &output) noexcept;

ERL_NIF_TERM encode(ErlNifEnv* env, SchemaItem* si, const ERL_NIF_TERM* input){
    ERL_NIF_TERM binary;
    ErlNifBinary retbin;
    std::vector<uint8_t> retv;

    retv.reserve(10000);

    if(!enif_is_map(env, *input)){
    	return enif_make_badarg(env);
    }
    

    encoderecord(si, env, input, &retv);

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}


int encodevalue(SchemaItem* si, ErlNifEnv* env, ERL_NIF_TERM* val, std::vector<uint8_t>* ret){ 
    switch(si->obj_type){
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
        default:
            std::cout << "ENCODE VALUE!!!\n\r";

    }
    return 0;
}

int encodemap(SchemaItem* si, ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){ 
    ErlNifMapIterator iter;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary sbin;
    unsigned int len;
    std::string mapkey;
    std::map<std::string, ERL_NIF_TERM> amap;
    std::map<std::string, ERL_NIF_TERM>::iterator amap_iter;


    if(enif_is_map(env, *input)){
        if(enif_map_iterator_create(env, *input, &iter, ERL_NIF_MAP_ITERATOR_HEAD)) {
            do{
                if(!enif_map_iterator_get_pair(env, &iter, &key, &val)) {
                    continue;
                }
                if (!enif_inspect_binary(env, key, &sbin)) {
                    continue;
                }
                mapkey.assign((const char*)sbin.data, sbin.size);
                amap.insert(std::pair<std::string, ERL_NIF_TERM>(mapkey, val));
            } while(enif_map_iterator_next(env, &iter));

            len = amap.size();
            encode_long_fast(env, len, ret);
            
            if(si->obj_field != "complex"){ // map of scalar types
                auto st = get_scalar_type(si->obj_field);
                for(amap_iter = amap.begin(); amap_iter != amap.end(); amap_iter++){
                    // insert key-lenght && key data
                    auto len3 = amap_iter->first.size();
                    encode_long_fast(env, len3, ret);
                    ret->insert(ret->end(), amap_iter->first.data(), amap_iter->first.data() + len3);
                    // encode value
                    encodescalar(st, env, &amap_iter->second, ret);
                }
            } else { // map of complex types
                for(amap_iter = amap.begin(); amap_iter != amap.end(); amap_iter++){
                    // insert key-lenght && key data
                    auto len3 = amap_iter->first.size();
                    encode_long_fast(env, len3, ret);
                    ret->insert(ret->end(), amap_iter->first.data(), amap_iter->first.data() + len3);
                    // encode value
                    encodevalue(si->childItems[0], env, &amap_iter->second, ret);
                }
            }
            ret->push_back(0);
            return 0;
        }
    }

    return 10;
}

int encoderecord(SchemaItem* si, ErlNifEnv* env, const ERL_NIF_TERM* input, std::vector<uint8_t>* ret){ 
    int len;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary bin;
    
    
    if(!enif_is_map(env, *input)){
    	return 9;
    }
    
    for( auto it: si->childItems ){
        len = it->obj_name.size();
        enif_alloc_binary(len, &bin);
        const auto *p = reinterpret_cast<const uint8_t *>(it->obj_name.c_str());
        memcpy(bin.data, p, len);
        key = enif_make_binary(env, &bin);

        if(enif_get_map_value(env, *input, key, &val)){
            int encodeCode = encodevalue(it, env, &val, ret);
            if(encodeCode != 0){
                //throw encodeCode;
                throw mkh_avro::AvroException("Rec:" + si->obj_name + " field:" + it->obj_name, encodeCode);
            }
        }else if(it->is_nullable == 1){
            ret->push_back(0);
        }

    }
    return 0;
}   

int encodearray(SchemaItem* si, ErlNifEnv* env, ERL_NIF_TERM* val, std::vector<uint8_t>* ret){ 
    unsigned int len;
    ERL_NIF_TERM elem;
    if(enif_is_list(env, *val)){
        enif_get_list_length(env, *val, &len);
        encode_long_fast(env, len, ret);
        if(si->obj_field != "complex"){
            auto st = get_scalar_type(si->obj_field);
            for(uint32_t i=0; i < len; i++){
                if(enif_get_list_cell(env, *val, &elem, val)){
                    encodescalar(st, env, &elem, ret);
                }
            }
        } else {
            // complex array - no support for union types yet
            for(uint32_t i=0; i < len; i++){
                if(enif_get_list_cell(env, *val, &elem, val)){
                    encodevalue(si->childItems[0], env, &elem, ret);
                }
            }
        }
        ret->push_back(0);
        return 0;
    }
    return 8;
}

int encodeunion(SchemaItem* si, ErlNifEnv* env, ERL_NIF_TERM* val, std::vector<uint8_t>* ret){ 
    std::array<uint8_t, 5> output;
    //assume null in union always first item
    if(si->childItems.size() == 1){
        ret->push_back(1 + si->is_nullable); 
        return encodevalue(si->childItems[0], env, val, ret);
    }else{
        ret->push_back(0); // reserve first for type index
        auto union_index = ret->size() - 1;
        for (auto iter = si->childItems.begin(); iter != si->childItems.end(); ++iter) {
            int index = std::distance(si->childItems.begin(), iter);
            auto ret_code = encodevalue(*iter, env, val, ret);
            if(ret_code == 0){
                encodeInt32(index + si->is_nullable, output);
                ret->at(union_index) = output[0];
                return ret_code;
            }
        }
        if(si->is_nullable == 1){
            return 0;
        }
        return 7;
    }
    return 0;
}

int encodescalar(int scalar_type, ErlNifEnv* env, ERL_NIF_TERM* val, std::vector<uint8_t>* ret){ 
    switch(scalar_type){
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

json get_type_object(std::string obj_name, json &data){
    if(data.is_array()){
        for (auto it : data){
            if(it.is_object() && it["type"].is_string() && it["type"] == "record" && it["name"] == obj_name ){
                return it;
            }else if(it.is_object() && it["type"].is_object() && it["type"]["type"] == "record"){
                if(it["type"]["name"] == obj_name){
                    return it["type"];
                }else{
                    get_type_object(obj_name, it["type"]["fields"]);
                }
            }
        }
    }
	return json::object({});
}


void resolve_user_types(std::string jpath, std::string ns, json &data, MJpatch &mp) {
    if(data.is_array()){
        int num = 0;
        for (auto it : data){
            if(it.is_object() && it["type"].is_string() && it["type"].get<std::string>().rfind(ns, 0) == 0 ){
                std::string cpath = jpath + "/" + std::to_string(num) + "/type";
                auto key = it["type"].get<std::string>();
                if(mp.find(key) == mp.end()){
                    mp[key] = {cpath};
                }else{
                    MJpaths jv = mp[key];
                    jv.push_back(cpath);
                    mp[key] = jv;
                }
            }else if(it.is_object() && it["type"].is_array()){
                int intnum = 0;
                for (auto ait : it["type"]){
                    if(ait.is_string() && ait.get<std::string>().rfind(ns, 0) == 0){
                        std::string cpath = jpath + "/" + std::to_string(num) + "/type/" + std::to_string(intnum);
                        auto key = ait.get<std::string>();
                        if(mp.find(key) == mp.end()){
                            mp[key] = {cpath};
                        }else{
                            MJpaths jv = mp[key];
                            jv.push_back(cpath);
                            mp[key] = jv;
                        }
                    }else if(ait.is_object() && ait["type"] == "record"){
                        resolve_user_types(jpath + "/" + std::to_string(num) + "/type/" + std::to_string(intnum), ns, ait["fields"], mp);
                    }
                    intnum++;
                }
            }else if(it.is_object() && it["type"].is_object() && it["type"]["type"] == "record"){
                resolve_user_types(jpath + "/" + std::to_string(num) + "/type/fields", ns, it["type"]["fields"], mp);
            }
            num++;
        }
    }
}

void resolve_user_types(json &data) {
    MJpatch mp;
    std::string ns = data["namespace"];
    resolve_user_types("/fields", ns, data["fields"], mp);

    for (const auto &ele : mp) {
        std::string fname = ele.first;
        fname.erase(0, ns.length() + 1);
        json repl_tst = get_type_object(fname, data["fields"]);
        for (auto & ppath : ele.second){
            std::string patch_command = "[{ \"op\": \"replace\", \"path\": \"" + ppath + "\", \"value\": " +  to_string(repl_tst) +  " }]";
            data = data.patch(json::parse(patch_command));
        }
    }

}


SchemaItem*  read_schema(std::string schemaName){
    SchemaItem* si;
    std::ifstream f(schemaName);
    json data = json::parse(f);
	resolve_user_types(data);
    si = new SchemaItem(data["name"], data["fields"], 3);
    return si;
}


// ------ primitive encoders

uint64_t encodeZigzag64(int64_t input) noexcept {
    // cppcheck-suppress shiftTooManyBitsSigned
    return ((input << 1) ^ (input >> 63));
}

int64_t decodeZigzag64(uint64_t input) noexcept {
    return static_cast<int64_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

uint32_t encodeZigzag32(int32_t input) noexcept {
    // cppcheck-suppress shiftTooManyBitsSigned
    return ((input << 1) ^ (input >> 31));
}

int32_t decodeZigzag32(uint32_t input) noexcept {
    return static_cast<int32_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

size_t
encodeInt64(int64_t input, std::array<uint8_t, 10> &output) noexcept {
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
encodeInt32(int32_t input, std::array<uint8_t, 5> &output) noexcept {
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

int encode_int(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 5> output;
    int32_t i32;
    
    if (!enif_get_int(env, *input, &i32)) {
        return 1;
    }
    auto len = encodeInt32(i32, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_long(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    int64_t i64;
    
    if (!enif_get_int64(env, *input, &i64)) {
        return 2;
    }
    auto len = encodeInt64(i64, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_long_fast(ErlNifEnv* env, int64_t input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    auto len = encodeInt64(input, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_float(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    float f;
    double dbl;
    
    if (!enif_get_double(env, *input, &dbl)) {
        return 3;
    }

    f = dbl;
    auto len = sizeof(float);
    const auto *p = reinterpret_cast<const uint8_t *>(&f);

    ret->insert(ret->end(), p, p + len);
    return 0;

}

int encode_double(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    double dbl;
    
    if (!enif_get_double(env, *input, &dbl)) {
        return 4;
    }

    auto len = sizeof(double);
    const auto *p = reinterpret_cast<const uint8_t *>(&dbl);
    ret->insert(ret->end(), p, p + len);
    return 0;
}

int encode_string(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    ErlNifBinary sbin;

    if (!enif_inspect_binary(env, *input, &sbin)) {
        return 5;
    }

    auto len = sbin.size;
    auto len2 = encodeInt64(len, output);
    ret->insert(ret->end(), output.data(), output.data() + len2);
    ret->insert(ret->end(), sbin.data, sbin.data + len);

    return 0;
}

int encode_bytes(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    ErlNifBinary sbin;

    if (!enif_inspect_binary(env, *input, &sbin)) {
        return 5;
    }

    auto len = sbin.size;
    auto len2 = encodeInt64(len, output);
    ret->insert(ret->end(), output.data(), output.data() + len2);
    ret->insert(ret->end(), sbin.data, sbin.data + len);

    return 0;
}

int encode_boolean(ErlNifEnv* env, ERL_NIF_TERM* input, std::vector<uint8_t>* ret){
    bool bl;
    
    if (!enif_get_bool(env, input, &bl)) {
        return 6;
    }

    ret->push_back(bl ? 1: 0);
    return 0;
}

inline int enif_get_bool(ErlNifEnv* env, ERL_NIF_TERM* term, bool* cond)
{
    char atom[256];
    int  len;
    if ((len = enif_get_atom(env, *term, atom, sizeof(atom), ERL_NIF_LATIN1)) == 0) {
        return false;
    }
    
    *cond = (std::strcmp(atom, "false") != 0 && std::strcmp(atom, "nil") != 0);
    
    return true;
}
}
