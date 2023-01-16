#include <array>
#include <iostream>
#include <fstream>
#include "include/json.hpp"
#include <algorithm>
#include <typeinfo>
using json = nlohmann::json;

namespace mkh_avro {

int encode_int(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_long(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_long_fast(ErlNifEnv*, int64_t, std::vector<uint8_t>*);
int encode_float(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_double(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_string(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_boolean(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int enif_get_bool(ErlNifEnv*, ERL_NIF_TERM, bool*);
int encode_primitive(std::string, ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_array(std::string, ErlNifEnv*, ERL_NIF_TERM&, std::vector<uint8_t>*);

struct SchemaItem{
    std::string fieldName;
    std::vector<std::string> fieldTypes;
    bool nullable = 0;
    bool defnull = 0;
    bool isunion = 0;
    uint8_t obj_type = 0; // 0 - primitive, 1 - array, 2 - map, 3 - record

    void set_null_default(){
        defnull = 1;
    }
    SchemaItem(std::string name, json ftypes){
        fieldName = name;
        isunion = 1;
        if(ftypes.is_array()){
            for(auto i: ftypes){
                //std::cout << "SI2:" << i << '\n' << '\r';
                if("null" == i){
                    nullable = 1;
                    fieldTypes.push_back(i);
                } else if(i.is_object() && (i["type"] == "array")){
                    fieldTypes.push_back(i["items"]);
                    obj_type = 1;
                } else {
                    fieldTypes.push_back(i);
                }
            }
        }else if(ftypes.is_object()){
            //std::cout << "SI3:" << '\n' << '\r';
            if(ftypes["type"] == "array"){
                fieldTypes.push_back(ftypes["items"]);
                obj_type = 1;
            }else if(ftypes["type"] == "map"){
                fieldTypes.push_back(ftypes["values"]);
                obj_type = 2;
            }
        }else if(ftypes.is_string()){
            //std::cout << "SI4:" << ftypes << '\n' << '\r';
            //std::cout << typeid(ftypes).name() << '\n' << '\r';
            fieldName = name;
            fieldTypes.push_back(ftypes);
        }
    }
};



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

std::vector<SchemaItem> read_schema(std::string schemaName){
    std::ifstream f(schemaName);
    json data = json::parse(f);

    json j = data["fields"];
    std::vector<SchemaItem> schema;
    for(auto it: j){
        SchemaItem si(it["name"], it["type"]);
        if(it.contains("default")){
            if(it["default"].is_null()){
                si.set_null_default();
            }
        }
        schema.push_back(si);
    }

    return schema;
}

int encode(SchemaItem it, ErlNifEnv* env, ERL_NIF_TERM term, std::vector<uint8_t>* ret){
    std::vector<std::string> atypes = it.fieldTypes; 
    auto alen = atypes.size();
    if(alen == 1){
        if(it.obj_type == 1){
            //std::cout << "E.ARRAY" << '\n' << '\r';
            return encode_array(atypes[0], env, term, ret);
        } else {
            //std::cout << "E.TYPE" << atypes[0] << '\n' << '\r';
            return encode_primitive(atypes[0], env, term, ret);
        }
    }else{
        //std::cout << "E.UNION:" << alen << '\n' << '\r';
        ret->insert(ret->begin(), 0); // reserve first for type index
        for (auto iter = atypes.begin(); iter != atypes.end(); ++iter) {
            int index = std::distance(atypes.begin(), iter);
            if(*iter != "null"){
                int eret = 999;
                if(it.obj_type == 1){ // array
                    eret = encode_array(*iter, env, term, ret);
                }else{
                    eret = encode_primitive(*iter, env, term, ret);
                }
                if(eret == 0){
                    std::array<uint8_t, 5> output;
                    encodeInt32(index, output);
                    ret->at(0) = output[0];
                    return 0;
                }
            }
        }
        if(it.defnull == 1){
            return 0;
        }else{
            std::cout << it.fieldName << "  " << it.defnull << '\n' << '\r';
        }
        return 666;
    }
}

int encode_array(std::string atype, ErlNifEnv* env, ERL_NIF_TERM& term, std::vector<uint8_t>* ret){
    unsigned int len;
    std::vector<uint8_t> eret;
    //int64_t tmpint;
    //std::cout << "E.ARRAY:" << atype << '\n' << '\r';

    if(enif_is_list(env, term)){
        enif_get_list_length(env, term, &len);
        if(len > 0){
            for(uint32_t i=0; i < len; i++){
                ERL_NIF_TERM elem;
                if(enif_get_list_cell(env, term, &elem, &term)){
                    encode_primitive(atype, env, elem, &eret);
                }
            }
            encode_long_fast(env, len, ret);
            ret->insert(ret->end(), eret.data(), eret.data() + eret.size());
            ret->push_back(0);
        }
        return 0;
    }
    return 667;
}

int encode_primitive(std::string atype, ErlNifEnv* env, ERL_NIF_TERM term, std::vector<uint8_t>* ret){
    if("int" == atype){
        return encode_int(env, term, ret);
    }else if("long" == atype){
        return encode_long(env, term, ret);
    }else if("float" == atype){
        return encode_float(env, term, ret);
    }else if("double" == atype){
        return encode_double(env, term, ret);
    }else if("string" == atype){
        return encode_string(env, term, ret);
    }else if("boolean" == atype){
        return encode_boolean(env, term, ret);
    }else{
        return 99999;
    }
}

int encode_int(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 5> output;
    int32_t i32;
    
    if (!enif_get_int(env, input, &i32)) {
        return 1;
    }
    auto len = mkh_avro::encodeInt32(i32, output);
    //ret->assign(output.data(), output.data() + len);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_long(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    int64_t i64;
    
    if (!enif_get_int64(env, input, &i64)) {
        return 2;
    }
    auto len = mkh_avro::encodeInt64(i64, output);
    //ret->assign(output.data(), output.data() + len);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_long_fast(ErlNifEnv* env, int64_t input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    auto len = mkh_avro::encodeInt64(input, output);
    ret->insert(ret->end(), output.data(), output.data() + len);
    return 0;
}

int encode_float(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    float f;
    double dbl;
    //long unsigned int i;
    
    if (!enif_get_double(env, input, &dbl)) {
        return 3;
    }

    f = dbl;
    auto len = sizeof(float);
    const auto *p = reinterpret_cast<const uint8_t *>(&f);

    //ret->assign(p, p+len);
    ret->insert(ret->end(), p, p + len);
    return 0;

}

int encode_double(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    double dbl;
    
    if (!enif_get_double(env, input, &dbl)) {
        return 4;
    }

    auto len = sizeof(double);
    const auto *p = reinterpret_cast<const uint8_t *>(&dbl);
    //ret->assign(p, p+len);
    ret->insert(ret->end(), p, p + len);
    return 0;
}

int encode_string(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    ErlNifBinary sbin;

    if (!enif_inspect_binary(env, input, &sbin)) {
        return 5;
    }

    auto len = sbin.size;
    auto len2 = mkh_avro::encodeInt64(len, output);
    ret->insert(ret->end(), output.data(), output.data() + len2);
    ret->insert(ret->end(), sbin.data, sbin.data + len);

    return 0;
}

int encode_boolean(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    bool bl;
    
    if (!enif_get_bool(env, input, &bl)) {
        return 6;
    }

    ret->push_back(bl ? 1: 0);
    return 0;
}

inline int enif_get_bool(ErlNifEnv* env, ERL_NIF_TERM term, bool* cond)
{
    char atom[256];
    int  len;
    if ((len = enif_get_atom(env, term, atom, sizeof(atom), ERL_NIF_LATIN1)) == 0) {
        return false;
    }
    
    *cond = (std::strcmp(atom, "false") != 0 && std::strcmp(atom, "nil") != 0);
    
    return true;
}

}
