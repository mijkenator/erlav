#include <array>
#include <iostream>
#include <fstream>
#include "include/json.hpp"
using json = nlohmann::json;

namespace mkh_avro {

int encode_int(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_long(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_float(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_double(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_string(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int encode_boolean(ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);
int enif_get_bool(ErlNifEnv*, ERL_NIF_TERM, bool*);
int encode_primitive(std::string, ErlNifEnv*, ERL_NIF_TERM, std::vector<uint8_t>*);

struct SchemaItem{
    std::string fieldName;
    std::vector<std::string> fieldTypes;
    bool nullable;

    SchemaItem(std::string name, std::string ftype){
        fieldName = name;
        fieldTypes.push_back(ftype);
        nullable = 0;
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
        std::cout << it["name"] << "___" << it["type"] << '\n' << '\r';
        SchemaItem si(it["name"], it["type"]);
        schema.push_back(si);
    }

    return schema;
}

int encode(std::vector<std::string > atypes, ErlNifEnv* env, ERL_NIF_TERM term, std::vector<uint8_t>* ret){
    auto alen = atypes.size();
    if(alen == 1){
        return encode_primitive(atypes[0], env, term, ret);
    }else{
        return encode_primitive(atypes[0], env, term, ret);
    }
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
    long unsigned int i;
    
    if (!enif_get_int(env, input, &i32)) {
        return 1;
    }

    auto len = mkh_avro::encodeInt32(i32, output);
    for(i = 0; i < len; i++){
        ret->push_back(output[i]);
    }
    return 0;
}

int encode_long(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    std::array<uint8_t, 10> output;
    int64_t i64;
    long unsigned int i;
    
    if (!enif_get_int64(env, input, &i64)) {
        return 2;
    }

    auto len = mkh_avro::encodeInt64(i64, output);
    for(i = 0; i < len; i++){
        ret->push_back(output[i]);
    }
    return 0;
}

int encode_float(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    float f;
    double dbl;
    //long unsigned int i;
    
    std::cout << "EF1" << '\n' << '\r';
    if (!enif_get_double(env, input, &dbl)) {
        return 3;
    }

    f = dbl;
    std::cout << "EF2" << f << '\n' << '\r';
    auto len = sizeof(float);
    const auto *p = reinterpret_cast<const uint8_t *>(&f);

    std::cout << "EF3:" << len << '\n' << '\r';
    ret->assign(p, p+len);
    std::cout << "EF4" << '\n' << '\r';
    return 0;

}

int encode_double(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    double dbl;
    
    if (!enif_get_double(env, input, &dbl)) {
        return 4;
    }

    auto len = sizeof(double);
    const auto *p = reinterpret_cast<const uint8_t *>(&dbl);
    ret->assign(p, p+len);
    return 0;
}

int encode_string(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    std::string strt;
    std::array<uint8_t, 10> output;
    ErlNifBinary sbin;

    if (!enif_inspect_binary(env, input, &sbin)) {
        return 5;
    }
    strt.assign((const char*)sbin.data, sbin.size);

    auto len = strt.size();
    const auto *p = reinterpret_cast<const uint8_t *>(strt.c_str());

    auto len2 = mkh_avro::encodeInt64(len, output);
    auto plen = len + len2;

    auto *p_array = new uint8_t[plen];
    memcpy(p_array, output.data(), len2);
    memcpy(p_array+len2, p, len);
    ret->assign(p_array, p_array+plen);
    delete[] p_array;

    return 0;
}

int encode_boolean(ErlNifEnv* env, ERL_NIF_TERM input, std::vector<uint8_t>* ret){
    bool bl;
    
    std::cout << "EB0:" << '\n' << '\r';
    if (!enif_get_bool(env, input, &bl)) {
        return 6;
    }
    std::cout << "EB1:" << '\n' << '\r';

    ret->push_back(bl ? 1: 0);
    std::cout << "EB2:" << '\n' << '\r';
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
