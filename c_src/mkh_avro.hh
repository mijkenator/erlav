#include <array>
#include <iostream>
#include <fstream>
#include "include/json.hpp"
using json = nlohmann::json;

namespace mkh_avro {

struct SchemaItem{
    std::string fieldName;
    std::vector<std::string> fieldTypes;
    bool nullable;

    SchemaItem(std::string name, std::string type){
        fieldName = name;
        fieldTypes.push_back(type);
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

std::vector<uint8_t> encode(std::string atype, ErlNifEnv* env, ERL_NIF_TERM term){
    if("int" == atype){

    }else if("long" == atype){

    }else if("float" == atype){

    }else if("double" == atype){

    }else if("string" == atype){

    }else if("bool" == atype){

    }

}

std::vector<uint8_t> encode_int(ErlNifEnv* env, ERL_NIF_TERM input){
    std::array<uint8_t, 5> output;
    std::vector<uint8_t> ret;
    int32_t i32;
    
    if (!enif_get_int(env, input, &i32)) {
        return enif_make_badarg(env);
    }

    auto len = mkh_avro::encodeInt32(i32, output);
    return ret;
}

}
