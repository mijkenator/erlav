#include <erl_nif.h>
#include <cstring>
#include <string>
#include <vector>
#include <stdint.h>
#include "mkh_avro.hh"
#include <string>
#include "include/json.hpp"
#include <fstream>
#include <iostream>
using json = nlohmann::json;

ERL_NIF_TERM encode_i32(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM encode_i64(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM encode_float(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM encode_double(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM encode_bool(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM encode_string(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM supertest(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM testencode(ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM do_encode_int(ErlNifEnv*, int, const ERL_NIF_TERM*);
int enif_get_bool(ErlNifEnv*, ERL_NIF_TERM, bool*);

std::map<int, std::vector<mkh_avro::SchemaItem > > encoders_map;
std::map<std::string, int> schema_map;

int add (int a, int b)
{
    return a + b;
}

ERL_NIF_TERM add_nif(ErlNifEnv* env, int argc, 
    const ERL_NIF_TERM argv[])
{
    int a = 0;
    int b = 0;
    
    if (!enif_get_int(env, argv[0], &a)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_int(env, argv[1], &b)) {
        return enif_make_badarg(env);
    }
    
    int result = add(a, b);
    return enif_make_int(env, result);
}

ERL_NIF_TERM create_encoder_nif(ErlNifEnv* env, int argc, 
    const ERL_NIF_TERM argv[])
{
    ErlNifBinary sbin;
    std::string key;
    int ret;
    
    if (!enif_inspect_binary(env, argv[0], &sbin)) {
        return enif_make_int(env, 0);
    }
    key.assign((const char*)sbin.data, sbin.size);

    if(schema_map.find(key) != schema_map.end()){
        ret = schema_map[key];
        return enif_make_int(env, ret);
    }else{
        ret = schema_map.size() + 1;
        schema_map[key] = ret;
        auto schema = mkh_avro::read_schema(key);
        encoders_map[ret] = schema;
    }

    return enif_make_int(env, ret);
}


ERL_NIF_TERM do_encode_nif(ErlNifEnv* env, int argc, 
    const ERL_NIF_TERM argv[])
{
    int enc_ref = 0;
    
    if (!enif_get_int(env, argv[0], &enc_ref)) {
        return enif_make_badarg(env);
    }
    
    return do_encode_int(env, enc_ref, &argv[1]);
}

ERL_NIF_TERM do_encode_int(ErlNifEnv* env, int schema_id, const ERL_NIF_TERM* input){
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

    auto schema = encoders_map[schema_id];
    
    for( auto it: schema ){
        len = it.fieldName.size();
        enif_alloc_binary(len, &bin);
        const auto *p = reinterpret_cast<const uint8_t *>(it.fieldName.c_str());
        memcpy(bin.data, p, len);
        key = enif_make_binary(env, &bin);

        if(enif_get_map_value(env, *input, key, &val)){
            rv.clear();
            int encodeCode = mkh_avro::encode(it, env, val, &rv);
            if(encodeCode == 0){
                retv.insert(retv.end(), rv.begin(), rv.end());
            }else{
                throw encodeCode;
            }
        }else if(it.defnull == 1){
            retv.push_back(0);
        }
    }

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}

ERL_NIF_TERM encode_nif(ErlNifEnv* env, int argc, 
    const ERL_NIF_TERM argv[])
{
    int enc_type = 0;
    
    if (!enif_get_int(env, argv[0], &enc_type)) {
        return enif_make_badarg(env);
    }
    
    if(1 == enc_type){
        return encode_i32(env, argv[1]);
    }else if(2 == enc_type){
        return encode_i64(env, argv[1]);
    }else if(3 == enc_type){
        return encode_float(env, argv[1]);
    }else if(4 == enc_type){
        return encode_double(env, argv[1]);
    }else if(5 == enc_type){
        return encode_bool(env, argv[1]);
    }else if(6 == enc_type){
        return encode_string(env, argv[1]);
    }else if(7 == enc_type){
        return supertest(env, argv[1]);
    }else if(8 == enc_type){
        return testencode(env, argv[1]);
    }else{
        return enif_make_badarg(env);
    }

}

ERL_NIF_TERM testencode(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary bin;
    ErlNifBinary retbin;
    int len;
    std::vector<uint8_t> retv;
    std::vector<uint8_t> rv;

    if(!enif_is_map(env, input)){
    	return enif_make_badarg(env);
    }
    auto schema = mkh_avro::read_schema("priv/tschema3.avsc");
    
    for( auto it: schema ){
        //std::cout << it.fieldName << '\n' << '\r';
        len = it.fieldName.size();
        enif_alloc_binary(len, &bin);
        const auto *p = reinterpret_cast<const uint8_t *>(it.fieldName.c_str());
        memcpy(bin.data, p, len);
        key = enif_make_binary(env, &bin);

        if(enif_get_map_value(env, input, key, &val)){
            //std::cout << "Getting value ...." << '\n' << '\r';
            rv.clear();
            int encodeCode = mkh_avro::encode(it, env, val, &rv);
            if(encodeCode == 0){
                //std::cout << '\t' << ".... OK " << '\n' << '\r';
                retv.insert(retv.end(), rv.begin(), rv.end());
            }else{
                throw encodeCode;
            }
        }
    }

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}

ERL_NIF_TERM supertest(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ERL_NIF_TERM key;
    ERL_NIF_TERM val;
    ErlNifBinary bin;
    ErlNifBinary retbin;
    int len;
    std::vector<uint8_t> retv;
    std::vector<uint8_t> rv;

    if(!enif_is_map(env, input)){
    	return enif_make_badarg(env);
    }
    auto schema = mkh_avro::read_schema("priv/tschema2.avsc");
    
    for( auto it: schema ){
        std::cout << it.fieldName << '\n' << '\r';
        len = it.fieldName.size();
        enif_alloc_binary(len, &bin);
        const auto *p = reinterpret_cast<const uint8_t *>(it.fieldName.c_str());
        memcpy(bin.data, p, len);
        key = enif_make_binary(env, &bin);

        if(enif_get_map_value(env, input, key, &val)){
            std::cout << "Getting value ...." << '\n' << '\r';
            rv.clear();
            int encodeCode = mkh_avro::encode(it, env, val, &rv);
            if(encodeCode == 0){
                std::cout << '\t' << ".... OK " << '\n' << '\r';
                retv.insert(retv.end(), rv.begin(), rv.end());
            }else{
                throw encodeCode;
            }
        }
    }

    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    binary = enif_make_binary(env, &retbin);
    return binary;
}

ERL_NIF_TERM encode_string(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    ErlNifBinary sbin;
    std::string strt;
    std::array<uint8_t, 10> output;

    if (!enif_inspect_binary(env, input, &sbin)) {
        return enif_make_badarg(env);
    }
    strt.assign((const char*)sbin.data, sbin.size);

    auto len = strt.size();
    const auto *p = reinterpret_cast<const uint8_t *>(strt.c_str());

    auto len2 = mkh_avro::encodeInt64(len, output);

    enif_alloc_binary(len+len2, &bin);
    memcpy(bin.data, output.data(), len2);
    memcpy(bin.data+len2, p, len);
    binary = enif_make_binary(env, &bin);
    return binary;
}

ERL_NIF_TERM encode_bool(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    bool bl;
    std::array<uint8_t, 1> output;
    
    if (!enif_get_bool(env, input, &bl)) {
        return enif_make_badarg(env);
    }

    output[0] = bl ? 1: 0;
    enif_alloc_binary(1, &bin);
    memcpy(bin.data, output.data(), 1);
    binary = enif_make_binary(env, &bin);
    return binary;
}

ERL_NIF_TERM encode_float(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    float f;
    double dbl;
    
    if (!enif_get_double(env, input, &dbl)) {
        return enif_make_badarg(env);
    }

    f = dbl;
    auto len = sizeof(float);
    const auto *p = reinterpret_cast<const uint8_t *>(&f);


    enif_alloc_binary(len, &bin);
    memcpy(bin.data, p, len);
    binary = enif_make_binary(env, &bin);
    return binary;
}

ERL_NIF_TERM encode_double(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    double dbl;
    
    if (!enif_get_double(env, input, &dbl)) {
        return enif_make_badarg(env);
    }

    auto len = sizeof(double);
    const auto *p = reinterpret_cast<const uint8_t *>(&dbl);


    enif_alloc_binary(len, &bin);
    memcpy(bin.data, p, len);
    binary = enif_make_binary(env, &bin);
    return binary;
}

ERL_NIF_TERM encode_i32(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    std::array<uint8_t, 5> output;
    int32_t i32;
    
    if (!enif_get_int(env, input, &i32)) {
        return enif_make_badarg(env);
    }

    auto len = mkh_avro::encodeInt32(i32, output);
    enif_alloc_binary(len, &bin);
    memcpy(bin.data, output.data(), len);
    binary = enif_make_binary(env, &bin);
    return binary;
}

ERL_NIF_TERM encode_i64(ErlNifEnv* env, ERL_NIF_TERM input){
    ERL_NIF_TERM binary;
    ErlNifBinary bin;
    std::array<uint8_t, 10> output;
    int64_t i64;

    if (!enif_get_int64(env, input, &i64)) {
        return enif_make_badarg(env);
    }

    auto len = mkh_avro::encodeInt64(i64, output);
    enif_alloc_binary(len, &bin);
    memcpy(bin.data, output.data(), len);
    binary = enif_make_binary(env, &bin);
    return binary;
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

ErlNifFunc nif_funcs[] = 
{
    {"add", 2, add_nif},
    {"encode", 2, encode_nif},
    {"create_encoder", 1, create_encoder_nif},
    {"do_encode", 2, do_encode_nif}
};

ERL_NIF_INIT(erlav_nif, nif_funcs, nullptr, 
    nullptr, nullptr, nullptr);
