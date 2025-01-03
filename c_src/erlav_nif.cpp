#include <cstring>
#include <fstream>
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "include/json.hpp"
#include <erl_nif.h>

#include "mkh_avro2.hh"

using json = nlohmann::json;

std::map<int, mkh_avro2::SchemaItem*> erlav_encoders_map;
std::map<std::string, int> erlav_schema_map;

ERL_NIF_TERM
erlav_init_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary sbin;
    std::string key;
    int ret;

    if (!enif_inspect_binary(env, argv[0], &sbin)) {
        return enif_make_int(env, 0);
    }
    key.assign((const char*) sbin.data, sbin.size);

    if (erlav_schema_map.find(key) != erlav_schema_map.end()) {
        ret = erlav_schema_map[key];
        return enif_make_int(env, ret);
    } else {
        ret = erlav_schema_map.size() + 1;
        erlav_schema_map[key] = ret;
        auto schema = mkh_avro2::read_schema(key);
        erlav_encoders_map[ret] = schema;
    }

    return enif_make_int(env, ret);
}

ERL_NIF_TERM
erlav_encode_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    int enc_ref = 0;

    if (!enif_get_int(env, argv[0], &enc_ref)) {
        return enif_make_badarg(env);
    }
    try {
        auto ret =
            mkh_avro2::encode(env, erlav_encoders_map[enc_ref], &argv[1]);
        return ret;
    } catch (int x) {
        return enif_make_int(env, x);
    } catch (mkh_avro::AvroException const& ae) {
        ERL_NIF_TERM t1 = enif_make_atom(env, "error");
        ERL_NIF_TERM t2 =
            enif_make_string(env, &(ae.message[0]), ERL_NIF_LATIN1);
        ERL_NIF_TERM t3 = enif_make_int(env, ae.code);
        return enif_make_tuple3(env, t1, t2, t3);
    } catch (std::out_of_range const& ofr) {
        ERL_NIF_TERM t1 = enif_make_atom(env, "error");
        std::string wstr = ofr.what();
        ERL_NIF_TERM t2 =
            enif_make_string(env, wstr.c_str(), ERL_NIF_LATIN1);
        ERL_NIF_TERM t3 = enif_make_int(env, 9990);
        return enif_make_tuple3(env, t1, t2, t3);
    } catch (...) {
        ERL_NIF_TERM t1 = enif_make_atom(env, "error");
        ERL_NIF_TERM t2 =
            enif_make_string(env, "unknown error", ERL_NIF_LATIN1);
        ERL_NIF_TERM t3 = enif_make_int(env, 9991);
        return enif_make_tuple3(env, t1, t2, t3);
    }
    return enif_make_int(env, -1);
}

ERL_NIF_TERM
erlav_decode_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary sbin;
    ERL_NIF_TERM ret_map = enif_make_new_map(env);
    int enc_ref = 0;

    if (!enif_get_int(env, argv[0], &enc_ref)) {
        return enif_make_badarg(env);
    }

    if (!enif_inspect_binary(env, argv[1], &sbin)) {
        return ret_map;
    }

    std::vector<uint8_t> encdata(sbin.data, sbin.data + sbin.size);
    //std::cout << "encdata vector length: " <<  encdata.size() << "\r\n";
    std::vector<uint8_t>::iterator it = encdata.begin();

    ret_map = mkh_avro2::decode(env, erlav_encoders_map[enc_ref], it);

    //std::cout << "decode done\r\n";
/*
    int64_t ret1 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret1 << "\r\n";
    int64_t ret2 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret2 << "\r\n";
    int64_t ret3 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret3 << "\r\n";
    int64_t ret4 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret4 << "\r\n";
    int64_t ret5 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret5 << "\r\n";
    int64_t ret6 = mkh_avro2::decodeLong(it);
    std::cout << "value: " <<  ret6 << "\r\n";
*/

    return ret_map;
}

ERL_NIF_TERM
erlav_decode_nif_fast(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary sbin;
    ERL_NIF_TERM ret_map = enif_make_new_map(env);
    int enc_ref = 0;

    if (!enif_get_int(env, argv[0], &enc_ref)) {
        return enif_make_badarg(env);
    }

    if (!enif_inspect_binary(env, argv[1], &sbin)) {
        return ret_map;
    }

    uint8_t* p = sbin.data;

    ret_map = mkh_avro2::decode(env, erlav_encoders_map[enc_ref], p);

    return ret_map;
}

ERL_NIF_TERM
int_encode_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary retbin;
    std::vector<uint8_t> retv;
    unsigned int len;
    long i64;
    ERL_NIF_TERM elem, list;
    retv.reserve(100);

    list = argv[0];
    if (!enif_is_list(env, list)) {
        return enif_make_badarg(env);
    }
    enif_get_list_length(env, list, &len);
    for (uint32_t i = 0; i < len; i++) {
        if (enif_get_list_cell(env, list, &elem, &list)) {
            enif_get_int64(env, elem, &i64);
            mkh_avro2::encodeVarint(i64, retv);
        }
    }


    auto retlen = retv.size();
    enif_alloc_binary(retlen, &retbin);
    memcpy(retbin.data, retv.data(), retlen);
    return enif_make_binary(env, &retbin);
}

ERL_NIF_TERM
int_decode_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary sbin;
    std::vector<ERL_NIF_TERM> retv;

    if (!enif_inspect_binary(env, argv[0], &sbin)) {
        return enif_make_badarg(env);
    }
    uint8_t* p = sbin.data;
    uint8_t* end = sbin.data + sbin.size;
    while ( p < end ){
        uint64_t ri = mkh_avro2::decodeVarint(p);
        retv.push_back(enif_make_int64(env, ri));
    }

    return enif_make_list_from_array(env, retv.data(), retv.size());
}

ErlNifFunc nif_funcs[] = {{"erlav_init", 1, erlav_init_nif},
                          {"erlav_encode", 2, erlav_encode_nif},
                          {"erlav_decode_fast", 2, erlav_decode_nif_fast},
                          {"int_encode", 1, int_encode_nif},
                          {"int_decode", 1, int_decode_nif},
                          {"erlav_decode", 2, erlav_decode_nif}};

ERL_NIF_INIT(erlav_nif, nif_funcs, nullptr, nullptr, nullptr, nullptr);
