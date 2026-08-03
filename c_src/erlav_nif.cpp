#include <cstring>
#include <fstream>
#include <iostream>
#include <shared_mutex>
#include <stdint.h>
#include <string>
#include <vector>

#include "include/json.hpp"
#include <erl_nif.h>

#include "mkh_avro2.hh"

using json = nlohmann::json;

std::map<int, mkh_avro2::SchemaItem*> erlav_encoders_map;
std::map<std::string, int> erlav_schema_map;

// Guards both global maps above. BEAM schedulers can call erlav_init/1
// concurrently from different threads (nothing in OTP serializes NIF calls
// across schedulers), and plain std::map has no built-in thread safety --
// concurrent find/insert on either map is a data race. erlav_init_nif is the
// only writer (registers a new schema) and takes a unique_lock; the
// encode/decode NIFs only ever read a schema pointer for a given id (via
// find(), never operator[] -- operator[] would insert a null entry for an
// unknown id, turning a lookup into a write) and take a shared_lock, so
// concurrent encode/decode calls run fully in parallel with each other and
// only block against the rare init-time write.
static std::shared_mutex erlav_schema_mutex;

// Looks up a previously-registered schema by id without ever mutating the
// map (unlike erlav_encoders_map[enc_ref], which inserts a null entry for an
// unknown id). Returns nullptr if enc_ref is not a valid schema id -- callers
// must check before dereferencing.
mkh_avro2::SchemaItem*
find_schema(int enc_ref) {
    std::shared_lock<std::shared_mutex> lock(erlav_schema_mutex);
    auto it = erlav_encoders_map.find(enc_ref);
    return it == erlav_encoders_map.end() ? nullptr : it->second;
}

// Builds the {error, Msg, Code} tuple shared by every failure path in this
// file -- erlav_init_nif and erlav_encode_nif both need the exact same
// shape, so callers on the Erlang side can branch on one consistent
// contract ({error, _, _} means failure) instead of each NIF having its own
// ad hoc error representation.
ERL_NIF_TERM
make_error_tuple(ErlNifEnv* env, const std::string& msg, int code) {
    ERL_NIF_TERM t1 = enif_make_atom(env, "error");
    ERL_NIF_TERM t2 = enif_make_string(env, msg.c_str(), ERL_NIF_LATIN1);
    ERL_NIF_TERM t3 = enif_make_int(env, code);
    return enif_make_tuple3(env, t1, t2, t3);
}

ERL_NIF_TERM
erlav_init_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary sbin;
    std::string key;
    int ret;

    if (!enif_inspect_binary(env, argv[0], &sbin)) {
        // Previously returned the bare integer 0 here, indistinguishable
        // from "handle 0" by any caller that only checks "is it a bare
        // int" -- now returns the same {error, Msg, Code} shape as every
        // other failure below, so erlav_init/1 has one consistent success
        // (integer id >= 1) / failure ({error, _, _}) contract.
        return make_error_tuple(env, "erlav_init: argument is not a binary", 100);
    }
    key.assign((const char*) sbin.data, sbin.size);

    std::unique_lock<std::shared_mutex> lock(erlav_schema_mutex);

    auto found = erlav_schema_map.find(key);
    if (found != erlav_schema_map.end()) {
        return enif_make_int(env, found->second);
    }

    // Schema parsing (read_schema -> mkh_avro2::read_schema -> json::parse
    // + resolve_user_types + the SchemaItem tree walk) was previously
    // completely unguarded here, unlike erlav_encode_nif's try/catch a few
    // lines down -- a malformed .avsc file (bad JSON, or one that fails the
    // hardcoded well-formed-schema assumptions in schema_item.hh, e.g. its
    // "Not implemented" branches) threw a raw C++ exception straight across
    // the NIF boundary. That's undefined behavior per the Erlang NIF API
    // (NIFs must not let a C++ exception escape into the calling BEAM
    // scheduler) and can crash the whole VM, not just fail this call.
    try {
        ret = static_cast<int>(erlav_schema_map.size()) + 1;
        auto* schema = mkh_avro2::read_schema(key);
        // Only register the new id once parsing has fully succeeded --
        // registering `key` first and filling in erlav_encoders_map after
        // (the previous order) meant a throw here left erlav_schema_map
        // pointing at an id with no matching encoder, so every future
        // erlav_init/1 call for this same filename would keep returning
        // that broken id instead of retrying (e.g. after the .avsc file on
        // disk is fixed).
        erlav_schema_map[key] = ret;
        erlav_encoders_map[ret] = schema;
    } catch (nlohmann::json::exception const& je) {
        return make_error_tuple(env, std::string("erlav_init: ") + je.what(), 101);
    } catch (std::exception const& e) {
        // Covers schema_item.hh's throw std::runtime_error("Not implemented
        // ...") for schema constructs read_schema doesn't support, plus any
        // other standard-library exception the parse/build path might
        // raise.
        return make_error_tuple(env, std::string("erlav_init: ") + e.what(), 102);
    } catch (...) {
        return make_error_tuple(env, "erlav_init: unknown error", 103);
    }

    return enif_make_int(env, ret);
}

ERL_NIF_TERM
erlav_encode_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    int enc_ref = 0;

    if (!enif_get_int(env, argv[0], &enc_ref)) {
        return enif_make_badarg(env);
    }
    auto* schema = find_schema(enc_ref);
    if (schema == nullptr) {
        // Unknown schema id -- previously this fell through to encode()
        // with a null SchemaItem* (via erlav_encoders_map[enc_ref], which
        // silently inserts a null entry for a missing key) and crashed on
        // first dereference. Fail explicitly instead.
        return enif_make_badarg(env);
    }
    try {
        auto ret = mkh_avro2::encode(env, schema, &argv[1]);
        return ret;
    } catch (int x) {
        return enif_make_int(env, x);
    } catch (mkh_avro::AvroException const& ae) {
        return make_error_tuple(env, ae.message, ae.code);
    } catch (std::out_of_range const& ofr) {
        return make_error_tuple(env, ofr.what(), 9990);
    } catch (...) {
        return make_error_tuple(env, "unknown error", 9991);
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

    auto* schema = find_schema(enc_ref);
    if (schema == nullptr) {
        return enif_make_badarg(env);
    }

    std::vector<uint8_t> encdata(sbin.data, sbin.data + sbin.size);
    //std::cout << "encdata vector length: " <<  encdata.size() << "\r\n";
    std::vector<uint8_t>::iterator it = encdata.begin();

    ret_map = mkh_avro2::decode(env, schema, it);

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

    auto* schema = find_schema(enc_ref);
    if (schema == nullptr) {
        return enif_make_badarg(env);
    }

    uint8_t* p = sbin.data;

    ret_map = mkh_avro2::decode(env, schema, p);

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
