#include "mkh_avro_decoder.hh"


namespace mkh_avro2 {



ERL_NIF_TERM  decode(ErlNifEnv* env, SchemaItem* si, std::vector<uint8_t>::iterator& it) {
    ERL_NIF_TERM ret = enif_make_new_map(env);
    ERL_NIF_TERM map_out;

    for(SchemaItem* si_e : si->childItems){
        //std::cout << "Field: " << si_e->obj_name << "  | type: " << si_e->obj_type << " | scalar_type: " << si_e->scalar_type  << "\r\n";
        if(si_e->obj_type == 0 && si_e->scalar_type >= 0){ // decode simple scalar
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_scalar(env, si_e->scalar_type, it);
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        }
    }

    return ret;
}

ERL_NIF_TERM decode_scalar(ErlNifEnv* env, int sctype, std::vector<uint8_t>::iterator& it) {
    if((sctype == 1) || (sctype == 0)){
        int64_t ret = decodeLong(it);
        ErlNifSInt64 retn = ret;
        return enif_make_int64(env, retn);
    }else if(sctype == 2){ // double
        double ret = decode_double(it);
        return enif_make_double(env, ret);
    }else if(sctype == 3){ // float
        float ret = decode_float(it);
        double retd = ret;
        return enif_make_double(env, retd);
    }
    return enif_make_badarg(env);
}

int64_t decodeLong(std::vector<uint8_t>::iterator& it) {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;

    do {
        if (shift >= 64) {
            throw std::invalid_argument("Invalid Avro varint");
        }
        u = *it;
        ++it;
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return decodeZigzag64(encoded);
}

float decode_float(std::vector<uint8_t>::iterator& it){
    std::array<uint8_t, sizeof(float)> vect2;
    for (long unsigned int i=0; i < sizeof(float); i++) {
        vect2[i] = *it;
        ++it;
    }
    const float* ret = reinterpret_cast<const float*>(&vect2);
    return *ret;
}

double decode_double(std::vector<uint8_t>::iterator& it){
    std::array<uint8_t, sizeof(double)> vect2;
    for (long unsigned int i=0; i < sizeof(double); i++) {
        vect2[i] = *it;
        ++it;
    }
    const double* ret = reinterpret_cast<const double*>(&vect2);
    return *ret;
}

//
//  ------------------------ pointer functions
//


ERL_NIF_TERM  decode(ErlNifEnv* env, SchemaItem* si, uint8_t*& it) {
    ERL_NIF_TERM ret = enif_make_new_map(env);
    ERL_NIF_TERM map_out;

    for(SchemaItem* si_e : si->childItems){
        std::cout << "Field: " << si_e->obj_name << "  | type: " << si_e->obj_type << " | scalar_type: " << si_e->scalar_type  << "\r\n";
        if(si_e->obj_type == 0 && si_e->scalar_type >= 0){ // decode simple scalar
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_scalar(env, si_e->scalar_type, it);
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        }else if(si_e->obj_type == 1){
            std::cout << "UNION TYPE!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            if(si_e->childItems.size() == 1){
                // just nullable scalar
                std::cout << "CHILDITEMS111 size: " << si_e->childItems.size() << "\r\n"; 
                //value = decode_nullable_scalar(env, si_e->childItems[0]->scalar_type, it);
                value = decode_union(env, si_e, it);
            }else{
                // real union
                std::cout << "CHILDITEMS size: " << si_e->childItems.size() << "\r\n"; 
                value = decode_union(env, si_e, it);
            }
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        } else if(si_e->obj_type == 2) {
            std::cout << "ARRAY TYPE!!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_array(env, si_e, it);
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        } else if(si_e->obj_type == 3) {
            std::cout << "RECORD TYPE!!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_record(env, si_e, it);
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        }
    }

    return ret;
}

ERL_NIF_TERM decodevalue(ErlNifEnv* env, SchemaItem* si, uint8_t*& it) {
    switch (si->obj_type) {
        case 0:
            return decode_scalar(env, si->scalar_type, it);
        case 1:
            return decode_union(env, si, it);
        case 2:
            return decode_array(env, si, it);
        case 3:
            return decode_record(env, si, it);
//        case 4:
//            return encodemap(si, env, val, ret);
//        case 5:
//            return encodeenum(si, env, val, ret);
        default:
            std::cout << "ENCODE VALUE!!!\n\r";
    }
    return 0;
}

ERL_NIF_TERM decode_record(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    std::cout << "Decode record" << " \r\n";
    return decode(env, si, it);
}

ERL_NIF_TERM decode_array(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    uint32_t arrayLen = decodeLong(it);
    std::vector<ERL_NIF_TERM> decoded_list;
    decoded_list.reserve(arrayLen);
    std::cout << "Array length --> " << arrayLen << " \r\n";
    if (si->obj_field != "complex") {
        auto st = get_scalar_type(si->obj_field);
        std::cout << "simple array: type --> " << st << " \r\n";
        for(uint64_t i=0; i < arrayLen; i++){
            decoded_list.push_back(decode_scalar(env, st, it));
        }
        return enif_make_list_from_array(env, decoded_list.data(), arrayLen);

    } else if ((si->obj_field == "complex") && si->array_type == 1) {
        std::cout << "complex ARRAY \r\n";
    } else {
        // complex array - no support for union types yet
        std::cout << "complex ARRAY uniom type \r\n";
    }

    return enif_make_atom(env, "undefined"); 
}

ERL_NIF_TERM decode_union(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    uint32_t childNumber = decodeInt32(it);
    if(si->is_nullable == 1){
        std::cout << "Child number: " << childNumber << "\r\n";
        if(childNumber == 0){
            return enif_make_atom(env, "undefined"); 
        }else{
            auto sctype = si->childItems[childNumber - 1]->scalar_type;
            std::cout << "SCTYPE: " << sctype << "\r\n";
            if(sctype >= 0){
                std::cout << "DECODE SCALAR" << "\r\n";
                return decode_scalar(env, sctype, it);
            } else {
                return decodevalue(env, si->childItems[childNumber - 1], it);
            }
        }
    } else {
        auto sctype = si->childItems[childNumber]->scalar_type;
        std::cout << "SCTYPE: " << sctype << "\r\n";
        if(sctype >= 0){
            std::cout << "DECODE SCALAR" << "\r\n";
            return decode_scalar(env, sctype, it);
        } else {
            return decodevalue(env, si->childItems[childNumber], it);
        }
    }
    return enif_make_atom(env, "undefined"); 
}

ERL_NIF_TERM decode_nullable_scalar(ErlNifEnv* env, int sctype, uint8_t*& it) {
    auto union_byte = *it;
    it++;
    if(union_byte == 0){
        return enif_make_atom(env, "undefined"); 
    } else {
        return decode_scalar(env, sctype, it);
    }
}

ERL_NIF_TERM decode_scalar(ErlNifEnv* env, int sctype, uint8_t*& it) {
    if((sctype == 1) || (sctype == 0)){
        int64_t ret = decodeLong(it);
        ErlNifSInt64 retn = ret;
        return enif_make_int64(env, retn);
    }else if(sctype == 2){ // double
        double ret = decode_double(it);
        return enif_make_double(env, ret);
    }else if(sctype == 3){ // float
        float ret = decode_float(it);
        double retd = ret;
        return enif_make_double(env, retd);
    }else if(sctype == 4){ // boolean
        return decode_boolean(env, it);
    }else if(sctype == 5){ // string
        return decode_string(env, it);
    }else if(sctype == 6){ // bytes
        return decode_string(env, it);
    }
    return enif_make_badarg(env);
}

ERL_NIF_TERM decode_boolean(ErlNifEnv* env, uint8_t*& it) {
    std::string atom;
    uint8_t bool_val = *it;
    it++;
    if(bool_val == 1){ //true
        atom = "true";
        return enif_make_atom_len(env, atom.c_str(), 4);
    } else { //false
        atom = "false";
        return enif_make_atom_len(env, atom.c_str(), 5);
    }
}

ERL_NIF_TERM decode_string(ErlNifEnv* env, uint8_t*& it) {
    ERL_NIF_TERM str_ret;
    
    auto len = decodeLong(it);
    std::cout << "decode string with length :" << len << "\r\n";

    unsigned char* str_data;
    str_data = enif_make_new_binary(env, len, &str_ret);
    memcpy(str_data, it, len);
    it += len;

    return str_ret;
}

int64_t decodeLong(uint8_t*& it) {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;

    do {
        if (shift >= 64) {
            throw std::invalid_argument("Invalid Avro varint");
        }
        u = *it;
        ++it;
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return decodeZigzag64(encoded);
}

int32_t decodeInt32(uint8_t*& it) {
    uint32_t encoded = 0;
    int shift = 0;
    uint8_t u;

    do {
        if (shift >= 32) {
            throw std::invalid_argument("Invalid Avro varint");
        }
        u = *it;
        ++it;
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return decodeZigzag32(encoded);
}

int64_t decodeVarint(uint8_t*& it) {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;

    do {
        if (shift >= 64) {
            throw std::invalid_argument("Invalid Avro varint");
        }
        u = *it;
        ++it;
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return encoded;
}

float decode_float(uint8_t*& it){
    std::array<uint8_t, sizeof(float)> vect2;
    for (long unsigned int i=0; i < sizeof(float); i++) {
        vect2[i] = *it;
        ++it;
    }
    const float* ret = reinterpret_cast<const float*>(&vect2);
    return *ret;
}

double decode_double(uint8_t*& it){
    std::array<uint8_t, sizeof(double)> vect2;
    for (long unsigned int i=0; i < sizeof(double); i++) {
        vect2[i] = *it;
        ++it;
    }
    const double* ret = reinterpret_cast<const double*>(&vect2);
    return *ret;
}

}
