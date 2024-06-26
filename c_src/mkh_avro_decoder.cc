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
    std::cout << "decode funcs!!!!!" << "\r\n";

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
        }else if(si_e->obj_type == 1){
            //std::cout << "UNION TYPE!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            if(si_e->childItems.size() == 1){
                // just nullable scalar OR nullable array,record, map....
                //std::cout << "CHILDITEMS111 size: " << si_e->childItems.size() << "\r\n"; 
                //value = decode_nullable_scalar(env, si_e->childItems[0]->scalar_type, it);
                value = decode_union(env, si_e, it);
            }else{
                // real union
                //std::cout << "CHILDITEMS size: " << si_e->childItems.size() << "\r\n"; 
                value = decode_union(env, si_e, it);
            }
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        } else if(si_e->obj_type == 2) {
            //std::cout << "ARRAY TYPE!!!!" << "\r\n";
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
            //std::cout << "RECORD TYPE!!!!" << "\r\n";
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
        } else if(si_e->obj_type == 4) {
            //std::cout << "MAP TYPE!!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_map(env, si_e, it);
            if(enif_make_map_put(env, ret, key, value, &map_out)){
                ret = map_out;
            }
        } else if(si_e->obj_type == 5) {
            std::cout << "ENUM TYPE!!!!" << "\r\n";
            ERL_NIF_TERM value;
            ERL_NIF_TERM key;
            unsigned char* key_data;
            auto len = si_e->obj_name.length();
            key_data = enif_make_new_binary(env, len, &key);
            memcpy(key_data, si_e->obj_name.c_str(), len);
            value = decode_enum(env, si_e, it);
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
        case 4:
            return decode_map(env, si, it);
        case 5: // enym
            return decode_enum(env, si, it);
        default:
            std::cout << "DECODE VALUE!!!" << std::to_string(si->obj_type) << "\n\r";
    }
    return 0;
}

ERL_NIF_TERM decode_record(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    //std::cout << "Decode record" << " \r\n";
    return decode(env, si, it);
}

ERL_NIF_TERM decode_enum(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    ERL_NIF_TERM str_ret;
    uint32_t valueNumber = decodeInt32(it);
    std::cout << "ItemNumber :" << std::to_string(valueNumber) << "\r\n";
    auto val = si->array_multi_type_reverse.at(valueNumber);
    std::cout << "ItemValue :" << val << "\r\n";

    auto len = val.length();
    unsigned char* str_data;
    str_data = enif_make_new_binary(env, len, &str_ret);
    memcpy(str_data, val.c_str(), len);

    return str_ret;
}

ERL_NIF_TERM decode_map(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    ERL_NIF_TERM ret = enif_make_new_map(env);
    ERL_NIF_TERM map_out;
    uint32_t mapLen = decodeLong(it);
    //std::cout << "MAP length --> " << mapLen << " \r\n";
    //auto child_len = si->childItems.size();
    //std::cout << "child len:" << child_len << "\r\n";
    if (si->obj_field != "complex") { // map of scalars
        for(uint64_t i=0; i < mapLen; i++){
            ERL_NIF_TERM map_key = decode_scalar(env, 5, it); // map keys always strings, (scalar_type = 6)
            ERL_NIF_TERM value = decode_scalar(env, si->scalar_type, it);
            if(enif_make_map_put(env, ret, map_key, value, &map_out)){
                ret = map_out;
            }
        }
    }else{
        for(uint64_t i=0; i < mapLen; i++){
            ERL_NIF_TERM map_key = decode_scalar(env, 5, it); // map keys always strings, (scalar_type = 6)
            ERL_NIF_TERM value = decodevalue(env, si->childItems[0], it);
            if(enif_make_map_put(env, ret, map_key, value, &map_out)){
                ret = map_out;
            }
        }

    }
    if(mapLen > 0){
        it++;
    }

    return ret; 
}

ERL_NIF_TERM decode_array(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    uint32_t arrayLen = decodeLong(it);
    std::vector<ERL_NIF_TERM> decoded_list;
    decoded_list.reserve(arrayLen);
    /*std::cout << "DA Array length --> " << arrayLen << " \r\n";
    std::cout << "complex ARRAY union type " << si->array_type << "\r\n";
    std::cout << "si->obj_type:" << std::to_string(si->obj_type) << "\r\n";
    std::cout << "si->scalar_type:" << std::to_string(si->scalar_type) << "\r\n";
    std::cout << "si->obj_field:" << si->obj_field << "\r\n";
    std::cout << "si->obj_simple_type:" << std::to_string(si->obj_simple_type) << "\r\n";
    std::cout << "si->array_type:" << std::to_string(si->array_type) << "\r\n";
    auto child_len = si->childItems.size();
    std::cout << "child len:" << child_len << "\r\n";
    std::cout << "----------------------------------- \r\n";*/

    if (si->obj_field != "complex") {
        auto st = get_scalar_type(si->obj_field);
        //std::cout << "simple array: type --> " << st << " \r\n";
        for(uint64_t i=0; i < arrayLen; i++){
            decoded_list.push_back(decode_scalar(env, st, it));
        }
        it++; // skip end of array, should be 0
        return enif_make_list_from_array(env, decoded_list.data(), arrayLen);

    } else if ((si->obj_field == "complex") && si->array_type == 1) {
        //std::cout << "complex ARRAY \r\n";
        for(uint64_t i=0; i < arrayLen; i++){
            int64_t type_index = decodeLong(it);
            //std::cout << "Type index:" << std::to_string(type_index) << "\r\n";
            std::string eletype = si->array_multi_type_reverse[type_index];
            //std::cout << "Type name:" << eletype << "\r\n";
            if((eletype == "string")||(eletype == "long")||(eletype == "double")){
                decoded_list.push_back(decode_scalar(env, get_scalar_type(eletype), it));
            }else if(eletype == "array"){
                decoded_list.push_back(decode_array(env, si->childItems[0], it));
            }
        }
        it++; // skip end of array, should be 0
        return enif_make_list_from_array(env, decoded_list.data(), arrayLen);
    } else {
        // complex array - no support for union types yet
        //std::cout << "complex array 2 \r\n";
        auto child_len = si->childItems.size();
        //std::cout << "child scalar_type: " << std::to_string(si->childItems[0]->scalar_type) << "\r\n";
        //std::cout << "child obj_field: " << si->childItems[0]->obj_field << "\r\n";

        if((child_len == 1) && (si->childItems[0]->scalar_type > 0) && (si->childItems[0]->obj_field != "complex")){
            // array of simple arrays
            for(uint64_t i=0; i < arrayLen; i++){
                decoded_list.push_back(decode_array(env, si->childItems[0], it));
            }
            it++; // skip end of array, should be 0
            return enif_make_list_from_array(env, decoded_list.data(), arrayLen);

        }else if(child_len == 1){
            // array of 1 complex type
            if(si->childItems[0]->obj_type == 2){
                for(uint64_t i=0; i < arrayLen; i++){
                    decoded_list.push_back(decode_array(env, si->childItems[0], it));
                }
            }else{
                for(uint64_t i=0; i < arrayLen; i++){
                    decoded_list.push_back(decode(env, si->childItems[0], it));
                }
            }
            it++; // skip end of array, should be 0
            return enif_make_list_from_array(env, decoded_list.data(), arrayLen);
        }else{
            // complex array multiple types
            std::cout << "MUHAHAHA"  << "\r\n";
        } 

        //for(uint64_t i=0; i < arrayLen; i++){
            //int64_t type_index = decodeLong(it);
            //std::cout << "Type index:" << std::to_string(type_index) << "\r\n";
            //std::string eletype = si->array_multi_type_reverse[type_index];
            //std::cout << "Type name:" << eletype << "\r\n";
            /*if((eletype == "string")||(eletype == "long")||(eletype == "double")){
                decoded_list.push_back(decode_scalar(env, get_scalar_type(eletype), it));
            }else if(eletype == "array"){
                decoded_list.push_back(decode_array(env, si->childItems[0], it));
            }*/
        //}
        //it++; // skip end of array, should be 0
        //return enif_make_list_from_array(env, decoded_list.data(), arrayLen);
    }

    return enif_make_atom(env, "undefined"); 
}

ERL_NIF_TERM decode_union(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    uint32_t childNumber = decodeInt32(it);
    if(si->is_nullable == 1){
        //std::cout << "Nullable Child number: " << childNumber << "\r\n";
        if(childNumber == 0){
            return enif_make_atom(env, "undefined"); 
        }else{
            auto sctype = si->childItems[childNumber - 1]->scalar_type;
            //std::cout << "SCTYPE: " << sctype << "\r\n";
            /*std::cout << "complex ARRAY union type " << si->childItems[childNumber - 1]->array_type << "\r\n";
            std::cout << "si->obj_type:" << std::to_string(si->childItems[childNumber - 1]->obj_type) << "\r\n";
            std::cout << "si->scalar_type:" << std::to_string(si->childItems[childNumber - 1]->scalar_type) << "\r\n";
            std::cout << "si->obj_field:" << si->childItems[childNumber - 1]->obj_field << "\r\n";
            std::cout << "si->obj_simple_type:" << std::to_string(si->childItems[childNumber - 1]->obj_simple_type) << "\r\n";
            std::cout << "si->array_type:" << std::to_string(si->childItems[childNumber - 1]->array_type) << "\r\n"; */
            if(sctype >= 0 && si->childItems[childNumber - 1]->obj_type == 0){
                //std::cout << "DECODE SCALAR" << "\r\n";
                return decode_scalar(env, sctype, it);
            } else {
                return decodevalue(env, si->childItems[childNumber - 1], it);
            }
        }
    } else {
        //std::cout << "NOT Nullable Child number: " << childNumber << "\r\n";
        auto sctype = si->childItems[childNumber]->scalar_type;
        //std::cout << "SCTYPE: " << sctype << "\r\n";
        if(sctype >= 0  && si->childItems[childNumber]->obj_type == 0){
            //std::cout << "DECODE SCALAR" << "\r\n";
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
    //std::cout << "decode string with length :" << len << "\r\n";

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
