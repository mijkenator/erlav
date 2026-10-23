#include "mkh_avro_decoder.hh"


namespace mkh_avro2 {

// Atom terms are immediates, valid in any env, so look each one up in the
// VM's atom table once instead of on every decoded null/boolean (each
// enif_make_atom takes the atom table's read lock and hashes the name).
// Function-local statics are initialised thread-safely on first use.
static inline ERL_NIF_TERM atom_undefined(ErlNifEnv* env) {
    static const ERL_NIF_TERM a = enif_make_atom(env, "undefined");
    return a;
}
static inline ERL_NIF_TERM atom_true(ErlNifEnv* env) {
    static const ERL_NIF_TERM a = enif_make_atom(env, "true");
    return a;
}
static inline ERL_NIF_TERM atom_false(ErlNifEnv* env) {
    static const ERL_NIF_TERM a = enif_make_atom(env, "false");
    return a;
}



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
    // Collect keys/values and build the map once with
    // enif_make_map_from_arrays, instead of one enif_make_map_put per field
    // (each put re-walks/copies the partial map and re-compares keys -- the
    // dominant cost of decoding a wide record). Field names are unique
    // (enforced at schema-init, see init_keys_with_env), so the
    // duplicate-key failure mode of from_arrays cannot occur.
    static constexpr size_t STACK_CAP = 64;
    const size_t nchildren = si->childItems.size();
    ERL_NIF_TERM stack_keys[STACK_CAP];
    ERL_NIF_TERM stack_vals[STACK_CAP];
    std::vector<ERL_NIF_TERM> heap_keys, heap_vals;
    ERL_NIF_TERM* keys = stack_keys;
    ERL_NIF_TERM* vals = stack_vals;
    if (nchildren > STACK_CAP) {
        heap_keys.resize(nchildren);
        heap_vals.resize(nchildren);
        keys = heap_keys.data();
        vals = heap_vals.data();
    }
    size_t n = 0;

    for(SchemaItem* si_e : si->childItems){
        // A scalar-typed field with no resolvable scalar type, or an
        // unknown obj_type, has always been skipped (no key emitted).
        if (si_e->obj_type == 0 && si_e->scalar_type < 0) continue;
        if (si_e->obj_type < 0 || si_e->obj_type > 6) continue;

        auto len = si_e->obj_name.length();
        unsigned char* key_data = enif_make_new_binary(env, len, &keys[n]);
        memcpy(key_data, si_e->obj_name.data(), len);
        vals[n] = decodevalue(env, si_e, it);
        n++;
    }

    ERL_NIF_TERM ret;
    if (!enif_make_map_from_arrays(env, keys, vals, n, &ret)) {
        throw mkh_avro::AvroException("Record decode: duplicate field name", 12);
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
        case 6: // fixed
            return decode_fixed(env, si, it);
        default:
            break;
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
    // const& -- this used to copy the symbol's std::string on every decode.
    const auto& val = si->array_multi_type_reverse.at(valueNumber);

    auto len = val.length();
    unsigned char* str_data;
    str_data = enif_make_new_binary(env, len, &str_ret);
    memcpy(str_data, val.data(), len);

    return str_ret;
}

// Avro "fixed" -- exactly si->fixed_size bytes, with no wire-encoded
// length prefix (unlike decode_string's bytes/string, whose length is
// read from the data). See #39.
ERL_NIF_TERM decode_fixed(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    ERL_NIF_TERM bin_ret;
    unsigned char* bin_data;
    bin_data = enif_make_new_binary(env, si->fixed_size, &bin_ret);
    memcpy(bin_data, it, si->fixed_size);
    it += si->fixed_size;
    return bin_ret;
}

// Key/value gather buffer for decode_map: the first STACK_CAP entries live
// on the stack (a std::vector would heap-allocate twice per call, which made
// 3-entry maps ~55% slower than the old put loop); larger maps spill to the
// heap.
struct MapEntries {
    static constexpr size_t STACK_CAP = 16;
    ERL_NIF_TERM skeys[STACK_CAP];
    ERL_NIF_TERM svals[STACK_CAP];
    std::vector<ERL_NIF_TERM> hkeys, hvals;
    size_t n = 0;
    void add(ERL_NIF_TERM k, ERL_NIF_TERM v) {
        if (n < STACK_CAP) {
            skeys[n] = k;
            svals[n] = v;
        } else {
            if (n == STACK_CAP) {
                hkeys.assign(skeys, skeys + STACK_CAP);
                hvals.assign(svals, svals + STACK_CAP);
            }
            hkeys.push_back(k);
            hvals.push_back(v);
        }
        n++;
    }
    ERL_NIF_TERM* keys() { return n <= STACK_CAP ? skeys : hkeys.data(); }
    ERL_NIF_TERM* vals() { return n <= STACK_CAP ? svals : hvals.data(); }
};

ERL_NIF_TERM decode_map(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    // Entries are gathered, then the map is built once for larger maps (see
    // decode() for why enif_make_map_put per entry is slow). Unlike record
    // field names, map keys on the wire are not guaranteed unique;
    // enif_make_map_from_arrays fails on a duplicate, so fall back to the
    // sequential put loop (last occurrence wins) in that case to keep
    // behaviour identical.
    MapEntries entries;
    // Loop over blocks instead of a single upfront count: a block header's
    // count may be negative (erlavro's usual form -- see #15), in which
    // case decode_block_count also consumes the following block-byte-length
    // long and returns the (positive) item count for that block. A count of
    // 0 ends the map -- this also covers the empty-map case (first block
    // read is 0) without any separate "was there a terminator" check.
    if (si->values_are_union) {
        // Map values are a union (e.g. {"values": ["null", "string"]}) --
        // every value has a per-entry union type-index byte on the wire
        // that decode_union already knows how to read (it reads
        // si->is_nullable/si->childItems directly, which read_internal_types
        // populated on this very SchemaItem the same way it would for a
        // nullable/union record field). Checked before obj_field, since
        // values_are_union can be true alongside either obj_field ==
        // "complex" (a non-null complex union member) or a scalar
        // obj_field (the union's non-null scalar member) -- neither of the
        // two branches below knows to read the discriminator byte, and
        // misreading it as the start of the next value corrupts the
        // cursor for everything decoded after (previously a VM-crashing
        // garbage length handed to enif_make_new_binary). See #32.
        for (;;) {
            int64_t mapLen = decode_block_count(it);
            if (mapLen == 0) break;
            for (int64_t i = 0; i < mapLen; i++) {
                ERL_NIF_TERM k = decode_scalar(env, 5, it); // map keys always strings
                entries.add(k, decode_union(env, si, it));
            }
        }
    } else if (si->obj_field != "complex") { // map of scalars
        for (;;) {
            int64_t mapLen = decode_block_count(it);
            if (mapLen == 0) break;
            for (int64_t i = 0; i < mapLen; i++) {
                ERL_NIF_TERM k = decode_scalar(env, 5, it); // map keys always strings
                entries.add(k, decode_scalar(env, si->scalar_type, it));
            }
        }
    } else {
        for (;;) {
            int64_t mapLen = decode_block_count(it);
            if (mapLen == 0) break;
            for (int64_t i = 0; i < mapLen; i++) {
                ERL_NIF_TERM k = decode_scalar(env, 5, it); // map keys always strings
                entries.add(k, decodevalue(env, si->childItems[0], it));
            }
        }
    }

    // Tiny maps are faster through the put loop: from_arrays has a fixed
    // setup cost (key sort + dedup) that only pays off once the put loop's
    // per-put copy/compare work grows. Using from_arrays for *every* map
    // made the OpenRTB fixture (whose maps are almost all 1-2 entries)
    // ~25% slower than the put loop; 4+ entries were consistently faster
    // (4: ~35%, 128: ~3x). 3 entries was not cleanly separated by the
    // measurements, so the threshold of 4 is a conservative pick, not a
    // measured optimum -- see docs/decode-map-build-perf.md.
    static constexpr size_t FROM_ARRAYS_MIN_ENTRIES = 4;
    ERL_NIF_TERM ret;
    if (entries.n >= FROM_ARRAYS_MIN_ENTRIES &&
        enif_make_map_from_arrays(
            env, entries.keys(), entries.vals(), entries.n, &ret)) {
        return ret;
    }
    ret = enif_make_new_map(env);
    ERL_NIF_TERM map_out;
    for (size_t i = 0; i < entries.n; i++) {
        if (enif_make_map_put(env, ret, entries.keys()[i], entries.vals()[i], &map_out)) {
            ret = map_out;
        }
    }
    return ret;
}

ERL_NIF_TERM decode_array(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    std::vector<ERL_NIF_TERM> decoded_list;
    /*std::cout << "complex ARRAY union type " << si->array_type << "\r\n";
    std::cout << "si->obj_type:" << std::to_string(si->obj_type) << "\r\n";
    std::cout << "si->scalar_type:" << std::to_string(si->scalar_type) << "\r\n";
    std::cout << "si->obj_field:" << si->obj_field << "\r\n";
    std::cout << "si->obj_simple_type:" << std::to_string(si->obj_simple_type) << "\r\n";
    std::cout << "si->array_type:" << std::to_string(si->array_type) << "\r\n";
    auto child_len = si->childItems.size();
    std::cout << "child len:" << child_len << "\r\n";
    std::cout << "----------------------------------- \r\n";*/

    // Loop over blocks instead of a single upfront count: a block header's
    // count may be negative (erlavro's usual form -- see #15), in which
    // case decode_block_count also consumes the following block-byte-length
    // long and returns the (positive) item count for that block. A count of
    // 0 ends the array -- this also covers the empty-array case (first
    // block read is 0) without any separate "was there a terminator" check.
    if (si->obj_field != "complex") {
        auto st = get_scalar_type(si->obj_field);
        //std::cout << "simple array: type --> " << st << " \r\n";
        for (;;) {
            int64_t arrayLen = decode_block_count(it);
            if (arrayLen == 0) break;
            for (int64_t i = 0; i < arrayLen; i++) {
                decoded_list.push_back(decode_scalar(env, st, it));
            }
        }
        return enif_make_list_from_array(env, decoded_list.data(), decoded_list.size());

    } else if ((si->obj_field == "complex") && si->array_type == 1) {
        //std::cout << "complex ARRAY \r\n";
        for (;;) {
            int64_t arrayLen = decode_block_count(it);
            if (arrayLen == 0) break;
            for (int64_t i = 0; i < arrayLen; i++) {
                int64_t type_index = decodeLong(it);
                //std::cout << "Type index:" << std::to_string(type_index) << "\r\n";
                std::string eletype = si->array_multi_type_reverse[type_index];
                //std::cout << "Type name:" << eletype << "\r\n";
                if((eletype == "string")||(eletype == "long")||(eletype == "double")){
                    decoded_list.push_back(decode_scalar(env, get_scalar_type(eletype), it));
                }else if(eletype == "null"){
                    decoded_list.push_back(atom_undefined(env));
                }else if(eletype == "array"){
                    int child_idx = si->array_multi_type_child_index.at("array");
                    decoded_list.push_back(decode_array(env, si->childItems[child_idx], it));
                }else if((eletype == "record")||(eletype == "map")||(eletype == "enum")){
                    int child_idx = si->array_multi_type_child_index.at(eletype);
                    decoded_list.push_back(decodevalue(env, si->childItems[child_idx], it));
                }else{
                    // Unresolved/unrecognized union member name -- fail loudly
                    // rather than silently skipping the element (which would
                    // desync the read cursor for everything decoded after
                    // this array) and rather than handing
                    // enif_make_list_from_array uninitialized memory to
                    // interpret as ERL_NIF_TERM values.
                    throw mkh_avro::AvroException(
                        "Array union: unresolved member type '" + eletype + "'",
                        10);
                }
            }
        }
        return enif_make_list_from_array(env, decoded_list.data(), decoded_list.size());
    } else {
        // complex array - no support for union types yet
        //std::cout << "complex array 2 \r\n";
        auto child_len = si->childItems.size();
        //std::cout << "child scalar_type: " << std::to_string(si->childItems[0]->scalar_type) << "\r\n";
        //std::cout << "child obj_field: " << si->childItems[0]->obj_field << "\r\n";

        // obj_type == 2 (array) must be checked explicitly here, not just
        // "does this child look scalar-shaped" (scalar_type > 0 &&
        // obj_field != "complex") -- a map-of-scalars or fixed SchemaItem
        // also has a scalar-looking scalar_type/obj_field (a map's own
        // scalar_type/obj_field describe its *values*, per decode_map's
        // "obj_field != complex" convention), so array<map<string>> used
        // to satisfy this condition and get routed into decode_array on a
        // map schema -- an immediate cursor desync that crashed the VM via
        // a garbage length handed to enif_make_new_binary. See #44.
        if((child_len == 1) && (si->childItems[0]->obj_type == 2) &&
           (si->childItems[0]->scalar_type > 0) &&
           (si->childItems[0]->obj_field != "complex")){
            // array of simple arrays
            for (;;) {
                int64_t arrayLen = decode_block_count(it);
                if (arrayLen == 0) break;
                for (int64_t i = 0; i < arrayLen; i++) {
                    decoded_list.push_back(decode_array(env, si->childItems[0], it));
                }
            }
            return enif_make_list_from_array(env, decoded_list.data(), decoded_list.size());

        }else if(child_len == 1){
            // Array of 1 complex type -- dispatch each element through
            // decodevalue(), which switches on obj_type to the right
            // decoder (decode_array/decode_record/decode_map/decode_enum/
            // decode_fixed/decode_union), mirroring encodearray's matching
            // complex-array branch (mkh_avro2.hh), which already calls
            // encodevalue() the same way.
            //
            // Previously this called decode() (the record-field-loop
            // decoder) unconditionally for anything that wasn't itself an
            // array, which only "worked" for array<record> by coincidence
            // (decode()'s childItems-as-record-fields loop happens to match
            // what a record decode needs) -- array<map> desynced the cursor
            // and crashed the VM via a garbage length handed to
            // enif_make_new_binary, and array<fixed>/array<enum> silently
            // decoded every element to an empty map instead of throwing or
            // reading the right bytes. See #44.
            for (;;) {
                int64_t arrayLen = decode_block_count(it);
                if (arrayLen == 0) break;
                for (int64_t i = 0; i < arrayLen; i++) {
                    decoded_list.push_back(decodevalue(env, si->childItems[0], it));
                }
            }
            return enif_make_list_from_array(env, decoded_list.data(), decoded_list.size());
        }else{
            // complex array multiple types -- unimplemented, see #41
        }
    }

    return atom_undefined(env);
}

ERL_NIF_TERM decode_union(ErlNifEnv* env, SchemaItem * si, uint8_t*& it) {
    uint32_t childNumber = decodeInt32(it);
    if(si->is_nullable == 1){
        //std::cout << "Nullable Child number: " << childNumber << "\r\n";
        if(childNumber == 0){
            return atom_undefined(env); 
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
    return atom_undefined(env); 
}

ERL_NIF_TERM decode_nullable_scalar(ErlNifEnv* env, int sctype, uint8_t*& it) {
    auto union_byte = *it;
    it++;
    if(union_byte == 0){
        return atom_undefined(env); 
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
    uint8_t bool_val = *it;
    it++;
    return bool_val == 1 ? atom_true(env) : atom_false(env);
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

// Reads one Avro array/map block header. A positive count means that many
// items follow directly. Avro also permits a negative count -N: a signed
// long giving the block's encoded byte length follows immediately (unused
// here -- items are decoded one at a time rather than skipped by byte
// offset), then N items. erlavro's encoder emits this negative form for
// essentially every array/map it writes (see #15), so decode_array/
// decode_map must handle both. Returns the item count for this block; 0
// means "no more blocks" -- the array/map terminator.
int64_t decode_block_count(uint8_t*& it) {
    int64_t count = decodeLong(it);
    if (count < 0) {
        decodeLong(it); // block byte-length, unused
        count = -count;
    }
    return count;
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
