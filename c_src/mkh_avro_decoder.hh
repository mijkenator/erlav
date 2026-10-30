#include <erl_nif.h>
#include <stdint.h>
#include <vector>
#include <array>
#include <stdexcept>

#include "avro_exceptions.hh"

#ifndef SI_H
#define SI_H
#include "schema_item.hh"
#endif

namespace mkh_avro2 {
// What the pointer-based decoder needs to know about the input beyond the
// cursor, passed down by const reference. The input term and options travel
// here rather than in a thread_local so a decode never depends on state set
// "around" it. (The one remaining per-call state, the zero-width item budget
// in mkh_avro_decoder.cc, is thread_local and relies on a decode running to
// completion on one scheduler thread without yielding; it is reset on every
// NIF call.)
struct DecodeCtx {
    const uint8_t* end;        // one past the last input byte
    const uint8_t* base;       // first input byte (offsets for sub-binaries)
    ERL_NIF_TERM input;        // the input binary term, for enif_make_sub_binary
    bool sub_binaries;         // erlav_init/2 `decode_sub_binaries` (#74)
};

int64_t decodeZigzag64(uint64_t) noexcept;
int32_t decodeZigzag32(uint32_t) noexcept;
int64_t decodeLong( std::vector<uint8_t>::iterator&, std::vector<uint8_t>::iterator );
int64_t decodeLong(uint8_t*& it, const uint8_t* end);
int64_t decode_block_count(uint8_t*& it, const uint8_t* end, size_t min_item_wire_size);
// Called at the start of every decode: resets the per-decode budget for
// array/map items that occupy no bytes on the wire (see #40).
void reset_decode_budget();
float decode_float(std::vector<uint8_t>::iterator&, std::vector<uint8_t>::iterator );
float decode_float(uint8_t*& it, const uint8_t* end);
double decode_double(std::vector<uint8_t>::iterator&, std::vector<uint8_t>::iterator );
double decode_double(uint8_t*& it, const uint8_t* end);
int64_t decodeVarint(uint8_t*& it, const uint8_t* end);
int32_t decodeInt32(uint8_t*& it, const uint8_t* end);
ERL_NIF_TERM decode_string(ErlNifEnv* env, uint8_t*& it, const DecodeCtx& ctx);
ERL_NIF_TERM decode_boolean(ErlNifEnv* env, uint8_t*& it, const uint8_t* end);

ERL_NIF_TERM decode_scalar(ErlNifEnv*, int , std::vector<uint8_t>::iterator&, std::vector<uint8_t>::iterator);
ERL_NIF_TERM decode_scalar(ErlNifEnv*, int , uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_nullable_scalar(ErlNifEnv*, int, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_union(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decodevalue(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_array(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_record(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_map(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_enum(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);
ERL_NIF_TERM decode_fixed(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);

ERL_NIF_TERM  decode(ErlNifEnv*, SchemaItem*, std::vector<uint8_t>::iterator&, std::vector<uint8_t>::iterator);
ERL_NIF_TERM  decode(ErlNifEnv*, SchemaItem*, uint8_t*&, const DecodeCtx&);

}

