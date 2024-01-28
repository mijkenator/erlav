#include <erl_nif.h>
#include <stdint.h>
#include <vector>
#include <array>
#include <stdexcept>

#ifndef SI_H
#define SI_H
#include "schema_item.hh"
#endif

namespace mkh_avro2 {
int64_t decodeZigzag64(uint64_t) noexcept;
int64_t decodeLong( std::vector<uint8_t>::iterator& );
int64_t decodeLong(uint8_t*& it);
float decode_float(std::vector<uint8_t>::iterator& );
float decode_float(uint8_t*& it);
double decode_double(std::vector<uint8_t>::iterator& );
double decode_double(uint8_t*& it);
int64_t decodeVarint(uint8_t*& it);

ERL_NIF_TERM decode_scalar(ErlNifEnv*, int , std::vector<uint8_t>::iterator&);
ERL_NIF_TERM decode_scalar(ErlNifEnv*, int , uint8_t*&);

ERL_NIF_TERM  decode(ErlNifEnv*, SchemaItem*, std::vector<uint8_t>::iterator&);
ERL_NIF_TERM  decode(ErlNifEnv*, SchemaItem*, uint8_t*&);

}

