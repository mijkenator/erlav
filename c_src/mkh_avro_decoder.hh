#include <erl_nif.h>
#include <stdint.h>
#include <vector>
#include <array>
#include <stdexcept>

namespace mkh_avro2 {
int64_t decodeZigzag64(uint64_t) noexcept;
int64_t decodeLong( std::vector<uint8_t>::iterator& );
float decode_float(std::vector<uint8_t>::iterator& );
double decode_double(std::vector<uint8_t>::iterator& );

}

