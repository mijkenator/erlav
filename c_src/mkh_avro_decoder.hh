#include <erl_nif.h>
#include <stdint.h>
#include <vector>
#include <stdexcept>

namespace mkh_avro2 {
int64_t decodeZigzag64(uint64_t) noexcept;
int64_t decodeLong( std::vector<uint8_t>::iterator& );

}

