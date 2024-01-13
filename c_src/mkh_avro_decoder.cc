#include "mkh_avro_decoder.hh"


namespace mkh_avro2 {

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


}
