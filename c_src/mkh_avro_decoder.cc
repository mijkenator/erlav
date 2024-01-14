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

float decode_float(std::vector<uint8_t>::iterator& it){
    std::array<uint8_t, sizeof(float)> vect2;
    for (int i=0; i < sizeof(float); i++) {
        vect2[i] = *it;
        ++it;
    }
    const float* ret = reinterpret_cast<const float*>(&vect2);
    return *ret;
}

double decode_double(std::vector<uint8_t>::iterator& it){
    std::array<uint8_t, sizeof(double)> vect2;
    for (int i=0; i < sizeof(double); i++) {
        vect2[i] = *it;
        ++it;
    }
    const double* ret = reinterpret_cast<const double*>(&vect2);
    return *ret;
}


}
