#include <exception>
#include <string>
#include <iostream>

namespace mkh_avro {

class AvroException : public std::exception {
public:
    std::string message;
    uint32_t code;

    AvroException(std::string msg, uint32_t c) : message(msg), code(c) {}
    char* what() {
        std::string ret("Error message: ");
        ret += message + std::string(" code: ") + std::to_string(code);
        return const_cast<char*>(ret.c_str());
        ;
    }
};

} // namespace mkh_avro
