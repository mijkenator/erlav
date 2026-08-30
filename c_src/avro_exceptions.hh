#ifndef AVRO_EXCEPTIONS_H
#define AVRO_EXCEPTIONS_H

#include <exception>
#include <string>
#include <iostream>

namespace mkh_avro {

class AvroException : public std::exception {
public:
    std::string message;
    uint32_t code;

    AvroException(std::string msg, uint32_t c) : message(msg), code(c) {}

    // Matches std::exception::what()'s actual signature (const, noexcept,
    // returning const char*) -- the previous non-const, non-noexcept,
    // char*-returning declaration didn't override the base method, it just
    // hid it via name lookup (-Woverloaded-virtual). formatted_what is a
    // member (not a local std::string) so the pointer what() hands back
    // stays valid for the exception object's lifetime, matching what()'s
    // contract; the old code returned a pointer into a local std::string's
    // buffer that was already destroyed by the time the caller could use it
    // (-Wreturn-stack-address) -- no call site hit that yet since every
    // catch block here reads message/code directly instead of calling
    // what(), but it was undefined behavior waiting for a future caller.
    const char* what() const noexcept override { return formatted_what.c_str(); }

private:
    // Declared after message/code so it can be built from them in its own
    // initializer -- member initialization order follows declaration order,
    // not constructor-argument order.
    std::string formatted_what =
        "Error message: " + message + " code: " + std::to_string(code);
};

} // namespace mkh_avro

#endif // AVRO_EXCEPTIONS_H
