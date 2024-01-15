#include "schema_item.hh"


namespace mkh_avro2 {

const std::vector<std::string> scalars{"int", "long", "double", "float", "boolean", "string", "bytes"};

bool
is_scalar(std::string ttype) {
    return std::find(scalars.begin(), scalars.end(), ttype) != scalars.end();
}
int
get_scalar_type(std::string ntype) {
    auto it = find(scalars.begin(), scalars.end(), ntype);
    if (it != scalars.end()) {
        int index = it - scalars.begin();
        return index;
    } else {
        return -1;
    }
}

}
