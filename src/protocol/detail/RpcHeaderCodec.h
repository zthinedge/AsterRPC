#pragma once

#include "asterrpc/protocol/RpcHeader.h"

#include <string>

namespace asterrpc::protocol::detail{

class RpcHeaderCodec{
public:
    std::string Encode(const RpcHeader& header)const;

    bool Decode(
        const char* data,
        RpcHeader* header,
        std::string* error
    )const;
};

}
