#pragma once

#include "asterrpc/protocol/RpcMeta.h"

#include <cstddef>
#include <string>

namespace asterrpc::protocol::detail{

class RpcMetaCodec{
public:
    std::string Encode(const RpcMeta& meta)const;

    bool Decode(
        const char* data,
        std::size_t length,
        RpcMeta* meta,
        std::string* error
    )const;
};

}
