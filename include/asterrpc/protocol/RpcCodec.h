#pragma once

#include "asterrpc/protocol/RpcMessage.h"

#include <string>

namespace asterrpc::net{
class Buffer;
}

namespace asterrpc::protocol{

enum class DecodeStatus{
    Ok,
    NeedMoreData,
    ProtocolError
};

class RpcCodec{
public:
    std::string Encode(
        const RpcMessage& message
    )const;

    DecodeStatus DecodeOne(
        net::Buffer* buffer,
        RpcMessage* message,
        std::string* error
    )const;
};

}