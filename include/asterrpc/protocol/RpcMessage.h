#pragma once

#include "asterrpc/protocol/RpcHeader.h"
#include "asterrpc/protocol/RpcMeta.h"

#include <cstdint>
#include <string>

namespace asterrpc::protocol{

struct RpcMessage{
    MessageType message_type=MessageType::Unknown;
    CodecType codec=CodecType::Protobuf;
    std::uint8_t flags=0;
    std::uint64_t request_id=0;

    RpcMeta meta;
    std::string payload;
};

}
