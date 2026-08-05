#pragma once

#include <cstddef>
#include <cstdint>

namespace asterrpc::protocol{

inline constexpr std::uint32_t kMagic=0x41525043;
inline constexpr std::uint8_t kVersion=3;
inline constexpr std::size_t kHeaderSize=24;

inline constexpr std::size_t kMaxMetaSize=64*1024;
inline constexpr std::size_t kMaxPayloadSize=4*1024*1024;

enum class MessageType : std::uint8_t{
    Unknown=0,
    Request=1,
    Response=2
};

enum class CodecType : std::uint8_t{
    Unknown=0,
    Protobuf=1
};

struct RpcHeader{
    std::uint32_t magic=kMagic;
    std::uint8_t version=kVersion;
    MessageType message_type=MessageType::Unknown;
    CodecType codec=CodecType::Protobuf;
    std::uint8_t flags=0;
    std::uint64_t request_id=0;
    std::uint32_t meta_len=0;
    std::uint32_t payload_len=0;
};

}
