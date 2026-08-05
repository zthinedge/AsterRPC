#include "detail/RpcHeaderCodec.h"
#include "detail/ByteOrder.h"

#include <cstdint>
#include <stdexcept>

namespace asterrpc::protocol::detail{
namespace{

void SetError(std::string* error,const std::string& value){
    if(error!=nullptr){
        *error=value;
    }
}

bool IsValidMessageType(std::uint8_t value){
    return
        value==static_cast<std::uint8_t>(MessageType::Request)||
        value==static_cast<std::uint8_t>(MessageType::Response);
}

}

std::string RpcHeaderCodec::Encode(
    const RpcHeader& header
)const{
    if(header.magic!=kMagic){
        throw std::invalid_argument("invalid rpc magic");
    }

    if(header.version!=kVersion){
        throw std::invalid_argument("unsupported rpc version");
    }

    std::uint8_t message_type=static_cast<std::uint8_t>(
        header.message_type
    );

    if(!IsValidMessageType(message_type)){
        throw std::invalid_argument("invalid rpc message type");
    }

    if(header.codec!=CodecType::Protobuf){
        throw std::invalid_argument("unsupported rpc codec");
    }

    if(header.flags!=0){
        throw std::invalid_argument("unsupported rpc flags");
    }

    if(header.request_id==0){
        throw std::invalid_argument("rpc request id must not be zero");
    }

    if(header.meta_len>kMaxMetaSize){
        throw std::invalid_argument("rpc meta is too large");
    }

    if(header.payload_len>kMaxPayloadSize){
        throw std::invalid_argument("rpc payload is too large");
    }

    std::string output;
    output.reserve(kHeaderSize);

    AppendUint32(&output,header.magic);
    AppendUint8(&output,header.version);
    AppendUint8(&output,message_type);
    AppendUint8(
        &output,
        static_cast<std::uint8_t>(header.codec)
    );
    AppendUint8(&output,header.flags);
    AppendUint64(&output,header.request_id);
    AppendUint32(&output,header.meta_len);
    AppendUint32(&output,header.payload_len);

    return output;
}

bool RpcHeaderCodec::Decode(
    const char* data,
    RpcHeader* header,
    std::string* error
)const{
    if(data==nullptr||header==nullptr){
        SetError(error,"rpc header output is null");
        return false;
    }

    RpcHeader decoded;
    decoded.magic=ReadUint32(data);
    decoded.version=ReadUint8(data+4);

    std::uint8_t message_type=ReadUint8(data+5);
    std::uint8_t codec=ReadUint8(data+6);

    decoded.flags=ReadUint8(data+7);
    decoded.request_id=ReadUint64(data+8);
    decoded.meta_len=ReadUint32(data+16);
    decoded.payload_len=ReadUint32(data+20);

    if(decoded.magic!=kMagic){
        SetError(error,"invalid rpc magic");
        return false;
    }

    if(decoded.version!=kVersion){
        SetError(error,"unsupported rpc version");
        return false;
    }

    if(!IsValidMessageType(message_type)){
        SetError(error,"invalid rpc message type");
        return false;
    }

    if(codec!=static_cast<std::uint8_t>(CodecType::Protobuf)){
        SetError(error,"unsupported rpc codec");
        return false;
    }

    if(decoded.flags!=0){
        SetError(error,"unsupported rpc flags");
        return false;
    }

    if(decoded.request_id==0){
        SetError(error,"rpc request id must not be zero");
        return false;
    }

    if(decoded.meta_len>kMaxMetaSize){
        SetError(error,"rpc meta is too large");
        return false;
    }

    if(decoded.payload_len>kMaxPayloadSize){
        SetError(error,"rpc payload is too large");
        return false;
    }

    decoded.message_type=static_cast<MessageType>(message_type);
    decoded.codec=static_cast<CodecType>(codec);
    *header=decoded;
    return true;
}

}
