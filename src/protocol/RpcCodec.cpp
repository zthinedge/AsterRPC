#include "asterrpc/protocol/RpcCodec.h"
#include "asterrpc/net/Buffer.h"
#include "detail/RpcHeaderCodec.h"
#include "detail/RpcMetaCodec.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::protocol{
namespace{

void SetError(std::string* error,const std::string& value){
    if(error!=nullptr){
        *error=value;
    }
}

}

std::string RpcCodec::Encode(const RpcMessage& message)const{
    if(message.message_type==MessageType::Request&&
       (message.meta.service_name.empty()||
        message.meta.method_name.empty())){
        throw std::invalid_argument("rpc request route is empty");
    }

    if(message.payload.size()>kMaxPayloadSize){
        throw std::invalid_argument("rpc payload is too large");
    }

    detail::RpcMetaCodec meta_codec;
    std::string meta=meta_codec.Encode(message.meta);

    RpcHeader header;
    header.message_type=message.message_type;
    header.codec=message.codec;
    header.flags=message.flags;
    header.request_id=message.request_id;
    header.meta_len=static_cast<std::uint32_t>(meta.size());
    header.payload_len=static_cast<std::uint32_t>(
        message.payload.size()
    );

    detail::RpcHeaderCodec header_codec;
    std::string output=header_codec.Encode(header);

    output.reserve(
        kHeaderSize+meta.size()+message.payload.size()
    );
    output.append(meta);
    output.append(message.payload);
    return output;
}

DecodeStatus RpcCodec::DecodeOne(
    net::Buffer* buffer,
    RpcMessage* message,
    std::string* error
)const{
    if(error!=nullptr){
        error->clear();
    }

    if(buffer==nullptr||message==nullptr){
        SetError(error,"rpc decode output is null");
        return DecodeStatus::ProtocolError;
    }

    if(buffer->ReadableBytes()<kHeaderSize){
        return DecodeStatus::NeedMoreData;
    }

    RpcHeader header;
    detail::RpcHeaderCodec header_codec;

    if(!header_codec.Decode(buffer->Peek(),&header,error)){
        return DecodeStatus::ProtocolError;
    }

    std::size_t total_size=
        kHeaderSize+
        static_cast<std::size_t>(header.meta_len)+
        static_cast<std::size_t>(header.payload_len);

    if(buffer->ReadableBytes()<total_size){
        return DecodeStatus::NeedMoreData;
    }

    RpcMessage decoded;
    decoded.message_type=header.message_type;
    decoded.codec=header.codec;
    decoded.flags=header.flags;
    decoded.request_id=header.request_id;

    const char* meta_data=buffer->Peek()+kHeaderSize;
    detail::RpcMetaCodec meta_codec;

    if(!meta_codec.Decode(
        meta_data,
        header.meta_len,
        &decoded.meta,
        error
    )){
        return DecodeStatus::ProtocolError;
    }

    if(decoded.message_type==MessageType::Request&&
       (decoded.meta.service_name.empty()||
        decoded.meta.method_name.empty())){
        SetError(error,"rpc request route is empty");
        return DecodeStatus::ProtocolError;
    }

    const char* payload_data=meta_data+header.meta_len;
    decoded.payload.assign(payload_data,header.payload_len);

    buffer->Retrieve(total_size);
    *message=std::move(decoded);
    return DecodeStatus::Ok;
}

}
