#include "detail/RpcMetaCodec.h"
#include "detail/ByteOrder.h"
#include "minirpc/protocol/RpcHeader.h"

#include <cstdint>
#include <stdexcept>

namespace minirpc::protocol::detail{
namespace{

void SetError(std::string* error,const std::string& value){
    if(error!=nullptr){
        *error=value;
    }
}

bool IsValidStatusCode(std::uint32_t value){
    return value<=static_cast<std::uint32_t>(
        StatusCode::ConnectionFailed
    );
}

bool ReadString(
    const char** cursor,
    const char* end,
    std::string* value
){
    std::size_t remaining=static_cast<std::size_t>(
        end-*cursor
    );

    if(remaining<4){
        return false;
    }

    std::uint32_t length=ReadUint32(*cursor);
    *cursor+=4;

    remaining=static_cast<std::size_t>(end-*cursor);
    if(length>remaining){
        return false;
    }

    value->assign(*cursor,length);
    *cursor+=length;
    return true;
}

}

std::string RpcMetaCodec::Encode(const RpcMeta& meta)const{
    std::size_t total_size=36;

    const std::string* fields[]={
        &meta.service_name,
        &meta.method_name,
        &meta.error_text,
        &meta.trace_id,
        &meta.span_id,
        &meta.parent_span_id
    };

    for(const std::string* field:fields){
        if(field->size()>kMaxMetaSize-total_size){
            throw std::invalid_argument("rpc meta is too large");
        }

        total_size+=field->size();
    }

    std::uint32_t status_code=static_cast<std::uint32_t>(
        meta.status_code
    );

    if(!IsValidStatusCode(status_code)){
        throw std::invalid_argument("invalid rpc status code");
    }

    std::string output;
    output.reserve(total_size);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.service_name.size())
    );
    output.append(meta.service_name);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.method_name.size())
    );
    output.append(meta.method_name);

    AppendUint32(&output,status_code);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.error_text.size())
    );
    output.append(meta.error_text);

    AppendUint64(&output,meta.deadline_us);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.trace_id.size())
    );
    output.append(meta.trace_id);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.span_id.size())
    );
    output.append(meta.span_id);

    AppendUint32(
        &output,
        static_cast<std::uint32_t>(meta.parent_span_id.size())
    );
    output.append(meta.parent_span_id);

    return output;
}

bool RpcMetaCodec::Decode(
    const char* data,
    std::size_t length,
    RpcMeta* meta,
    std::string* error
)const{
    if(data==nullptr||meta==nullptr){
        SetError(error,"rpc meta output is null");
        return false;
    }

    const char* cursor=data;
    const char* end=data+length;
    RpcMeta decoded;

    if(!ReadString(&cursor,end,&decoded.service_name)){
        SetError(error,"invalid rpc service name");
        return false;
    }

    if(!ReadString(&cursor,end,&decoded.method_name)){
        SetError(error,"invalid rpc method name");
        return false;
    }

    if(static_cast<std::size_t>(end-cursor)<4){
        SetError(error,"invalid rpc status code");
        return false;
    }

    std::uint32_t status_code=ReadUint32(cursor);
    cursor+=4;

    if(!IsValidStatusCode(status_code)){
        SetError(error,"invalid rpc status code");
        return false;
    }

    decoded.status_code=static_cast<StatusCode>(status_code);

    if(!ReadString(&cursor,end,&decoded.error_text)){
        SetError(error,"invalid rpc error text");
        return false;
    }

    if(static_cast<std::size_t>(end-cursor)<8){
        SetError(error,"invalid rpc deadline");
        return false;
    }

    decoded.deadline_us=ReadUint64(cursor);
    cursor+=8;

    if(!ReadString(&cursor,end,&decoded.trace_id)){
        SetError(error,"invalid rpc trace id");
        return false;
    }

    if(!ReadString(&cursor,end,&decoded.span_id)){
        SetError(error,"invalid rpc span id");
        return false;
    }

    if(!ReadString(&cursor,end,&decoded.parent_span_id)){
        SetError(error,"invalid rpc parent span id");
        return false;
    }

    if(cursor!=end){
        SetError(error,"unexpected rpc meta bytes");
        return false;
    }

    *meta=std::move(decoded);
    return true;
}

}
