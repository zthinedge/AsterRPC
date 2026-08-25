#include "asterrpc/net/Buffer.h"
#include "asterrpc/protocol/RpcCodec.h"

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <string>

using namespace asterrpc::net;
using namespace asterrpc::protocol;

namespace{

RpcMessage MakeRequest(){
    RpcMessage message;
    message.message_type=MessageType::Request;
    message.request_id=42;
    message.meta.service_name="UserService";
    message.meta.method_name="GetUser";
    message.meta.deadline_us=123456789;
    message.payload=std::string("user\0id",7);
    return message;
}

RpcMessage MakeResponse(){
    RpcMessage message;
    message.message_type=MessageType::Response;
    message.request_id=42;
    message.meta.status_code=StatusCode::MethodNotFound;
    message.meta.error_text="method not found";
    return message;
}

void AssertMessageEqual(
    const RpcMessage& left,
    const RpcMessage& right
){
    assert(left.message_type==right.message_type);
    assert(left.codec==right.codec);
    assert(left.flags==right.flags);
    assert(left.request_id==right.request_id);
    assert(left.meta.service_name==right.meta.service_name);
    assert(left.meta.method_name==right.meta.method_name);
    assert(left.meta.status_code==right.meta.status_code);
    assert(left.meta.error_text==right.meta.error_text);
    assert(left.meta.deadline_us==right.meta.deadline_us);
    assert(left.payload==right.payload);
}

void WriteUint32(
    std::string* data,
    std::size_t offset,
    std::uint32_t value
){
    (*data)[offset]=static_cast<char>((value>>24)&0xff);
    (*data)[offset+1]=static_cast<char>((value>>16)&0xff);
    (*data)[offset+2]=static_cast<char>((value>>8)&0xff);
    (*data)[offset+3]=static_cast<char>(value&0xff);
}

std::uint32_t ReadUint32(
    const std::string& data,
    std::size_t offset
){
    return
        (static_cast<std::uint32_t>(
            static_cast<unsigned char>(data[offset])
        )<<24)|
        (static_cast<std::uint32_t>(
            static_cast<unsigned char>(data[offset+1])
        )<<16)|
        (static_cast<std::uint32_t>(
            static_cast<unsigned char>(data[offset+2])
        )<<8)|
        static_cast<std::uint32_t>(
            static_cast<unsigned char>(data[offset+3])
        );
}

std::uint64_t ReadUint64(
    const std::string& data,
    std::size_t offset
){
    std::uint64_t value=0;

    for(std::size_t i=0;i<8;i++){
        value=(value<<8)|static_cast<unsigned char>(
            data[offset+i]
        );
    }

    return value;
}

void ExpectProtocolError(const std::string& bytes){
    RpcCodec codec;
    Buffer buffer;
    buffer.Append(bytes);

    RpcMessage message;
    std::string error;
    std::size_t readable=buffer.ReadableBytes();

    DecodeStatus status=codec.DecodeOne(
        &buffer,
        &message,
        &error
    );

    assert(status==DecodeStatus::ProtocolError);
    assert(!error.empty());
    assert(buffer.ReadableBytes()==readable);
}

void TestRequestRoundTrip(){
    RpcCodec codec;
    RpcMessage input=MakeRequest();
    std::string bytes=codec.Encode(input);

    assert(bytes.size()>=kHeaderSize);

    Buffer buffer;
    buffer.Append(bytes);

    RpcMessage output;
    std::string error;
    DecodeStatus status=codec.DecodeOne(
        &buffer,
        &output,
        &error
    );

    assert(status==DecodeStatus::Ok);
    assert(error.empty());
    assert(buffer.ReadableBytes()==0);
    AssertMessageEqual(input,output);
}

void TestWireHeaderLayout(){
    RpcCodec codec;
    std::string bytes=codec.Encode(MakeRequest());

    assert(static_cast<unsigned char>(bytes[0])==0x41);
    assert(static_cast<unsigned char>(bytes[1])==0x52);
    assert(static_cast<unsigned char>(bytes[2])==0x50);
    assert(static_cast<unsigned char>(bytes[3])==0x43);
    assert(static_cast<unsigned char>(bytes[4])==kVersion);
    assert(static_cast<unsigned char>(bytes[5])==1);
    assert(static_cast<unsigned char>(bytes[6])==1);
    assert(static_cast<unsigned char>(bytes[7])==0);
    assert(ReadUint64(bytes,8)==42);
    assert(ReadUint32(bytes,16)==
           bytes.size()-kHeaderSize-7);
    assert(ReadUint32(bytes,20)==7);
}

void TestResponseRoundTrip(){
    RpcCodec codec;
    RpcMessage input=MakeResponse();
    std::string bytes=codec.Encode(input);

    Buffer buffer;
    buffer.Append(bytes);

    RpcMessage output;
    std::string error;
    DecodeStatus status=codec.DecodeOne(
        &buffer,
        &output,
        &error
    );

    assert(status==DecodeStatus::Ok);
    AssertMessageEqual(input,output);
}

void TestAllHalfPacketPositions(){
    RpcCodec codec;
    RpcMessage input=MakeRequest();
    std::string bytes=codec.Encode(input);

    for(std::size_t split=0;split<bytes.size();split++){
        Buffer buffer;

        if(split!=0){
            buffer.Append(bytes.data(),split);
        }

        RpcMessage output;
        std::string error;
        DecodeStatus status=codec.DecodeOne(
            &buffer,
            &output,
            &error
        );

        assert(status==DecodeStatus::NeedMoreData);
        assert(error.empty());
        assert(buffer.ReadableBytes()==split);

        buffer.Append(
            bytes.data()+split,
            bytes.size()-split
        );

        status=codec.DecodeOne(&buffer,&output,&error);
        assert(status==DecodeStatus::Ok);
        assert(buffer.ReadableBytes()==0);
        AssertMessageEqual(input,output);
    }
}

void TestStickyPackets(){
    RpcCodec codec;
    RpcMessage request=MakeRequest();
    RpcMessage response=MakeResponse();

    std::string bytes=
        codec.Encode(request)+codec.Encode(response);

    Buffer buffer;
    buffer.Append(bytes);

    RpcMessage output;
    std::string error;

    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::Ok);
    AssertMessageEqual(request,output);

    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::Ok);
    AssertMessageEqual(response,output);

    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::NeedMoreData);
    assert(buffer.ReadableBytes()==0);
}

void TestCompleteAndHalfPacket(){
    RpcCodec codec;
    RpcMessage request=MakeRequest();
    RpcMessage response=MakeResponse();

    std::string first=codec.Encode(request);
    std::string second=codec.Encode(response);
    std::size_t half=second.size()/2;

    Buffer buffer;
    buffer.Append(first);
    buffer.Append(second.data(),half);

    RpcMessage output;
    std::string error;

    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::Ok);
    AssertMessageEqual(request,output);

    std::size_t readable=buffer.ReadableBytes();
    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::NeedMoreData);
    assert(buffer.ReadableBytes()==readable);

    buffer.Append(second.data()+half,second.size()-half);
    assert(codec.DecodeOne(&buffer,&output,&error)==
           DecodeStatus::Ok);
    AssertMessageEqual(response,output);
}

void TestInvalidHeaders(){
    RpcCodec codec;
    std::string valid=codec.Encode(MakeRequest());

    std::string bytes=valid;
    bytes[0]=0;
    ExpectProtocolError(bytes);

    bytes=valid;
    bytes[4]=static_cast<char>(kVersion+1);
    ExpectProtocolError(bytes);

    bytes=valid;
    bytes[5]=99;
    ExpectProtocolError(bytes);

    bytes=valid;
    bytes[6]=99;
    ExpectProtocolError(bytes);

    bytes=valid;
    bytes[7]=1;
    ExpectProtocolError(bytes);

    bytes=valid;
    for(std::size_t i=8;i<16;i++){
        bytes[i]=0;
    }
    ExpectProtocolError(bytes);
}

void TestInvalidLengthsAndMeta(){
    RpcCodec codec;
    std::string valid=codec.Encode(MakeRequest());

    std::string bytes=valid;
    WriteUint32(
        &bytes,
        16,
        static_cast<std::uint32_t>(kMaxMetaSize+1)
    );
    ExpectProtocolError(bytes);

    bytes=valid;
    WriteUint32(
        &bytes,
        20,
        static_cast<std::uint32_t>(kMaxPayloadSize+1)
    );
    ExpectProtocolError(bytes);

    bytes=valid;
    WriteUint32(
        &bytes,
        kHeaderSize,
        static_cast<std::uint32_t>(kMaxMetaSize)
    );
    ExpectProtocolError(bytes);

    bytes=valid;
    std::size_t status_offset=
        kHeaderSize+
        4+MakeRequest().meta.service_name.size()+
        4+MakeRequest().meta.method_name.size();
    WriteUint32(&bytes,status_offset,99);
    ExpectProtocolError(bytes);
}

void TestEncodeValidation(){
    RpcCodec codec;

    RpcMessage message=MakeRequest();
    message.message_type=MessageType::Unknown;

    bool threw=false;
    try{
        codec.Encode(message);
    }catch(const std::invalid_argument&){
        threw=true;
    }
    assert(threw);

    message=MakeRequest();
    message.meta.service_name.clear();
    threw=false;
    try{
        codec.Encode(message);
    }catch(const std::invalid_argument&){
        threw=true;
    }
    assert(threw);

    message=MakeRequest();
    message.payload.resize(kMaxPayloadSize+1);
    threw=false;
    try{
        codec.Encode(message);
    }catch(const std::invalid_argument&){
        threw=true;
    }
    assert(threw);

    message=MakeRequest();
    message.meta.service_name.resize(kMaxMetaSize);
    threw=false;
    try{
        codec.Encode(message);
    }catch(const std::invalid_argument&){
        threw=true;
    }
    assert(threw);
}

}

int main(){
    TestRequestRoundTrip();
    TestWireHeaderLayout();
    TestResponseRoundTrip();
    TestAllHalfPacketPositions();
    TestStickyPackets();
    TestCompleteAndHalfPacket();
    TestInvalidHeaders();
    TestInvalidLengthsAndMeta();
    TestEncodeValidation();
    return 0;
}
