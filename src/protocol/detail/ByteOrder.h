#pragma once

#include <cstdint>
#include <string>

namespace asterrpc::protocol::detail{

inline void AppendUint8(
    std::string* output,
    std::uint8_t value
){
    output->push_back(static_cast<char>(value));
}

inline void AppendUint32(
    std::string* output,
    std::uint32_t value
){
    output->push_back(static_cast<char>((value>>24)&0xff));
    output->push_back(static_cast<char>((value>>16)&0xff));
    output->push_back(static_cast<char>((value>>8)&0xff));
    output->push_back(static_cast<char>(value&0xff));
}

inline void AppendUint64(
    std::string* output,
    std::uint64_t value
){
    for(int shift=56;shift>=0;shift-=8){
        output->push_back(
            static_cast<char>((value>>shift)&0xff)
        );
    }
}

inline std::uint8_t ReadUint8(const char* data){
    return static_cast<std::uint8_t>(
        static_cast<unsigned char>(data[0])
    );
}

inline std::uint32_t ReadUint32(const char* data){
    return
        (static_cast<std::uint32_t>(ReadUint8(data))<<24)|
        (static_cast<std::uint32_t>(ReadUint8(data+1))<<16)|
        (static_cast<std::uint32_t>(ReadUint8(data+2))<<8)|
        static_cast<std::uint32_t>(ReadUint8(data+3));
}

inline std::uint64_t ReadUint64(const char* data){
    std::uint64_t value=0;

    for(int i=0;i<8;i++){
        value=(value<<8)|ReadUint8(data+i);
    }

    return value;
}

}
