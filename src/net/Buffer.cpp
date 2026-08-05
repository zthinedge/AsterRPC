#include "asterrpc/net/Buffer.h"

namespace asterrpc::net{

Buffer::Buffer(size_t size)
    : buf_(size),
      reader_idx_(0),
      writer_idx_(0) {}


//当前buf有多少字节可读
size_t Buffer::ReadableBytes()const noexcept{
    return writer_idx_-reader_idx_;
}

//当前buf后面还有多少字节可写--用于扩容
size_t Buffer::WritableBytes()const noexcept{
    return buf_.size()-writer_idx_;
}

//返回可读数据的起始位置
const char* Buffer::Peek()const noexcept{
    return buf_.data() + reader_idx_;
}

//移除可读区域中的前len个字节
void Buffer::Retrieve(size_t len){
    if(len<ReadableBytes()){
        reader_idx_+=len;
    }else{
        RetrieveAll();
    }
}


void Buffer::RetrieveAll() noexcept{
    reader_idx_ = 0;
    writer_idx_ = 0;
}

std::string Buffer::RetrieveAsString(std::size_t len){
    // std::string res;
    // for(size_t i=reader_idx_;i<writer_idx_&&i<reader_idx_+len;i++){
    //     res+=buf_[i];
    // }
    // Retrieve(len);
    // return res;

    if (len > ReadableBytes()) {
        len = ReadableBytes();
    }

    std::string res(Peek(), len);
    Retrieve(len);
    return res;
}

std::string Buffer::RetrieveAllAsString(){
    return RetrieveAsString(ReadableBytes());
}

void Buffer::Append(const std::string& data){
    Append(data.data(), data.size());
}

void Buffer::Append(const char*data,size_t len){
    //扩容
    if (WritableBytes() < len) {
        buf_.resize(writer_idx_ + len);
    }

    std::copy(data, data + len, buf_.begin() + writer_idx_);
    writer_idx_ += len;
}

}