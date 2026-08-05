#pragma once
#include <string>
#include <vector>
#include <algorithm>
#include <cstddef>


namespace asterrpc::net{

class Buffer{

public:
    explicit Buffer(size_t size=1024);

    size_t ReadableBytes()const noexcept;
    size_t WritableBytes()const noexcept;

    const char* Peek()const noexcept;
    void Retrieve(size_t len);
    void RetrieveAll() noexcept;
    std::string RetrieveAsString(std::size_t len);
    std::string RetrieveAllAsString();
    
    void Append(const std::string& data);
    void Append(const char*data,size_t len);


private:
    std::vector<char>buf_;
    size_t reader_idx_;
    size_t writer_idx_;
};


}