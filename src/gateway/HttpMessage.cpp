#include "asterrpc/gateway/HttpMessage.h"

#include "asterrpc/net/Buffer.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace asterrpc::gateway{
namespace{

std::string Lower(std::string value){
    std::transform(
        value.begin(),
        value.end(),
        value.begin(),
        [](unsigned char character){
            return static_cast<char>(std::tolower(character));
        }
    );
    return value;
}

std::string_view Trim(std::string_view value){
    while(!value.empty()&&
          std::isspace(static_cast<unsigned char>(value.front()))){
        value.remove_prefix(1);
    }
    while(!value.empty()&&
          std::isspace(static_cast<unsigned char>(value.back()))){
        value.remove_suffix(1);
    }
    return value;
}

bool ParseContentLength(
    const std::string& value,
    std::size_t* result
){
    if(value.empty()){
        return false;
    }

    std::size_t length=0;
    for(unsigned char character:value){
        if(!std::isdigit(character)){
            return false;
        }
        std::size_t digit=static_cast<std::size_t>(
            character-static_cast<unsigned char>('0')
        );
        if(length>
           (std::numeric_limits<std::size_t>::max()-digit)/10){
            return false;
        }
        length=length*10+digit;
    }
    *result=length;
    return true;
}

HttpParseResult Error(std::string message){
    return {HttpParseStatus::Error,std::move(message)};
}

}

std::string HttpRequest::Header(const std::string& name)const{
    auto header=headers.find(Lower(name));
    return header==headers.end()?std::string{}:header->second;
}

HttpParseResult ParseHttpRequest(
    net::Buffer* buffer,
    HttpRequest* request,
    HttpParserLimits limits
){
    if(buffer==nullptr||request==nullptr){
        throw std::invalid_argument(
            "HTTP parser input must not be null"
        );
    }
    if(limits.max_header_bytes==0||limits.max_body_bytes==0){
        throw std::invalid_argument(
            "HTTP parser limits must be positive"
        );
    }

    const char* begin=buffer->Peek();
    std::size_t readable=buffer->ReadableBytes();
    constexpr std::string_view delimiter="\r\n\r\n";
    const char* header_end=std::search(
        begin,
        begin+readable,
        delimiter.begin(),
        delimiter.end()
    );

    if(header_end==begin+readable){
        if(readable>limits.max_header_bytes){
            return Error("HTTP headers are too large");
        }
        return {};
    }

    std::size_t header_bytes=static_cast<std::size_t>(
        header_end-begin
    )+delimiter.size();
    if(header_bytes>limits.max_header_bytes){
        return Error("HTTP headers are too large");
    }

    std::string header_block(begin,header_bytes-delimiter.size());
    std::size_t first_line_end=header_block.find("\r\n");
    std::string request_line=header_block.substr(0,first_line_end);
    std::istringstream line_stream(request_line);

    HttpRequest parsed;
    std::string trailing;
    if(!(line_stream>>parsed.method>>parsed.target>>parsed.version)||
       (line_stream>>trailing)){
        return Error("invalid HTTP request line");
    }
    if(parsed.version!="HTTP/1.1"&&parsed.version!="HTTP/1.0"){
        return Error("unsupported HTTP version");
    }

    std::size_t cursor=first_line_end==std::string::npos
        ?header_block.size():first_line_end+2;
    while(cursor<header_block.size()){
        std::size_t line_end=header_block.find("\r\n",cursor);
        if(line_end==std::string::npos){
            line_end=header_block.size();
        }

        std::string_view line(
            header_block.data()+cursor,
            line_end-cursor
        );
        std::size_t colon=line.find(':');
        if(colon==std::string_view::npos){
            return Error("invalid HTTP header");
        }

        std::string name=Lower(std::string(Trim(line.substr(0,colon))));
        std::string value(Trim(line.substr(colon+1)));
        if(name.empty()||parsed.headers.count(name)!=0){
            return Error("invalid or duplicate HTTP header");
        }
        parsed.headers.emplace(std::move(name),std::move(value));
        cursor=line_end+2;
    }

    if(parsed.headers.count("transfer-encoding")!=0){
        return Error("Transfer-Encoding is not supported");
    }

    std::size_t content_length=0;
    auto length_header=parsed.headers.find("content-length");
    if(length_header!=parsed.headers.end()&&
       !ParseContentLength(length_header->second,&content_length)){
        return Error("invalid Content-Length");
    }
    if(content_length>limits.max_body_bytes){
        return Error("HTTP body is too large");
    }
    if(readable-header_bytes<content_length){
        return {};
    }

    parsed.body.assign(begin+header_bytes,content_length);
    buffer->Retrieve(header_bytes+content_length);
    *request=std::move(parsed);
    return {HttpParseStatus::Complete,{}};
}

std::string SerializeHttpResponse(const HttpResponse& response){
    std::ostringstream output;
    output<<"HTTP/1.1 "<<response.status<<' '
          <<response.reason<<"\r\n";

    bool has_content_type=false;
    for(const auto& header:response.headers){
        std::string name=Lower(header.first);
        if(name=="content-length"||name=="connection"){
            continue;
        }
        if(name=="content-type"){
            has_content_type=true;
        }
        output<<header.first<<": "<<header.second<<"\r\n";
    }

    if(!has_content_type){
        output<<"Content-Type: application/json\r\n";
    }
    output<<"Content-Length: "<<response.body.size()<<"\r\n"
          <<"Connection: close\r\n\r\n"
          <<response.body;
    return output.str();
}

}
