#pragma once

#include <cstddef>
#include <map>
#include <string>

namespace asterrpc::net{
class Buffer;
}

namespace asterrpc::gateway{

struct HttpRequest{
    std::string method;
    std::string target;
    std::string version;
    std::map<std::string,std::string> headers;
    std::string body;

    std::string Header(const std::string& name)const;
};

struct HttpResponse{
    int status=200;
    std::string reason="OK";
    std::map<std::string,std::string> headers;
    std::string body;
};

enum class HttpParseStatus{
    NeedMoreData,
    Complete,
    Error
};

struct HttpParseResult{
    HttpParseStatus status=HttpParseStatus::NeedMoreData;
    std::string error;
};

struct HttpParserLimits{
    std::size_t max_header_bytes=32*1024;
    std::size_t max_body_bytes=4*1024*1024;
};

HttpParseResult ParseHttpRequest(
    net::Buffer* buffer,
    HttpRequest* request,
    HttpParserLimits limits={}
);

std::string SerializeHttpResponse(const HttpResponse& response);

}
