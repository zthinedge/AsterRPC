#include "minirpc/gateway/ProtobufHttpGateway.h"

#include "minirpc/gateway/RpcChannel.h"
#include "minirpc/protocol/RpcMeta.h"
#include "minirpc/trace/TraceContext.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace minirpc::gateway{
namespace{

struct Route{
    std::string service;
    std::string method;
};

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

bool IsValidTraceId(std::string_view trace_id){
    if(trace_id.empty()||trace_id.size()>128){
        return false;
    }
    return std::all_of(
        trace_id.begin(),
        trace_id.end(),
        [](unsigned char character){
            return std::isalnum(character)||
                   character=='-'||
                   character=='_'||
                   character=='.';
        }
    );
}

bool ParseRoute(const std::string& target,Route* route){
    constexpr std::string_view prefix="/rpc/";
    std::string_view path(target);
    std::size_t query=path.find('?');
    if(query!=std::string_view::npos){
        path=path.substr(0,query);
    }
    if(path.substr(0,prefix.size())!=prefix){
        return false;
    }

    path.remove_prefix(prefix.size());
    std::size_t separator=path.find('/');
    if(separator==std::string_view::npos||
       separator==0||
       separator+1>=path.size()||
       path.find('/',separator+1)!=std::string_view::npos){
        return false;
    }

    route->service=std::string(path.substr(0,separator));
    route->method=std::string(path.substr(separator+1));
    return true;
}

HttpResponse JsonError(
    int status,
    std::string reason,
    std::string code,
    std::string message,
    const std::string& trace_id
){
    HttpResponse response;
    response.status=status;
    response.reason=std::move(reason);
    response.headers.emplace("X-Trace-Id",trace_id);
    response.body=nlohmann::json{
        {"error",std::move(code)},
        {"message",std::move(message)},
        {"trace_id",trace_id}
    }.dump();
    return response;
}

HttpResponse RpcErrorResponse(
    protocol::StatusCode status,
    const std::string& message,
    const std::string& trace_id
){
    switch(status){
        case protocol::StatusCode::ServiceNotFound:
        case protocol::StatusCode::MethodNotFound:
            return JsonError(
                404,
                "Not Found",
                "rpc_not_found",
                message,
                trace_id
            );
        case protocol::StatusCode::Timeout:
            return JsonError(
                504,
                "Gateway Timeout",
                "rpc_timeout",
                message,
                trace_id
            );
        case protocol::StatusCode::ConnectionFailed:
            return JsonError(
                503,
                "Service Unavailable",
                "rpc_unavailable",
                message,
                trace_id
            );
        case protocol::StatusCode::DecodeError:
            return JsonError(
                502,
                "Bad Gateway",
                "rpc_decode_error",
                message,
                trace_id
            );
        case protocol::StatusCode::InvokeError:
        case protocol::StatusCode::InternalError:
            return JsonError(
                500,
                "Internal Server Error",
                "rpc_internal_error",
                message,
                trace_id
            );
        case protocol::StatusCode::Ok:
            break;
    }
    return JsonError(
        500,
        "Internal Server Error",
        "rpc_unknown_error",
        message,
        trace_id
    );
}

}

class ProtobufHttpGateway::State{
public:
    State(
        RpcChannel* channel,
        ProtobufHttpGatewayOptions options
    ):channel_(channel),
      options_(options){
        if(channel_==nullptr){
            throw std::invalid_argument(
                "HTTP gateway RPC channel is null"
            );
        }
        if(options_.default_timeout<=
           std::chrono::milliseconds::zero()){
            throw std::invalid_argument(
                "HTTP gateway timeout must be positive"
            );
        }
    }

    void RegisterService(
        const google::protobuf::ServiceDescriptor* service
    ){
        if(service==nullptr){
            throw std::invalid_argument(
                "HTTP gateway service descriptor is null"
            );
        }

        std::string full_name(
            service->full_name().data(),
            service->full_name().size()
        );
        std::string short_name_value(
            service->name().data(),
            service->name().size()
        );

        std::lock_guard<std::mutex> lock(mutex_);
        auto full=services_.find(full_name);
        if(full!=services_.end()&&full->second!=service){
            throw std::logic_error(
                "duplicate Protobuf service name: "+full_name
            );
        }

        auto short_name=services_.find(short_name_value);
        if(short_name!=services_.end()&&short_name->second!=service){
            throw std::logic_error(
                "ambiguous Protobuf short service name: "+
                short_name_value
            );
        }

        services_[full_name]=service;
        services_[short_name_value]=service;
    }

    void Handle(HttpRequest request,Completion completion){
        trace::TraceContext request_trace=trace::CreateRootTrace();
        std::string inherited_trace=request.Header("x-trace-id");
        if(!inherited_trace.empty()){
            if(!IsValidTraceId(inherited_trace)){
                completion(JsonError(
                    400,
                    "Bad Request",
                    "invalid_trace_id",
                    "X-Trace-Id contains unsupported characters",
                    request_trace.trace_id
                ));
                return;
            }
            request_trace.trace_id=std::move(inherited_trace);
        }
        const std::string trace_id=request_trace.trace_id;

        if(request.method!="POST"){
            HttpResponse response=JsonError(
                405,
                "Method Not Allowed",
                "method_not_allowed",
                "only POST is supported",
                trace_id
            );
            response.headers.emplace("Allow","POST");
            completion(std::move(response));
            return;
        }

        std::string content_type=Lower(request.Header("content-type"));
        bool is_json=content_type=="application/json"||
                     content_type.compare(
                         0,
                         17,
                         "application/json;"
                     )==0;
        if(!is_json){
            completion(JsonError(
                415,
                "Unsupported Media Type",
                "unsupported_media_type",
                "Content-Type must be application/json",
                trace_id
            ));
            return;
        }

        Route route;
        if(!ParseRoute(request.target,&route)){
            completion(JsonError(
                404,
                "Not Found",
                "route_not_found",
                "expected POST /rpc/{service}/{method}",
                trace_id
            ));
            return;
        }

        const google::protobuf::ServiceDescriptor* service=nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found=services_.find(route.service);
            if(found!=services_.end()){
                service=found->second;
            }
        }
        if(service==nullptr){
            completion(JsonError(
                404,
                "Not Found",
                "service_not_found",
                "unknown Protobuf service: "+route.service,
                trace_id
            ));
            return;
        }

        const google::protobuf::MethodDescriptor* method=
            service->FindMethodByName(route.method);
        if(method==nullptr){
            completion(JsonError(
                404,
                "Not Found",
                "method_not_found",
                "unknown Protobuf method: "+route.method,
                trace_id
            ));
            return;
        }

        google::protobuf::DynamicMessageFactory request_factory;
        const google::protobuf::Message* request_prototype=
            request_factory.GetPrototype(method->input_type());
        if(request_prototype==nullptr){
            completion(JsonError(
                500,
                "Internal Server Error",
                "descriptor_error",
                "request message prototype is unavailable",
                trace_id
            ));
            return;
        }

        std::unique_ptr<google::protobuf::Message> protobuf_request(
            request_prototype->New()
        );
        auto json_status=google::protobuf::util::JsonStringToMessage(
            request.body,
            protobuf_request.get()
        );
        if(!json_status.ok()){
            completion(JsonError(
                400,
                "Bad Request",
                "invalid_protobuf_json",
                std::string(json_status.message()),
                trace_id
            ));
            return;
        }

        std::string payload;
        if(!protobuf_request->SerializeToString(&payload)){
            completion(JsonError(
                500,
                "Internal Server Error",
                "protobuf_encode_error",
                "failed to serialize Protobuf request",
                trace_id
            ));
            return;
        }

        rpc::CallOptions call_options;
        call_options.timeout=options_.default_timeout;
        auto idempotency=method->options().idempotency_level();
        call_options.idempotent=
            idempotency==
                google::protobuf::MethodOptions::
                    IDEMPOTENT||
            idempotency==
                google::protobuf::MethodOptions::
                    NO_SIDE_EFFECTS;
        trace::TraceScope trace_scope(request_trace);

        try{
            channel_->AsyncCall(
                std::string(
                    service->full_name().data(),
                    service->full_name().size()
                ),
                std::string(
                    method->name().data(),
                    method->name().size()
                ),
                std::move(payload),
                call_options,
                [
                    method,
                    trace_id,
                    completion
                ](protocol::RpcMessage response)mutable{
                    if(response.meta.status_code!=
                       protocol::StatusCode::Ok){
                        completion(RpcErrorResponse(
                            response.meta.status_code,
                            response.meta.error_text,
                            trace_id
                        ));
                        return;
                    }

                    google::protobuf::DynamicMessageFactory
                        response_factory;
                    const google::protobuf::Message*
                        response_prototype=
                            response_factory.GetPrototype(
                                method->output_type()
                            );
                    if(response_prototype==nullptr){
                        completion(JsonError(
                            500,
                            "Internal Server Error",
                            "descriptor_error",
                            "response message prototype is unavailable",
                            trace_id
                        ));
                        return;
                    }

                    std::unique_ptr<google::protobuf::Message>
                        protobuf_response(response_prototype->New());
                    if(!protobuf_response->ParseFromString(
                           response.payload
                       )){
                        completion(JsonError(
                            502,
                            "Bad Gateway",
                            "invalid_protobuf_response",
                            "RPC response is not valid Protobuf",
                            trace_id
                        ));
                        return;
                    }

                    std::string json;
                    auto status=
                        google::protobuf::util::MessageToJsonString(
                            *protobuf_response,
                            &json
                        );
                    if(!status.ok()){
                        completion(JsonError(
                            500,
                            "Internal Server Error",
                            "protobuf_json_error",
                            std::string(status.message()),
                            trace_id
                        ));
                        return;
                    }

                    HttpResponse http_response;
                    http_response.headers.emplace(
                        "X-Trace-Id",
                        trace_id
                    );
                    http_response.body=std::move(json);
                    completion(std::move(http_response));
                }
            );
        }catch(const std::exception& error){
            completion(JsonError(
                503,
                "Service Unavailable",
                "rpc_channel_error",
                error.what(),
                trace_id
            ));
        }
    }

private:
    RpcChannel* channel_;
    ProtobufHttpGatewayOptions options_;
    std::mutex mutex_;
    std::map<
        std::string,
        const google::protobuf::ServiceDescriptor*
    > services_;
};

ProtobufHttpGateway::ProtobufHttpGateway(
    RpcChannel* channel,
    ProtobufHttpGatewayOptions options
):state_(std::make_shared<State>(channel,options)){}

ProtobufHttpGateway::~ProtobufHttpGateway()=default;

void ProtobufHttpGateway::RegisterService(
    const google::protobuf::ServiceDescriptor* service
){
    state_->RegisterService(service);
}

void ProtobufHttpGateway::Handle(
    HttpRequest request,
    Completion completion
){
    if(!completion){
        throw std::invalid_argument(
            "HTTP gateway completion callback is empty"
        );
    }
    state_->Handle(std::move(request),std::move(completion));
}

}
