#include "asterrpc/gateway/AdminApi.h"

#include "AdminPage.h"

#include <nlohmann/json.hpp>

#include <stdexcept>
#include <string_view>

namespace asterrpc::gateway{
namespace{

using Json=nlohmann::json;

std::string_view Path(const std::string& target)noexcept{
    std::string_view path(target);
    std::size_t query=path.find('?');
    return path.substr(0,query);
}

const char* HealthStatusName(
    health::HealthStatus status
)noexcept{
    switch(status){
        case health::HealthStatus::Healthy:
            return "healthy";
        case health::HealthStatus::Suspect:
            return "suspect";
        case health::HealthStatus::Unhealthy:
            return "unhealthy";
        case health::HealthStatus::HalfOpen:
            return "half_open";
    }
    return "unknown";
}

Json MetricsJson(const metrics::RpcMetricsSnapshot& metrics){
    return {
        {"total_requests",metrics.total_requests},
        {"successful_requests",metrics.successful_requests},
        {"failed_requests",metrics.failed_requests},
        {"timeout_requests",metrics.timeout_requests},
        {"retries",metrics.retries},
        {"inflight_requests",metrics.inflight_requests},
        {"active_connections",metrics.active_connections},
        {"latency_samples",metrics.latency_samples},
        {"average_latency_us",metrics.AverageLatencyMicros()},
        {"max_latency_us",metrics.max_latency_us},
        {"p50_latency_us",metrics.p50_latency_us},
        {"p95_latency_us",metrics.p95_latency_us},
        {"p99_latency_us",metrics.p99_latency_us}
    };
}

Json ServicesJson(const AdminDataSource& source){
    Json services=Json::array();
    for(const auto& service:source.Services()){
        services.push_back({
            {"name",service.name},
            {"methods",service.methods}
        });
    }
    return {{"services",std::move(services)}};
}

Json InstancesJson(const AdminDataSource& source){
    Json instances=Json::array();
    for(const auto& instance:source.Instances()){
        Json value={
            {"service",instance.service},
            {"endpoint",instance.endpoint},
            {"discovery_status",instance.discovery_status},
            {"pool",{
                {"connections",instance.pool.connections},
                {"connected",instance.pool.connected},
                {"inflight",instance.pool.in_flight},
                {"retries",instance.pool.retries}
            }}
        };
        if(instance.has_health_state){
            value["health"]={
                {"status",HealthStatusName(instance.health.status)},
                {"selectable",instance.health.selectable},
                {"inflight",instance.health.inflight},
                {"ewma_latency_us",instance.health.ewma_latency_us},
                {"consecutive_failures",
                 instance.health.consecutive_failures},
                {"weight",instance.health.weight},
                {"score",instance.health.score}
            };
        }else{
            value["health"]=nullptr;
        }
        instances.push_back(std::move(value));
    }
    return {{"instances",std::move(instances)}};
}

Json EndpointMetricsJson(const AdminDataSource& source){
    Json endpoints=Json::array();
    for(const auto& endpoint:source.Metrics()){
        Json methods=Json::array();
        for(const auto& method:endpoint.methods){
            methods.push_back({
                {"service",method.service_name},
                {"method",method.method_name},
                {"metrics",MetricsJson(method.metrics)}
            });
        }
        endpoints.push_back({
            {"endpoint",endpoint.endpoint},
            {"metrics",MetricsJson(endpoint.totals)},
            {"methods",std::move(methods)}
        });
    }
    return {{"endpoints",std::move(endpoints)}};
}

Json ConfigJson(const AdminDataSource& source){
    Json services=Json::array();
    for(const auto& entry:source.Config()){
        services.push_back({
            {"service",entry.service},
            {"default_timeout_ms",
             entry.config.default_timeout.count()},
            {"retry_count",entry.config.retry_count},
            {"load_balancer",
             config::ToString(entry.config.load_balancer)},
            {"health_check_interval_ms",
             entry.config.health_check_interval.count()},
            {"failure_threshold",
             entry.config.failure_threshold},
            {"ewma_alpha",entry.config.ewma_alpha}
        });
    }
    return {{"services",std::move(services)}};
}

Json TracesJson(const AdminDataSource& source){
    Json traces=Json::array();
    for(const auto& trace:source.Traces()){
        traces.push_back({
            {"trace_id",trace.trace_id},
            {"service",trace.service},
            {"method",trace.method},
            {"endpoint",trace.endpoint},
            {"status",trace.status},
            {"started_at_us",trace.started_at_us},
            {"latency_us",trace.latency_us}
        });
    }
    return {{"traces",std::move(traces)}};
}

HttpResponse JsonResponse(Json body,int status=200){
    HttpResponse response;
    response.status=status;
    response.reason=status==200?"OK":
                    status==404?"Not Found":
                    status==405?"Method Not Allowed":
                    status==503?"Service Unavailable":
                    "Internal Server Error";
    response.body=body.dump();
    return response;
}

HttpResponse AdminPageResponse(){
    HttpResponse response;
    response.headers.emplace(
        "Content-Type",
        "text/html; charset=utf-8"
    );
    response.headers.emplace("Cache-Control","no-store");
    response.headers.emplace(
        "Content-Security-Policy",
        "default-src 'self'; "
        "script-src 'unsafe-inline'; "
        "style-src 'unsafe-inline'; "
        "connect-src 'self'"
    );
    response.body=AdminPageHtml();
    return response;
}

}

AdminApi::AdminApi(AdminDataSource* data_source)
    :data_source_(data_source){
    if(data_source_==nullptr){
        throw std::invalid_argument(
            "admin API data source is null"
        );
    }
}

bool AdminApi::Matches(const std::string& target)noexcept{
    std::string_view path=Path(target);
    return path=="/admin"||
           path.substr(0,7)=="/admin/";
}

HttpResponse AdminApi::Handle(const HttpRequest& request)const{
    if(request.method!="GET"){
        HttpResponse response=JsonResponse({
            {"error","method_not_allowed"},
            {"message","admin API only supports GET"}
        },405);
        response.headers.emplace("Allow","GET");
        return response;
    }

    try{
        std::string_view path=Path(request.target);
        if(path=="/admin"||path=="/admin/"){
            return AdminPageResponse();
        }
        if(path=="/admin/api/services"){
            return JsonResponse(ServicesJson(*data_source_));
        }
        if(path=="/admin/api/instances"){
            return JsonResponse(InstancesJson(*data_source_));
        }
        if(path=="/admin/api/metrics"){
            return JsonResponse(EndpointMetricsJson(*data_source_));
        }
        if(path=="/admin/api/config"){
            return JsonResponse(ConfigJson(*data_source_));
        }
        if(path=="/admin/api/traces"){
            return JsonResponse(TracesJson(*data_source_));
        }
        if(path=="/admin/api/health"){
            AdminHealthInfo health=data_source_->Health();
            return JsonResponse({
                {"status",health.ready?"ready":"not_ready"},
                {"zookeeper_connected",
                 health.zookeeper_connected},
                {"services",health.services},
                {"instances",health.instances},
                {"selectable_instances",
                 health.selectable_instances}
            },health.ready?200:503);
        }
        return JsonResponse({
            {"error","admin_route_not_found"},
            {"message","unknown admin API route"}
        },404);
    }catch(const std::exception& error){
        return JsonResponse({
            {"error","admin_snapshot_error"},
            {"message",error.what()}
        },500);
    }
}

}
