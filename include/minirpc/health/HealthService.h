#pragma once

#include <string>

namespace minirpc::rpc{
class RpcServer;
}

namespace minirpc::health{

class HealthService{
public:
    static constexpr const char* ServiceName()noexcept{
        return "HealthService";
    }

    static constexpr const char* MethodName()noexcept{
        return "Check";
    }

    static constexpr const char* ServingPayload()noexcept{
        return "SERVING";
    }

    static void RegisterTo(rpc::RpcServer* server);
    static std::string Check(const std::string& request);
};

}
