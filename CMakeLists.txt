cmake_minimum_required(VERSION 3.22)

project(AsterRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(nlohmann_json 3.10 REQUIRED)

option(
    ASTERRPC_WITH_ZOOKEEPER
    "Build the ZooKeeper service registry adapter"
    OFF
)

include(cmake/Protobuf.cmake)

add_library(asterrpc
    src/common/ThreadPool.cpp
    src/cluster/RetryPolicy.cpp
    src/health/HealthService.cpp
    src/metrics/RpcMetrics.cpp
    src/net/InetAddress.cpp
    src/net/Socket.cpp
    src/net/Buffer.cpp
    src/net/Channel.cpp
    src/net/Poller.cpp
    src/net/EventLoop.cpp
    src/net/EventLoopThread.cpp
    src/net/EventLoopThreadPool.cpp
    src/net/TimerQueue.cpp
    src/net/Acceptor.cpp
    src/net/Connector.cpp
    src/net/TcpClient.cpp
    src/net/TcpConnection.cpp
    src/net/TcpServer.cpp
    src/protocol/RpcHeaderCodec.cpp
    src/protocol/RpcMetaCodec.cpp
    src/protocol/RpcCodec.cpp
    src/rpc/ServiceDispatcher.cpp
    src/rpc/detail/CallSupport.cpp
    src/rpc/RpcServer.cpp
    src/rpc/PendingCalls.cpp
    src/rpc/RpcClient.cpp
    src/trace/TraceContext.cpp
)
add_library(AsterRPC::Core ALIAS asterrpc)

target_include_directories(asterrpc
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(asterrpc PUBLIC
    Threads::Threads
)
add_library(asterrpc_cluster
    src/config/RpcConfig.cpp
    src/cluster/Endpoint.cpp
    src/cluster/RoundRobin.cpp
    src/cluster/ConnectionPool.cpp
    src/cluster/ChannelManager.cpp
    src/health/EndpointState.cpp
    src/health/HealthChecker.cpp
    src/loadbalance/P2cEwmaLoadBalancer.cpp
)
add_library(AsterRPC::Cluster ALIAS asterrpc_cluster)
target_include_directories(asterrpc_cluster
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(asterrpc_cluster
    PUBLIC asterrpc
    PRIVATE nlohmann_json::nlohmann_json
)

add_library(asterrpc_logging
    src/log/AsyncLogger.cpp
    src/log/RollingFileSink.cpp
)
add_library(AsterRPC::Logging ALIAS asterrpc_logging)
target_include_directories(asterrpc_logging
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(asterrpc_logging PUBLIC asterrpc)

set(ASTERRPC_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${ASTERRPC_GENERATED_DIR}")
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/web/admin.html" ASTERRPC_ADMIN_PAGE_HTML)
configure_file(
    src/gateway/AdminPage.cpp.in
    "${ASTERRPC_GENERATED_DIR}/AdminPage.cpp"
    @ONLY
)

add_library(asterrpc_gateway
    src/gateway/AdminApi.cpp
    "${ASTERRPC_GENERATED_DIR}/AdminPage.cpp"
    src/gateway/HttpGatewayServer.cpp
    src/gateway/HttpMessage.cpp
    src/gateway/ProtobufHttpGateway.cpp
)
add_library(AsterRPC::Gateway ALIAS asterrpc_gateway)
target_include_directories(asterrpc_gateway
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(asterrpc_gateway
    PUBLIC
        asterrpc_cluster
        protobuf::libprotobuf
    PRIVATE
        nlohmann_json::nlohmann_json
)

if(ASTERRPC_WITH_ZOOKEEPER)
    find_path(
        ZOOKEEPER_INCLUDE_DIR
        NAMES zookeeper/zookeeper.h
        REQUIRED
    )
    find_library(
        ZOOKEEPER_MT_LIBRARY
        NAMES zookeeper_mt
        REQUIRED
    )

    add_library(asterrpc_zookeeper
        src/registry/ZooKeeperClient.cpp
        src/registry/ZooKeeperConfigCenter.cpp
        src/registry/ZooKeeperDiscovery.cpp
        src/registry/ZooKeeperProvider.cpp
    )
    add_library(AsterRPC::ZooKeeper ALIAS asterrpc_zookeeper)
    target_include_directories(asterrpc_zookeeper
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE
            ${ZOOKEEPER_INCLUDE_DIR}
    )
    target_link_libraries(asterrpc_zookeeper
        PUBLIC
            asterrpc_cluster
        PRIVATE
            ${ZOOKEEPER_MT_LIBRARY}
    )

    add_executable(registry_server
        examples/registry/registry_server.cpp
    )
    target_link_libraries(registry_server PRIVATE
        asterrpc_zookeeper
        asterrpc_logging
    )

    add_executable(registry_client
        examples/registry/registry_client.cpp
    )
    target_link_libraries(registry_client PRIVATE
        asterrpc_zookeeper
        asterrpc_logging
    )
endif()

add_subdirectory(examples/services/calculator)

if(ASTERRPC_WITH_ZOOKEEPER)
    add_executable(http_gateway
        examples/gateway/GatewayRuntime.cpp
        examples/gateway/http_gateway.cpp
    )
    target_link_libraries(http_gateway PRIVATE
        calculator_proto
        asterrpc_gateway
        asterrpc_zookeeper
        asterrpc_logging
    )

    add_executable(gateway_calculator_server
        examples/gateway/gateway_calculator_server.cpp
    )
    target_link_libraries(gateway_calculator_server PRIVATE
        calculator_rpc
        asterrpc_zookeeper
        asterrpc_logging
    )
endif()

add_executable(async_logger_demo
    examples/logging/async_logger_demo.cpp
)
target_link_libraries(async_logger_demo PRIVATE
    asterrpc_logging
)

add_executable(calculator_server
    examples/server/calculator_server.cpp
)
target_link_libraries(calculator_server PRIVATE
    calculator_rpc
    asterrpc_logging
)

add_executable(calculator_client
    examples/client/calculator_client.cpp
)
target_link_libraries(calculator_client PRIVATE
    calculator_rpc
    asterrpc_logging
)

add_executable(rpc_bench
    benchmarks/rpc_bench.cpp
)
target_link_libraries(rpc_bench PRIVATE
    asterrpc_cluster
)

enable_testing()

function(asterrpc_add_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE ${ARGN})

    # 现有测试使用 assert，Release 构建也必须保留断言。
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target} PRIVATE -UNDEBUG)
    endif()

    add_test(NAME ${target} COMMAND ${target})
endfunction()

asterrpc_add_test(test_socket tests/net/testsocket.cpp asterrpc)
asterrpc_add_test(test_buffer tests/net/testbuffer.cpp asterrpc)
asterrpc_add_test(test_connector tests/net/testconnector.cpp asterrpc)
asterrpc_add_test(test_tcp tests/net/testtcp.cpp asterrpc)
asterrpc_add_test(test_event_loop tests/net/testeventloop.cpp asterrpc)
asterrpc_add_test(
    test_event_loop_thread_pool
    tests/net/test_event_loop_thread_pool.cpp
    asterrpc
)
asterrpc_add_test(test_thread_pool tests/common/test_thread_pool.cpp asterrpc)
asterrpc_add_test(test_rpc_codec tests/protocol/test_rpc_codec.cpp asterrpc)
asterrpc_add_test(test_rpc_server tests/rpc/test_rpc_server.cpp asterrpc)
asterrpc_add_test(test_rpc_client tests/rpc/test_rpc_client.cpp calculator_rpc)
asterrpc_add_test(
    test_async_logger
    tests/log/test_async_logger.cpp
    asterrpc_logging
)
asterrpc_add_test(
    test_connection_pool
    tests/cluster/test_connection_pool.cpp
    asterrpc_cluster
)
asterrpc_add_test(
    test_round_robin
    tests/cluster/test_round_robin.cpp
    asterrpc_cluster
)
asterrpc_add_test(
    test_p2c_ewma
    tests/loadbalance/test_p2c_ewma.cpp
    asterrpc_cluster
)
asterrpc_add_test(
    test_health_checker
    tests/health/test_health_checker.cpp
    asterrpc_cluster
)
asterrpc_add_test(test_rpc_metrics tests/metrics/test_rpc_metrics.cpp asterrpc)
asterrpc_add_test(test_trace_context tests/trace/test_trace_context.cpp asterrpc)
asterrpc_add_test(
    test_rpc_config
    tests/config/test_rpc_config.cpp
    asterrpc_cluster
)
asterrpc_add_test(
    test_http_gateway
    tests/gateway/test_http_gateway.cpp
    calculator_proto
    asterrpc_gateway
)
if(ASTERRPC_WITH_ZOOKEEPER)
    asterrpc_add_test(
        test_zookeeper_registry
        tests/registry/test_zookeeper_registry.cpp
        asterrpc_zookeeper
    )
endif()
