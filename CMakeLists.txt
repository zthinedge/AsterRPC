cmake_minimum_required(VERSION 3.22)

project(mini_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(nlohmann_json 3.10 REQUIRED)

option(
    MINIRPC_WITH_ZOOKEEPER
    "Build the ZooKeeper service registry adapter"
    OFF
)

include(cmake/Protobuf.cmake)

add_library(minirpc
    src/common/ThreadPool.cpp
    src/config/RpcConfig.cpp
    src/cluster/Endpoint.cpp
    src/cluster/RoundRobin.cpp
    src/cluster/RetryPolicy.cpp
    src/cluster/ConnectionPool.cpp
    src/cluster/ChannelManager.cpp
    src/health/EndpointState.cpp
    src/health/HealthChecker.cpp
    src/health/HealthService.cpp
    src/loadbalance/P2cEwmaLoadBalancer.cpp
    src/log/AsyncLogger.cpp
    src/log/RollingFileSink.cpp
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
    src/rpc/RpcServer.cpp
    src/rpc/PendingCalls.cpp
    src/rpc/RpcClient.cpp
    src/trace/TraceContext.cpp
)

target_include_directories(minirpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(minirpc PUBLIC
    Threads::Threads
)
target_link_libraries(minirpc PRIVATE
    nlohmann_json::nlohmann_json
)

add_library(minirpc_gateway
    src/gateway/AdminApi.cpp
    src/gateway/AdminPage.cpp
    src/gateway/HttpGatewayServer.cpp
    src/gateway/HttpMessage.cpp
    src/gateway/ProtobufHttpGateway.cpp
)
target_include_directories(minirpc_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(minirpc_gateway
    PUBLIC
        minirpc
        protobuf::libprotobuf
    PRIVATE
        nlohmann_json::nlohmann_json
)

if(MINIRPC_WITH_ZOOKEEPER)
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

    add_library(minirpc_zookeeper
        src/registry/ZooKeeperClient.cpp
        src/registry/ZooKeeperConfigCenter.cpp
        src/registry/ZooKeeperDiscovery.cpp
        src/registry/ZooKeeperProvider.cpp
    )
    target_include_directories(minirpc_zookeeper
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE
            ${ZOOKEEPER_INCLUDE_DIR}
    )
    target_link_libraries(minirpc_zookeeper
        PUBLIC
            minirpc
        PRIVATE
            ${ZOOKEEPER_MT_LIBRARY}
    )

    add_executable(registry_server
        examples/registry/registry_server.cpp
    )
    target_link_libraries(registry_server PRIVATE
        minirpc_zookeeper
    )

    add_executable(registry_client
        examples/registry/registry_client.cpp
    )
    target_link_libraries(registry_client PRIVATE
        minirpc_zookeeper
    )
endif()

add_subdirectory(examples/services/calculator)

if(MINIRPC_WITH_ZOOKEEPER)
    add_executable(http_gateway
        examples/gateway/GatewayRuntime.cpp
        examples/gateway/http_gateway.cpp
    )
    target_link_libraries(http_gateway PRIVATE
        calculator_proto
        minirpc_gateway
        minirpc_zookeeper
    )

    add_executable(gateway_calculator_server
        examples/gateway/gateway_calculator_server.cpp
    )
    target_link_libraries(gateway_calculator_server PRIVATE
        calculator_rpc
        minirpc_zookeeper
    )
endif()

add_executable(async_logger_demo
    examples/logging/async_logger_demo.cpp
)
target_link_libraries(async_logger_demo PRIVATE
    minirpc
)

add_executable(calculator_server
    examples/server/calculator_server.cpp
)
target_link_libraries(calculator_server PRIVATE
    calculator_rpc
)

add_executable(calculator_client
    examples/client/calculator_client.cpp
)
target_link_libraries(calculator_client PRIVATE
    calculator_rpc
)

add_executable(rpc_bench
    benchmarks/rpc_bench.cpp
)
target_link_libraries(rpc_bench PRIVATE
    minirpc
)

enable_testing()

add_executable(test_socket
    tests/net/testsocket.cpp
)
add_executable(test_buffer
    tests/net/testbuffer.cpp
)
add_executable(test_connector
    tests/net/testconnector.cpp
)
add_executable(test_tcp
    tests/net/testtcp.cpp
)
add_executable(test_event_loop
    tests/net/testeventloop.cpp
)
add_executable(test_event_loop_thread_pool
    tests/net/test_event_loop_thread_pool.cpp
)
add_executable(test_thread_pool
    tests/common/test_thread_pool.cpp
)
add_executable(test_rpc_codec
    tests/protocol/test_rpc_codec.cpp
)
add_executable(test_rpc_server
    tests/rpc/test_rpc_server.cpp
)
add_executable(test_rpc_client
    tests/rpc/test_rpc_client.cpp
)
add_executable(test_async_logger
    tests/log/test_async_logger.cpp
)
add_executable(test_connection_pool
    tests/cluster/test_connection_pool.cpp
)
add_executable(test_round_robin
    tests/cluster/test_round_robin.cpp
)
add_executable(test_p2c_ewma
    tests/loadbalance/test_p2c_ewma.cpp
)
add_executable(test_health_checker
    tests/health/test_health_checker.cpp
)
add_executable(test_rpc_metrics
    tests/metrics/test_rpc_metrics.cpp
)
add_executable(test_trace_context
    tests/trace/test_trace_context.cpp
)
add_executable(test_rpc_config
    tests/config/test_rpc_config.cpp
)
add_executable(test_http_gateway
    tests/gateway/test_http_gateway.cpp
)
if(MINIRPC_WITH_ZOOKEEPER)
    add_executable(test_zookeeper_registry
        tests/registry/test_zookeeper_registry.cpp
    )
endif()

set(MINIRPC_TEST_TARGETS
    test_socket
    test_buffer
    test_connector
    test_tcp
    test_event_loop
    test_event_loop_thread_pool
    test_thread_pool
    test_rpc_codec
    test_rpc_server
    test_rpc_client
    test_async_logger
    test_connection_pool
    test_round_robin
    test_p2c_ewma
    test_health_checker
    test_rpc_metrics
    test_trace_context
    test_rpc_config
    test_http_gateway
)
if(MINIRPC_WITH_ZOOKEEPER)
    list(APPEND MINIRPC_TEST_TARGETS test_zookeeper_registry)
endif()

# 现有测试使用 assert 做检查，Release 构建也必须保留这些断言。
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    foreach(test_target IN LISTS MINIRPC_TEST_TARGETS)
        target_compile_options(${test_target} PRIVATE -UNDEBUG)
    endforeach()
endif()

target_link_libraries(test_socket PRIVATE
    minirpc
)
target_link_libraries(test_buffer PRIVATE
    minirpc
)
target_link_libraries(test_connector PRIVATE
    minirpc
)
target_link_libraries(test_tcp PRIVATE
    minirpc
)
target_link_libraries(test_event_loop PRIVATE
    minirpc
)
target_link_libraries(test_event_loop_thread_pool PRIVATE
    minirpc
)
target_link_libraries(test_thread_pool PRIVATE
    minirpc
)
target_link_libraries(test_rpc_codec PRIVATE
    minirpc
)
target_link_libraries(test_rpc_server PRIVATE
    minirpc
)
target_link_libraries(test_rpc_client PRIVATE
    calculator_rpc
)
target_link_libraries(test_async_logger PRIVATE
    minirpc
)
target_link_libraries(test_connection_pool PRIVATE
    minirpc
)
target_link_libraries(test_round_robin PRIVATE
    minirpc
)
target_link_libraries(test_p2c_ewma PRIVATE
    minirpc
)
target_link_libraries(test_health_checker PRIVATE
    minirpc
)
target_link_libraries(test_rpc_metrics PRIVATE
    minirpc
)
target_link_libraries(test_trace_context PRIVATE
    minirpc
)
target_link_libraries(test_rpc_config PRIVATE
    minirpc
)
target_link_libraries(test_http_gateway PRIVATE
    calculator_proto
    minirpc_gateway
)
if(MINIRPC_WITH_ZOOKEEPER)
    target_link_libraries(test_zookeeper_registry PRIVATE
        minirpc_zookeeper
    )
endif()


add_test(NAME test_socket COMMAND test_socket)
add_test(NAME test_buffer COMMAND test_buffer)
add_test(NAME test_connector COMMAND test_connector)
add_test(NAME test_tcp COMMAND test_tcp)
add_test(NAME test_event_loop COMMAND test_event_loop)
add_test(
    NAME test_event_loop_thread_pool
    COMMAND test_event_loop_thread_pool
)
add_test(NAME test_thread_pool COMMAND test_thread_pool)
add_test(NAME test_rpc_codec COMMAND test_rpc_codec)
add_test(NAME test_rpc_server COMMAND test_rpc_server)
add_test(NAME test_rpc_client COMMAND test_rpc_client)
add_test(NAME test_async_logger COMMAND test_async_logger)
add_test(NAME test_connection_pool COMMAND test_connection_pool)
add_test(NAME test_round_robin COMMAND test_round_robin)
add_test(NAME test_p2c_ewma COMMAND test_p2c_ewma)
add_test(NAME test_health_checker COMMAND test_health_checker)
add_test(NAME test_rpc_metrics COMMAND test_rpc_metrics)
add_test(NAME test_trace_context COMMAND test_trace_context)
add_test(NAME test_rpc_config COMMAND test_rpc_config)
add_test(NAME test_http_gateway COMMAND test_http_gateway)
if(MINIRPC_WITH_ZOOKEEPER)
    add_test(
        NAME test_zookeeper_registry
        COMMAND test_zookeeper_registry
    )
endif()
