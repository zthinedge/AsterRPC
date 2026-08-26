cmake_minimum_required(VERSION 3.22)

project(AsterRPC LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

include(cmake/Protobuf.cmake)

add_library(asterrpc
    src/common/ThreadPool.cpp
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
)
add_library(AsterRPC::Core ALIAS asterrpc)

target_include_directories(asterrpc
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(asterrpc PUBLIC
    Threads::Threads
)
add_subdirectory(examples/services/calculator)

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

add_executable(rpc_benchmark
    benchmarks/rpc_benchmark.cpp
)
target_link_libraries(rpc_benchmark PRIVATE
    asterrpc
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
asterrpc_add_test(test_rpc_core tests/rpc/test_rpc_core.cpp asterrpc)
