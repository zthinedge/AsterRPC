set(PROTO_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/calculator.proto
)
set(GENERATED_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)
set(GENERATED_SOURCE
    ${GENERATED_DIR}/calculator.pb.cc
)
set(GENERATED_HEADER
    ${GENERATED_DIR}/calculator.pb.h
)

file(MAKE_DIRECTORY ${GENERATED_DIR})

add_custom_command(
    OUTPUT
        ${GENERATED_SOURCE}
        ${GENERATED_HEADER}
    COMMAND
        $<TARGET_FILE:protobuf::protoc>
    ARGS
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        --cpp_out=${GENERATED_DIR}
        ${PROTO_FILE}
    DEPENDS
        protobuf::protoc
        ${PROTO_FILE}
    COMMENT
        "Generating calculator Protobuf sources"
    VERBATIM
)

add_library(calculator_proto
    ${GENERATED_SOURCE}
    ${GENERATED_HEADER}
)

target_include_directories(calculator_proto PUBLIC
    ${GENERATED_DIR}
)

target_link_libraries(calculator_proto PUBLIC
    protobuf::libprotobuf
)

add_library(calculator_rpc
    CalculatorService.cpp
    CalculatorService.h
    CalculatorStub.cpp
    CalculatorStub.h
)

target_include_directories(calculator_rpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(calculator_rpc PUBLIC
    calculator_proto
    asterrpc
)
