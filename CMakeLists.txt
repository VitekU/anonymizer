cmake_minimum_required(VERSION 3.14)
project(anonymizer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DEPS REQUIRED IMPORTED_TARGET rdkafka capnp)

include(FetchContent)

FetchContent_Declare(
    clickhouse_cpp
    GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp.git
    GIT_TAG v2.6.2
)
FetchContent_MakeAvailable(clickhouse_cpp)

add_executable(
    consumer
    src/consumer.cpp
    src/decoder.cpp
    src/setup.cpp
    src/http_log.capnp.c++
    src/db_client_functions.cpp
)

target_link_libraries(consumer PRIVATE PkgConfig::DEPS clickhouse-cpp-lib)