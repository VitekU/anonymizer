cmake_minimum_required(VERSION 3.14)
project(anonymizer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DEPS REQUIRED IMPORTED_TARGET rdkafka capnp)

add_executable(
    consumer
    src/consumer.cpp
    src/decoder.cpp
    src/setup.cpp
    src/http_log.capnp.c++
)

target_link_libraries(consumer PRIVATE PkgConfig::DEPS)