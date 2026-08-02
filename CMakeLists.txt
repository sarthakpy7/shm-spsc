cmake_minimum_required(VERSION 3.16)
project(shmspsc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(shmspsc INTERFACE)
target_include_directories(shmspsc INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

if(NOT APPLE AND UNIX)
  target_link_libraries(shmspsc INTERFACE rt) # shm_open lives in librt on Linux
endif()

foreach(target test_queue bench_throughput bench_latency demo_producer demo_consumer)
  add_executable(${target} src/${target}.cpp)
  target_link_libraries(${target} PRIVATE shmspsc)
  target_include_directories(${target} PRIVATE src)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
endforeach()

enable_testing()
add_test(NAME queue COMMAND test_queue)
