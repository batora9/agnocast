cmake_minimum_required(VERSION 3.16)
project(agnocast_daemon CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../agnocast_kmod/version.txt" AGNOCAST_VERSION_RAW)
string(STRIP "${AGNOCAST_VERSION_RAW}" AGNOCAST_VERSION_RAW)

add_executable(agnocast_daemon
  main.cpp
  metadata_store.cpp
  memory_allocator.cpp
  command_handlers.cpp
  socket_server.cpp
)

target_compile_definitions(agnocast_daemon PRIVATE AGNOCAST_VERSION="${AGNOCAST_VERSION_RAW}")

target_include_directories(agnocast_daemon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(agnocast_daemon PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(agnocast_daemon PRIVATE pthread)
