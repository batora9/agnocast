cmake_minimum_required(VERSION 3.14)
project(agnocast_consistency)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(agnocastlib REQUIRED)
find_package(agnocast_sample_interfaces REQUIRED)

foreach(exe IN ITEMS consistency_publisher consistency_subscriber consistency_observer)
  add_executable(${exe} src/${exe}.cpp)
  ament_target_dependencies(${exe} agnocastlib agnocast_sample_interfaces)
  target_include_directories(${exe} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  install(TARGETS ${exe} DESTINATION lib/${PROJECT_NAME})
endforeach()

ament_package()
