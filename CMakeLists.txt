cmake_minimum_required(VERSION 3.16)
project(Minilog VERSION 1.0.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_library(minilog STATIC src/minilog.cpp)
target_include_directories(minilog PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_options(minilog PRIVATE -Wall -Wextra -Wpedantic)
add_executable(main example/main.cpp)

target_link_libraries(main PRIVATE minilog)

find_package(Threads REQUIRED)
target_link_libraries(main PRIVATE Threads::Threads)

install(TARGETS minilog
    EXPORT minilogTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION include
)
install(EXPORT minilogTargets
    FILE minilogTargets.cmake
    NAMESPACE minilog::
  
    DESTINATION lib/cmake/minilog
)
include(CMakePackageConfigHelpers)
Configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/minilogConfig.cmake.in
    ${CMAKE_BINARY_DIR}/minilogConfig.cmake
    INSTALL_DESTINATION lib/cmake/minilog
)
install(FILES ${CMAKE_BINARY_DIR}/minilogConfig.cmake
    DESTINATION lib/cmake/minilog
)