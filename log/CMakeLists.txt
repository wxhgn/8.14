cmake_minimum_required(VERSION 3.16)
project(Minilog VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---------- 静态库 ----------
add_library(minilog STATIC minilog.cpp)

target_include_directories(minilog PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

target_compile_options(minilog PRIVATE -Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)
target_link_libraries(minilog PUBLIC Threads::Threads)

# ---------- 安装 ----------
include(GNUInstallDirs)

install(TARGETS minilog
    EXPORT minilogTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# 头文件在根目录，用 FILES 安装
install(FILES ${PROJECT_SOURCE_DIR}/minilog.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT minilogTargets
    FILE minilogTargets.cmake
    NAMESPACE minilog::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/minilog
)

include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/minilogConfig.cmake.in
    ${CMAKE_BINARY_DIR}/minilogConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/minilog
)

write_basic_package_version_file(
    ${CMAKE_BINARY_DIR}/minilogConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_BINARY_DIR}/minilogConfig.cmake
    ${CMAKE_BINARY_DIR}/minilogConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/minilog
)