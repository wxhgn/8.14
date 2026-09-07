cmake_minimum_required(VERSION 3.16)
project(kk1)
set(CMAKE_CXX_STANDARD 17)
find_package(minilog REQUIRED NO_MODULE)
message("minilog")
add_executable(demo main.cpp)

target_link_libraries(demo PRIVATE minilog::minilog)
