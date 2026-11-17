cmake_minimum_required(VERSION 3.16)
project(kk)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# redis-plus-plus 头文件和生成文件路径全家桶
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/src)
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/build)
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/build/sw)
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/src/sw/redis++/cxx17)
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/src/sw/redis++/no_tls)
include_directories(/home/xh/8.14/tcp/tmp/redis-plus-plus/src/sw/redis++/no_tls/sw)
link_directories(/usr/local/lib)

# === 原有的 ===
add_library(socket_fd STATIC socket_fd.cpp)
find_package(minilog REQUIRED NO_MODULE)
find_package(Threads REQUIRED)

add_executable(serve 
    serve.cpp 
    Timer.cpp 
    TimerQueue.cpp 
    EventLoop.cpp 
    EventLoopThreadPool.cpp 
    redis/RedisManager.cpp 
)
add_executable(client client.cpp)

target_include_directories(serve PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(client PRIVATE minilog::minilog Threads::Threads socket_fd)

target_include_directories(serve PRIVATE /usr/include/mysql)
target_link_libraries(serve PRIVATE 
    minilog::minilog 
    Threads::Threads 
    socket_fd
    mysqlclient
    redis++
    hiredis
)

# === 测试 ===
enable_testing()
find_package(GTest REQUIRED)

add_executable(serve_tests
    tests/test_utils.cpp
    tests/test_buffer.cpp
    tests/test_blacklist.cpp
    tests/test_pack_msg.cpp
    tests/test_safe_queue.cpp
)
target_include_directories(serve_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(serve_tests GTest::gtest_main pthread)

include(GoogleTest)
gtest_discover_tests(serve_tests)