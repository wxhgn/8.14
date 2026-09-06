# TCP多线程聊天室
基于C++17实现的TCP聊天室，CMake构建，支持多客户端同时接入聊天，使用MySQL持久化用户信息与聊天记录。

## 项目简介
实现一个C/S架构聊天室服务端与客户端。服务端可以接收多个客户端连接，客户端发送消息，服务端广播消息给所有在线用户；支持用户登录，聊天数据存入MySQL数据库。
锻炼 Linux Socket编程、多线程同步、CMake工程管理、数据库调用能力。

### 功能特性
- ✅ TCP Socket 服务端 + 客户端
- ✅ 多线程处理多个客户端并发连接
- ✅ 互斥锁保证线程安全，防止资源竞争
- ✅ 用户简单登录校验
- ✅ 消息广播，群聊功能
- ✅ MySQL存储用户信息、历史聊天记录
- ✅ CMake跨平台编译构建
- ✅ GDB可调试，处理异常断开

## 技术栈
- 语言：C++17
- 编译构建：CMake
- 网络：Linux Socket API、TCP
- 并发：C++ std::thread、std::mutex 互斥锁
- 数据库：MySQL
- 工具：Git、GDB

## 环境依赖
- Ubuntu 22.04 / Linux
- GCC 7+
- CMake >= 3.12
- MySQL 开发库 libmysqlclient-dev

## 编译运行
```bash
# 克隆项目
git clone 【https://github.com/wxhgn/8.14.git】
cd tcp

# 创建构建目录
mkdir build && cd build

# cmake生成构建文件
cmake ..

# 编译
make -j4

# 运行服务端
./serve

# 新开终端运行客户端
./client
