cmake_minimum_required(VERSION 3.17)
project(OsEx3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -pthread)

add_executable(OsEx3 Atomic/atomicdemo_multivar.cpp)
add_library(MapReduceClient MapReduceClient.h MapReduceClient.cpp MapReduceFramework MapReduceFramework.h  MapReduceFramework.cpp)