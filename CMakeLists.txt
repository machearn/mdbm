cmake_minimum_required(VERSION 3.14)

PROJECT(MDBM)

set(CMAKE_C_STANDARD 11)

add_library(mdbm btree.c mdbm.c lock.c data.c)

add_executable(main main.c)
target_link_libraries(main mdbm)