cmake_minimum_required(VERSION 3.0)
set (CMAKE_C_COMPILER "/usr/bin/clang")
set (CMAKE_CXX_COMPILER "/usr/bin/clang++")

project(deltabench)
enable_language(CXX)
enable_language(C)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 11)

add_compile_options(-g)

set(GDELTA ${PROJECT_SOURCE_DIR}/gdelta/)
set(GDELTA_ORIGINAL ${PROJECT_SOURCE_DIR}/gdelta_original/)
set(XDELTA ${PROJECT_SOURCE_DIR}/xdelta/xdelta3)
set(EDELTA ${PROJECT_SOURCE_DIR}/edelta)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/edelta/util)

add_subdirectory(${GDELTA})
add_subdirectory(${GDELTA_ORIGINAL})
add_subdirectory(${EDELTA})
add_library(xdelta STATIC ${XDELTA}/xdelta3.c)

set(DELTA_LIBS xdelta gdelta edelta gdelta_original)

set(TEST_LIBS ${DELTA_LIBS} boost_filesystem boost_system pthread)

AUX_SOURCE_DIRECTORY(${PROJECT_SOURCE_DIR} src)
AUX_SOURCE_DIRECTORY(${PROJECT_SOURCE_DIR}/util util_src)
add_executable(deltabench ${src} ${util_src})
target_link_libraries(deltabench ${TEST_LIBS})
