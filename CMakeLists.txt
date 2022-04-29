cmake_minimum_required(VERSION 3.0)

project(deltabench)
enable_language(CXX)
enable_language(C)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 11)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(GDELTA ${PROJECT_SOURCE_DIR}/gdelta/)
set(GDELTA_ORIGINAL ${PROJECT_SOURCE_DIR}/gdelta_original/)
set(GDELTA_INIT ${PROJECT_SOURCE_DIR}/gdelta_init/)
set(XDELTA ${PROJECT_SOURCE_DIR}/xdelta/xdelta3)
set(EDELTA ${PROJECT_SOURCE_DIR}/edelta)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/edelta/util)

add_subdirectory(${GDELTA})
add_subdirectory(${GDELTA_ORIGINAL})
add_subdirectory(${GDELTA_INIT})
add_subdirectory(${EDELTA})
add_library(xdelta STATIC ${XDELTA}/xdelta3.c)

set(DELTA_LIBS xdelta gdelta edelta gdelta_original gdelta_init)

set(TEST_LIBS ${DELTA_LIBS} boost_filesystem boost_system pthread)

aux_source_directory(${PROJECT_SOURCE_DIR} src)
aux_source_directory(${PROJECT_SOURCE_DIR}/util util_src)
add_executable(deltabench ${src} ${util_src})
target_link_libraries(deltabench ${TEST_LIBS})
