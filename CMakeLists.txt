# Copyright 2017 The TimberSaw Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.0)
# Keep the version below in sync with the one in db.h
project(TimberSaw VERSION 1.22.0 LANGUAGES C CXX)

# C cmpstandard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
  # This project can use C11, but will gracefully decay down to C89.
  set(CMAKE_C_STANDARD 11)
  set(CMAKE_C_STANDARD_REQUIRED OFF)
  set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++17.
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

if (WIN32)
  set(TimberSaw_PLATFORM_NAME TimberSaw_PLATFORM_WINDOWS)
  # TODO(cmumford): Make UNICODE configurable for Windows.
  add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
  set(TimberSaw_PLATFORM_NAME TimberSaw_PLATFORM_POSIX)
endif (WIN32)

option(TimberSaw_BUILD_TESTS "Build TimberSaw's unit tests" ON)
option(TimberSaw_BUILD_BENCHMARKS "Build TimberSaw's benchmarks" ON)
option(TimberSaw_INSTALL "Install TimberSaw's header and library" ON)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
message(STATUS "Have snappy ? ${HAVE_SNAPPY}")

#check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)
# disable tcmalloc when we want to enable the fsantinize=address

string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)
if(uppercase_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
  SET(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
endif()

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Disable C++ exceptions.
  string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
  add_definitions(-D_HAS_EXCEPTIONS=0)

  # Disable RTTI.
  string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Enable strict prototype warnings for C code in clang and gcc.
  if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
  endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

  # Disable C++ exceptions.
  string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

  # Disable RTTI.
  string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_SUPPORT_THREAD_LOCAL -Wall -Wextra") #set ggdb  -fsanitize=address flag here.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lnuma") # for core number statistic

message(STATUS "compling mode ${CMAKE_BUILD_TYPE}")


# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
                        TimberSaw_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(TimberSaw_PUBLIC_INCLUDE_DIR "include/TimberSaw")
set(TimberSaw_PORT_CONFIG_DIR "include/port")

configure_file(
  "port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${TimberSaw_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "${PROJECT_BINARY_DIR}/db"
  "${PROJECT_BINARY_DIR}/port"
  "${PROJECT_BINARY_DIR}/util"
  "."
)

if(BUILD_SHARED_LIBS)
  # Only export TimberSaw_EXPORT symbols from the shared library.
  add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(TimberSaw)
target_sources(TimberSaw
  PRIVATE
    "${PROJECT_BINARY_DIR}/${TimberSaw_PORT_CONFIG_DIR}/port_config.h"
    "db/builder.cc"
    "db/builder.h"
    "db/c.cc"
    "db/db_impl.cc"
    "db/db_impl.h"
    "db/db_impl_sharding.cpp"
    "db/db_impl_sharding.h"
    "db/db_iter.cc"
    "db/db_iter.h"
    "db/dbformat.cc"
    "db/dbformat.h"
    "db/dumpfile.cc"
    "db/filename.cc"
    "db/filename.h"
    "db/inlineskiplist.h"
    "db/log_format.h"
    "db/log_reader.cc"
    "db/log_reader.h"
    "db/log_writer.cc"
    "db/log_writer.h"
    "db/memtable.cc"
    "db/memtable.h"
    "db/memtable_list.cc"
    "db/memtable_list.h"
    "db/repair.cc"
    "db/skiplist.h"
    "db/snapshot.h"
    "db/table_cache.cc"
    "db/table_cache.h"
    "db/version_edit.cc"
    "db/version_edit.h"
    "db/version_set.cc"
    "db/version_set.h"
    "db/write_batch_internal.h"
    "db/write_batch.cc"
    "util/ThreadPool.cpp"
    "util/ThreadPool.h"
    "util/allocator.h"
    "port/port_posix.h"
    "port/port_posix.cc"
    "port/port.h"
    "port/thread_annotations.h"
    "memory_node/memory_node_keeper.h"
    "memory_node/memory_node_keeper.cpp"
    "table/table_builder.cpp"
    "table/table_builder_memoryside.h"
    "table/table_builder_memoryside.cpp"
    "table/table_memoryside.h"
    "table/table_memoryside.cpp"
    "table/block_builder.cc"
    "table/block_builder.h"
    "table/block.cc"
    "table/block.h"
    "table/filter_block.cc"
    "table/filter_block.h"
    "table/full_filter_block.cc"
    "table/full_filter_block.h"
    "table/format.cc"
    "table/format.h"
    "table/iterator_wrapper.h"
    "table/iterator.cc"
    "table/merger.cc"
    "table/merger.h"
    "table/table_builder_computeside.h"
    "table/table_builder_computeside.cc"
    "table/table_builder_bacs.cpp"
    "table/table_builder_bacs.h"
    "table/table_builder_bams.cpp"
    "table/table_builder_bams.h"
    "table/table.cc"
    "include/TimberSaw/table_builder.h"
    "table/two_level_iterator.cc"
    "table/two_level_iterator.h"
    "table/byte_addressable_RA_iterator.h"
    "table/byte_addressable_RA_iterator.cpp"
    "table/byte_addressable_SEQ_iterrator.cpp"
    "table/byte_addressable_SEQ_iterrator.h"
    "util/arena.cc"
    "util/arena.h"
    "util/autovector.h"
    "util/concurrent_arena.cc"
    "util/concurrent_arena.h"
    "util/core_local.h"
    "util/bloom.cc"
    "util/bloom_impl.h"
    "util/cache.cc"
#    "util/clock.cc"
    "util/coding.cc"
    "util/coding.h"
    "util/comparator.cc"
    "util/crc32c.cc"
    "util/crc32c.h"
    "util/env_posix.h"
    "util/env.cc"
    "util/fastrange.h"
    "util/filter_policy.cc"
    "util/hash.cc"
    "util/hash.h"
    "util/logging.cc"
    "util/logging.h"
    "util/mutexlock.h"
    "util/no_destructor.h"
    "util/options.cc"
    "util/random.cc"
    "util/random.h"
    "util/rdma.cc"
    "util/rdma.h"
    "util/Resource_Printer_Plan.h"
    "util/Resource_Printer_Plan.cpp"
    "util/RPC_Process.cpp"
    "util/RPC_Process.h"
    "util/thread_local.cc"
    "util/thread_local.h"
    "util/status.cc"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/c.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/cache.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/comparator.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/db.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/dumpfile.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/env.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/export.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/filter_policy.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/iterator.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/options.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/slice.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/status.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/table_builder.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/table.h"
    "${TimberSaw_PUBLIC_INCLUDE_DIR}/write_batch.h"
)

#if (WIN32)
#  target_sources(TimberSaw
#    PRIVATE
#      "util/env_windows.cc"
#      "util/windows_logger.h"
#  )
#else (WIN32)
  target_sources(TimberSaw
    PRIVATE
      "util/env_posix.cc"
      "util/posix_logger.h"
  )
#endif (WIN32)

# MemEnv is not part of the interface and could be pulled to a separate library.
target_sources(TimberSaw
  PRIVATE
    "helpers/memenv/memenv.cc"
    "helpers/memenv/memenv.h"
)

target_include_directories(TimberSaw
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(TimberSaw
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(TimberSaw
  PRIVATE
    # Used by include/export.h when building shared libraries.
    TimberSaw_COMPILE_LIBRARY
    # Used by port/port.h.
    ${TimberSaw_PLATFORM_NAME}=1
)
if (NOT HAVE_CXX17_HAS_INCLUDE)
  target_compile_definitions(TimberSaw
    PRIVATE
      TimberSaw_HAS_PORT_CONFIG_H=1
  )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(TimberSaw
    PUBLIC
      # Used by include/export.h.
      TimberSaw_SHARED_LIBRARY
  )
endif(BUILD_SHARED_LIBS)

if(HAVE_CLANG_THREAD_SAFETY)
  target_compile_options(TimberSaw
    PUBLIC
      -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

if(HAVE_CRC32C)
  target_link_libraries(TimberSaw crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(TimberSaw snappy)
endif(HAVE_SNAPPY)
#if(HAVE_TCMALLOC)
#  target_link_libraries(TimberSaw tcmalloc)
#endif(HAVE_TCMALLOC)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(TimberSaw Threads::Threads)
find_package(ibverbs REQUIRED)
target_link_libraries(TimberSaw ibverbs::ibverbs)
#find_package(Boost REQUIRED)
#INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
#target_link_libraries(TimberSaw Boost::boost)
#if(WITH_NUMA)
find_package(NUMA REQUIRED)
add_definitions(-DNUMA)
include_directories(${NUMA_INCLUDE_DIR})
target_link_libraries(TimberSaw NUMA::NUMA)
#endif()
#add_executable(TimberSawutil
#  "db/TimberSawutil.cc"
#)
#target_link_libraries(TimberSawutil TimberSaw)
add_executable(Mem_leak_checker memory_leak.cpp)
target_link_libraries(Mem_leak_checker TimberSaw)
add_executable(Server server.cc)
target_link_libraries(Server TimberSaw)
if(TimberSaw_BUILD_TESTS)
  enable_testing()

  # Prevent overriding the parent project's compiler/linker settings on Windows.
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(install_gtest OFF)
  set(install_gmock OFF)
  set(build_gmock ON)

  # This project is tested using GoogleTest.
  add_subdirectory("third_party/googletest")

  # This project uses Google benchmark for benchmarking.
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "" FORCE)
  add_subdirectory("third_party/benchmark")

  # GoogleTest triggers a missing field initializers warning.
  if(TimberSaw_HAVE_NO_MISSING_FIELD_INITIALIZERS)
    set_property(TARGET gtest
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
    set_property(TARGET gmock
        APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
  endif(TimberSaw_HAVE_NO_MISSING_FIELD_INITIALIZERS)

  function(TimberSaw_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)

    add_executable("${test_target_name}" "")
    target_sources("${test_target_name}"
      PRIVATE
        "${PROJECT_BINARY_DIR}/${TimberSaw_PORT_CONFIG_DIR}/port_config.h"
        "util/testutil.cc"
        "util/testutil.h"

        "${test_file}"
    )
    target_link_libraries("${test_target_name}" TimberSaw gmock gtest benchmark)
    target_compile_definitions("${test_target_name}"
      PRIVATE
        ${TimberSaw_PLATFORM_NAME}=1
    )
    if (NOT HAVE_CXX17_HAS_INCLUDE)
      target_compile_definitions("${test_target_name}"
        PRIVATE
          TimberSaw_HAS_PORT_CONFIG_H=1
      )
    endif(NOT HAVE_CXX17_HAS_INCLUDE)

    add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
  endfunction(TimberSaw_test)

#  TimberSaw_test("db/c_test.c")
#  TimberSaw_test("db/fault_injection_test.cc")
#
#  TimberSaw_test("issues/issue178_test.cc")
#  TimberSaw_test("issues/issue200_test.cc")
#  TimberSaw_test("issues/issue320_test.cc")
#
#  TimberSaw_test("util/env_test.cc")
#  TimberSaw_test("util/status_test.cc")
#  TimberSaw_test("util/no_destructor_test.cc")
#
#  if(NOT BUILD_SHARED_LIBS)
#    TimberSaw_test("db/autocompact_test.cc")
#    TimberSaw_test("db/corruption_test.cc")
#    TimberSaw_test("db/db_test.cc")
#    TimberSaw_test("db/dbformat_test.cc")
#    TimberSaw_test("db/filename_test.cc")
#    TimberSaw_test("db/log_test.cc")
#    TimberSaw_test("db/recovery_test.cc")
#    TimberSaw_test("db/skiplist_test.cc")
#    TimberSaw_test("db/version_edit_test.cc")
#    TimberSaw_test("db/version_set_test.cc")
#    TimberSaw_test("db/write_batch_test.cc")
#
#    TimberSaw_test("helpers/memenv/memenv_test.cc")
#
#    TimberSaw_test("table/filter_block_test.cc")
#    TimberSaw_test("table/table_test.cc")
#
#    TimberSaw_test("util/arena_test.cc")
#    TimberSaw_test("util/bloom_test.cc")
#    TimberSaw_test("util/cache_test.cc")
#    TimberSaw_test("util/coding_test.cc")
#    TimberSaw_test("util/crc32c_test.cc")
#    TimberSaw_test("util/hash_test.cc")
#    TimberSaw_test("util/logging_test.cc")

    # TODO(costan): This test also uses
    #               "util/env_{posix|windows}_test_helper.h"
#    if (WIN32)
#      TimberSaw_test("util/env_windows_test.cc")
#    else (WIN32)
#      TimberSaw_test("util/env_posix_test.cc")
#    endif (WIN32)
#  endif(NOT BUILD_SHARED_LIBS)
endif(TimberSaw_BUILD_TESTS)

if(TimberSaw_BUILD_BENCHMARKS)
  function(TimberSaw_benchmark bench_file)
    get_filename_component(bench_target_name "${bench_file}" NAME_WE)

    add_executable("${bench_target_name}" "")
    target_sources("${bench_target_name}"
      PRIVATE
        "${PROJECT_BINARY_DIR}/${TimberSaw_PORT_CONFIG_DIR}/port_config.h"
        "util/histogram.cc"
        "util/histogram.h"
        "util/testutil.cc"
        "util/testutil.h"

        "${bench_file}"
    )
    target_link_libraries("${bench_target_name}" TimberSaw gmock gtest)
    target_compile_definitions("${bench_target_name}"
      PRIVATE
        ${TimberSaw_PLATFORM_NAME}=1
    )
    if (NOT HAVE_CXX17_HAS_INCLUDE)
      target_compile_definitions("${bench_target_name}"
        PRIVATE
          TimberSaw_HAS_PORT_CONFIG_H=1
      )
    endif(NOT HAVE_CXX17_HAS_INCLUDE)
  endfunction(TimberSaw_benchmark)

  if(NOT BUILD_SHARED_LIBS)
    TimberSaw_benchmark("benchmarks/db_bench.cc")
  endif(NOT BUILD_SHARED_LIBS)

  check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
  if(HAVE_SQLITE3)
    TimberSaw_benchmark("benchmarks/db_bench_sqlite3.cc")
    target_link_libraries(db_bench_sqlite3 sqlite3)
  endif(HAVE_SQLITE3)

  # check_library_exists is insufficient here because the library names have
  # different manglings when compiled with clang or gcc, at least when installed
  # with Homebrew on Mac.
  set(OLD_CMAKE_REQURED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
  list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
  check_cxx_source_compiles("
#include <kcpolydb.h>

int main() {
  kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
  delete db;
  return 0;
}
  "  HAVE_KYOTOCABINET)
  set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQURED_LIBRARIES})
  if(HAVE_KYOTOCABINET)
    TimberSaw_benchmark("benchmarks/db_bench_tree_db.cc")
    target_link_libraries(db_bench_tree_db kyotocabinet)
  endif(HAVE_KYOTOCABINET)
endif(TimberSaw_BUILD_BENCHMARKS)

if(TimberSaw_INSTALL)
  install(TARGETS TimberSaw
    EXPORT TimberSawTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
  install(
    FILES
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/c.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/cache.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/comparator.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/db.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/dumpfile.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/env.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/export.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/filter_policy.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/iterator.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/options.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/slice.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/status.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/table_builder.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/table.h"
      "${TimberSaw_PUBLIC_INCLUDE_DIR}/write_batch.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/TimberSaw"
  )

  include(CMakePackageConfigHelpers)
  configure_package_config_file(
    "cmake/${PROJECT_NAME}Config.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
  )
  install(
    EXPORT TimberSawTargets
    NAMESPACE TimberSaw::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  install(
    FILES
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
endif(TimberSaw_INSTALL)
