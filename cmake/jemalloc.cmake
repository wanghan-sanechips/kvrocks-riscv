# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

if (NOT DISABLE_CACHE_OBLIVIOUS)
  set(DISABLE_CACHE_OBLIVIOUS "")
else()
  set(DISABLE_CACHE_OBLIVIOUS "--disable-cache-oblivious")
endif()

if (NOT DISABLE_JEMALLOC_PROFILING)
  set(ENABLE_JEMALLOC_PROFILING "--enable-prof")
else()
  set(ENABLE_JEMALLOC_PROFILING "")
endif()

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(jemalloc
  jemalloc/jemalloc 5.3.0  
  SHA1=1be8fdba021e9d6ed201e7d6a3c464b2223fc927
)

FetchContent_GetProperties(jemalloc)
if(NOT jemalloc_POPULATED)
  FetchContent_Populate(jemalloc)

  if(CMAKE_CROSSCOMPILING AND CMAKE_SYSTEM_PROCESSOR MATCHES "riscv64")
    message(STATUS "Configuring jemalloc for RISC-V cross-compilation")
    set(JEMALLOC_CROSS_FLAGS
      "--host=riscv64-unknown-linux-gnu"
      "--build=${CMAKE_HOST_SYSTEM_PROCESSOR}-pc-linux-gnu"
      "--with-lg-vaddr=48"
      )
  else()
    set(JEMALLOC_CROSS_FLAGS "")
  endif()

  execute_process(COMMAND autoconf
    WORKING_DIRECTORY ${jemalloc_SOURCE_DIR}
  )
  execute_process(COMMAND ${jemalloc_SOURCE_DIR}/configure CC=${CMAKE_C_COMPILER} -C ${JEMALLOC_CROSS_FLAGS} --enable-autogen
                    --disable-shared --disable-libdl ${DISABLE_CACHE_OBLIVIOUS} ${ENABLE_JEMALLOC_PROFILING}
                    --with-jemalloc-prefix=""
    WORKING_DIRECTORY ${jemalloc_BINARY_DIR}
  )
  add_custom_target(make_jemalloc 
    COMMAND ${MAKE_COMMAND} ${NINJA_MAKE_JOBS_FLAG}
    WORKING_DIRECTORY ${jemalloc_BINARY_DIR}
    BYPRODUCTS ${jemalloc_BINARY_DIR}/lib/libjemalloc.a
  )
endif()

find_package(Threads REQUIRED)

add_library(jemalloc INTERFACE)
target_include_directories(jemalloc INTERFACE $<BUILD_INTERFACE:${jemalloc_BINARY_DIR}/include>)
target_link_libraries(jemalloc INTERFACE $<BUILD_INTERFACE:${jemalloc_BINARY_DIR}/lib/libjemalloc.a> Threads::Threads)
target_compile_definitions(jemalloc INTERFACE ENABLE_JEMALLOC)
add_dependencies(jemalloc make_jemalloc)
