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

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(tbb
  uxlfoundation/oneTBB v2021.13.0
  MD5=2dd9b7cfa5de5bb3add2f7392e0c9bab
)

FetchContent_MakeAvailableWithArgs(tbb
  TBB_STRICT=OFF
  TBB_TEST=OFF
  TBB_EXAMPLES=OFF
  TBBMALLOC_BUILD=OFF
  BUILD_SHARED_LIBS=OFF
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 12)
    target_compile_options(tbb PRIVATE "-Wno-error=stringop-overflow")
endif()
