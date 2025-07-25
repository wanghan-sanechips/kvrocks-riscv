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

FetchContent_DeclareGitHubWithMirror(zlib
  zlib-ng/zlib-ng 2.2.4
  MD5=9fbaac3919af8d5a0ad5726ef9c7c30b
)

FetchContent_MakeAvailableWithArgs(zlib
  WITH_GTEST=OFF
  ZLIB_ENABLE_TESTS=OFF
  ZLIBNG_ENABLE_TESTS=OFF
  BUILD_SHARED_LIBS=OFF
  ZLIB_COMPAT=ON
)
