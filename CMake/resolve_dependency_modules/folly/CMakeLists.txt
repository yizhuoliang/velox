# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
project(Folly)
cmake_minimum_required(VERSION 3.14)

set(VELOX_FOLLY_BUILD_VERSION v2024.07.01.00)
set(VELOX_FOLLY_BUILD_SHA256_CHECKSUM
    e78584ab7ba9a687285f2849bc0141e2422a5c808ad6ab3558c83d85975e25ed)
set(VELOX_FOLLY_SOURCE_URL
    "https://github.com/facebook/folly/releases/download/${VELOX_FOLLY_BUILD_VERSION}/folly-${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(FOLLY)

message(STATUS "Building Folly from source")

if(gflags_SOURCE STREQUAL "BUNDLED")
  set(glog_patch && git apply ${CMAKE_CURRENT_LIST_DIR}/folly-gflags-glog.patch)
endif()

FetchContent_Declare(
  folly
  URL ${VELOX_FOLLY_SOURCE_URL}
  URL_HASH ${VELOX_FOLLY_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/folly-no-export.patch
                ${glog_patch})

# Suppress all warnings
set(FOLLY_CXX_FLAGS -w)
# Enable INT128 support
set(FOLLY_HAVE_INT128_T ON)
FetchContent_MakeAvailable(folly)

# Folly::folly is not valid for FC but we want to match FindFolly
add_library(Folly::folly ALIAS folly)

# The folly target does not contain any include directories, they are propagated
# from folly_base. This marks them as system headers which should suppress
# warnigs generated by them when they are included else where.
set_target_properties(
  folly_deps
  PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
             $<TARGET_PROPERTY:folly_deps,INTERFACE_INCLUDE_DIRECTORIES>)
set_target_properties(
  folly_base
  PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
             $<TARGET_PROPERTY:folly_base,INTERFACE_INCLUDE_DIRECTORIES>)

if(${gflags_SOURCE} STREQUAL "BUNDLED")
  add_dependencies(folly glog gflags_static fmt::fmt)
endif()

set(FOLLY_BENCHMARK_STATIC_LIB
    ${folly_BINARY_DIR}/folly/libfollybenchmark${CMAKE_STATIC_LIBRARY_SUFFIX}
    PARENT_SCOPE)
