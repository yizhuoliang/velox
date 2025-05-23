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

add_executable(
  velox_wave_common_test
  GpuArenaTest.cpp
  CudaTest.cpp
  CudaTest.cu
  BreezeCudaTest.cu
  CompileTest.cu
  BlockTest.cpp
  BlockTest.cu
  HashTableTest.cpp
  HashTestUtil.cpp)

add_test(velox_wave_common_test velox_wave_common_test)
set_tests_properties(velox_wave_common_test PROPERTIES LABELS cuda_driver)

target_include_directories(velox_wave_common_test PRIVATE ../../../breeze)
target_link_libraries(
  velox_wave_common_test
  velox_wave_common
  GTest::gtest
  GTest::gtest_main
  CUDA::cudart)
