add_executable(velox_avg_bench AvgBench.cpp)
target_link_libraries(
  velox_avg_bench
  velox_vector
  velox_core
  velox_type
  velox_memory
  velox_common_base
  velox_exception
  velox_serialization
  velox_time
  velox_buffer
  velox_file
  velox_common_compression
  velox_common_config
  velox_process
  velox_status
  velox_external_date
  velox_expression
  velox_functions_lib
  velox_caching
  velox_aggregates
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  fmt::fmt
  folly
  gflags::gflags
  glog::glog
  dl
  pthread
  ${ZLIB_LIBRARIES}
)
target_compile_options(velox_avg_bench PRIVATE -g -fdebug-default-version=3 -O3)