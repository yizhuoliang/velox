To compile with LDB instrumentation, use
```
make BUILD_DIR=release_minimal EXTRA_CMAKE_FLAGS="\
  -DCMAKE_C_FLAGS='-fno-omit-frame-pointer -O3 -g' \
  -DCMAKE_CXX_FLAGS='-fno-omit-frame-pointer -O3 -g' \
  -DCMAKE_C_COMPILER=/home/cc/llvm14-ldb/build/bin/clang \
  -DCMAKE_CXX_COMPILER=/home/cc/llvm14-ldb/build/bin/clang++ \
  -DCMAKE_VERBOSE_MAKEFILE=ON" -k
```s