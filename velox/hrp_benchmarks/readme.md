To compile with LDB instrumentation, use
```
make BUILD_DIR=release_minimal EXTRA_CMAKE_FLAGS="\
  -DCMAKE_C_FLAGS='-fno-omit-frame-pointer -g -fdebug-default-version=3' \
  -DCMAKE_CXX_FLAGS='-fno-omit-frame-pointer -g -fdebug-default-version=3' \
  -DCMAKE_C_COMPILER=/home/cc/llvm14-ldb/build/bin/clang \
  -DCMAKE_CXX_COMPILER=/home/cc/llvm14-ldb/build/bin/clang++ \
  -DCMAKE_VERBOSE_MAKEFILE=ON" -k
```s

Changed files include
CMakeLists.txt
velox/CMakeLists.txt

Added files are only in this directory.

Should call `make clean` then `ccache -C` for a brand new build!