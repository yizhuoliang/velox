To compile with LDB instrumentation, use
```
make BUILD_DIR=release_minimal EXTRA_CMAKE_FLAGS="\
  -DCMAKE_C_FLAGS='-fno-omit-frame-pointer -g -fdebug-default-version=3' \
  -DCMAKE_CXX_FLAGS='-fno-omit-frame-pointer -g -fdebug-default-version=3' \
  -DCMAKE_C_COMPILER=/home/cc/llvm14-ldb/build/bin/clang \
  -DCMAKE_CXX_COMPILER=/home/cc/llvm14-ldb/build/bin/clang++ \
  -DCMAKE_VERBOSE_MAKEFILE=ON" -k
```

Note that need to adjust the paths in project root and velox/ CMakeLists. Search for user home dir "cc", the default user of chameleon.

Changed files include
CMakeLists.txt
velox/CMakeLists.txt

Added files are only in this directory.

Should call `make clean` then `ccache -C` for a brand new build!


---

For building original velox, note that only gcc/g++ 11 work! So use
```
make EXTRA_CMAKE_FLAGS="\
  -DCMAKE_C_COMPILER=/usr/bin/clang-15 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-15 \
  -DCMAKE_VERBOSE_MAKEFILE=ON"
```

# Experiment commands

Velox dummy scheduling policy for running threads with lower contention. The tripple projection is more membw aggressive than the sin projection.
```
taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82 numactl --membind=0 ./_build/release/velox/hrp_benchmarks/velox_new_sched 10000000 4 20
```

