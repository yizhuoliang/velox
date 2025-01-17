#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <iostream>
#include <chrono>
#include <random>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

// Velox + test namespace usage.
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

//-----------------------------------------------------------------------------
// Global reference time
//-----------------------------------------------------------------------------
static std::chrono::time_point<std::chrono::high_resolution_clock> gStartTime;
static double msSinceStart() {
  auto now = std::chrono::high_resolution_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now - gStartTime).count();
  return static_cast<double>(diff);
}

//-----------------------------------------------------------------------------
// Barrier (no C++20)
//-----------------------------------------------------------------------------
struct SimpleBarrier {
  int totalCount;
  std::atomic<int> arrivedCount{0};
  std::mutex barrierMutex;
  std::condition_variable barrierCv;
  bool barrierOpen{false};

  explicit SimpleBarrier(int total) : totalCount(total) {}

  void wait() {
    std::unique_lock<std::mutex> lk(barrierMutex);
    int count = arrivedCount.fetch_add(1) + 1;
    if (count == totalCount) {
      barrierOpen = true;
      barrierCv.notify_all();
    } else {
      while (!barrierOpen) {
        barrierCv.wait(lk);
      }
    }
  }
};

struct ThreadResult {
  double startTimeMs{0.0};
  double endTimeMs{0.0};
};

//-----------------------------------------------------------------------------
// Data generation
//-----------------------------------------------------------------------------
RowVectorPtr generateBigIntData(
    int64_t numRows,
    int64_t numColumns,
    memory::MemoryPool* pool,
    int64_t seedOffset) {
  std::vector<VectorPtr> columns(numColumns);
  std::vector<std::string> columnNames(numColumns);

  std::mt19937_64 rng(42 + seedOffset);
  std::uniform_int_distribution<int64_t> dist(-10000, 10000);

  for (int64_t col = 0; col < numColumns; ++col) {
    columnNames[col] = "c" + std::to_string(col);
    auto vectorPtr = BaseVector::create(BIGINT(), numRows, pool);
    auto flat = vectorPtr->asFlatVector<int64_t>();
    auto rawValues = flat->mutableRawValues();
    for (int64_t row = 0; row < numRows; ++row) {
      rawValues[row] = dist(rng);
    }
    columns[col] = vectorPtr;
  }

  auto rowType = ROW(std::move(columnNames),
                     std::vector<TypePtr>(numColumns, BIGINT()));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr),
                                     numRows, columns);
}

RowVectorPtr generateDoubleData(
    int64_t numRows,
    int64_t numColumns,
    memory::MemoryPool* pool,
    int64_t seedOffset) {
  // same tweak: *2/6
  numRows = numRows * 2 / 6;

  std::vector<VectorPtr> columns(numColumns);
  std::vector<std::string> columnNames(numColumns);

  std::mt19937_64 rng(84 + seedOffset);
  std::uniform_real_distribution<double> dist(-10000.0, 10000.0);

  for (int64_t col = 0; col < numColumns; ++col) {
    columnNames[col] = "c" + std::to_string(col);
    auto vectorPtr = BaseVector::create(DOUBLE(), numRows, pool);
    auto flat = vectorPtr->asFlatVector<double>();
    auto rawValues = flat->mutableRawValues();
    for (int64_t row = 0; row < numRows; ++row) {
      rawValues[row] = dist(rng);
    }
    columns[col] = vectorPtr;
  }

  auto rowType = ROW(std::move(columnNames),
                     std::vector<TypePtr>(numColumns, DOUBLE()));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr),
                                     numRows, columns);
}

//-----------------------------------------------------------------------------
// Worker functions (triple, sin)
//   - optional barrier
//   - record startTime before Task::create()
//   - run task
//   - record endTime
//-----------------------------------------------------------------------------
void tripleWorker(
    RowVectorPtr data,
    int64_t numCols,
    ThreadResult* result,
    SimpleBarrier* barrierOrNull) {

  // Pre-work: build expressions
  std::vector<std::string> projections;
  projections.reserve(numCols);
  for (int64_t i = 0; i < numCols; ++i) {
    projections.push_back(
        "c" + std::to_string(3*i) + " * c" + std::to_string(3*i + 1) +
        " + c" + std::to_string(3*i + 2) + " as expr_" + std::to_string(i));
  }
  auto plan = PlanBuilder()
      .values({data})
      .project(projections)
      .planFragment();

  // Wait if needed
  if (barrierOrNull) {
    barrierOrNull->wait();
  }

  // start time
  result->startTimeMs = msSinceStart();

  // create & run Task
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  auto task = exec::Task::create(
      "triple_proj",
      std::move(plan),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }

  // end time
  result->endTimeMs = msSinceStart();
}

void sinWorker(
    RowVectorPtr data,
    int64_t numCols,
    ThreadResult* result,
    SimpleBarrier* barrierOrNull) {

  // Pre-work
  std::vector<std::string> projections;
  projections.reserve(numCols);
  for (int64_t i = 0; i < numCols; ++i) {
    projections.push_back(
        "sin(c" + std::to_string(i) + ") as sin_" + std::to_string(i));
  }
  auto plan = PlanBuilder()
      .values({data})
      .project(projections)
      .planFragment();

  if (barrierOrNull) {
    barrierOrNull->wait();
  }

  // start time
  result->startTimeMs = msSinceStart();

  // create & run Task
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  auto task = exec::Task::create(
      "sin_proj",
      std::move(plan),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }

  // end time
  result->endTimeMs = msSinceStart();
}

//-----------------------------------------------------------------------------
// MAIN
//-----------------------------------------------------------------------------
int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  memory::initializeMemoryManager({});

  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols> <userThreads>\n";
    return 1;
  }
  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);
  int64_t userThreads = std::stoll(argv[3]);
  if (numRows <= 0 || numCols <= 0 || userThreads <= 0) {
    std::cerr << "Rows, cols, threads must be positive.\n";
    return 1;
  }

  gStartTime = std::chrono::high_resolution_clock::now();

  // Velox registration
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  //----------------------------------------------------------------------
  // Round 1
  //----------------------------------------------------------------------
  std::cout << "\n==================== Round 1 ====================\n"
            << "[All triple start via barrier; each finishing triple spawns 1 sin]\n";

  {
    int64_t tripleCount1 = 2 * userThreads;
    int64_t sinCount1 = 2 * userThreads;

    // Generate data
    auto pool = memory::memoryManager()->addLeafPool();
    std::vector<RowVectorPtr> tripleData1(tripleCount1);
    std::vector<RowVectorPtr> sinData1(sinCount1);

    for (int i = 0; i < tripleCount1; ++i) {
      tripleData1[i] = generateBigIntData(numRows, 3 * numCols, pool.get(), 1000 + i);
    }
    for (int i = 0; i < sinCount1; ++i) {
      sinData1[i] = generateDoubleData(numRows, numCols, pool.get(), 2000 + i);
    }

    std::vector<ThreadResult> tripleResults1(tripleCount1);
    std::vector<ThreadResult> sinResults1(sinCount1);

    // We'll store the actual thread objects so we can join them.
    std::vector<std::thread> tripleThreads1;
    tripleThreads1.reserve(tripleCount1);

    std::mutex sinThreads1Mutex;
    std::vector<std::thread> sinThreads1;

    // Barrier for triple only
    SimpleBarrier tripleBarrier1(tripleCount1);

    std::atomic<int> tripleFinishedCount1{0};

    // Round 1 triple
    auto tripleWorker1 = [&](int idx) {
      tripleWorker(tripleData1[idx], numCols, &tripleResults1[idx], &tripleBarrier1);
      // spawn sin
      int finishedCount = tripleFinishedCount1.fetch_add(1);
      if (finishedCount < (int)sinCount1) {
        // sin index
        int sinIdx = finishedCount;
        std::thread st([&, sinIdx]() {
          sinWorker(sinData1[sinIdx], numCols, &sinResults1[sinIdx], nullptr);
        });
        // store
        {
          std::lock_guard<std::mutex> lk(sinThreads1Mutex);
          sinThreads1.push_back(std::move(st));
        }
      }
    };

    // Start all triple
    for (int i = 0; i < tripleCount1; ++i) {
      tripleThreads1.emplace_back(tripleWorker1, i);
    }

    // Join triple
    for (auto &t : tripleThreads1) {
      t.join();
    }

    // Join all sin
    {
      std::lock_guard<std::mutex> lk(sinThreads1Mutex);
      for (auto &t : sinThreads1) {
        t.join();
      }
    }

    // Print Round 1 results
    std::cout << "\n[Round1 summary]\nTRIPLE:\n";
    for (int i = 0; i < tripleCount1; ++i) {
      double dur = tripleResults1[i].endTimeMs - tripleResults1[i].startTimeMs;
      std::cout << "  triple[" << i << "] start=" << tripleResults1[i].startTimeMs
                << " end=" << tripleResults1[i].endTimeMs
                << " dur=" << dur << " ms\n";
    }
    std::cout << "\nSIN:\n";
    for (int i = 0; i < sinCount1; ++i) {
      double dur = sinResults1[i].endTimeMs - sinResults1[i].startTimeMs;
      std::cout << "  sin[" << i << "] start=" << sinResults1[i].startTimeMs
                << " end=" << sinResults1[i].endTimeMs
                << " dur=" << dur << " ms\n";
    }
  }

  //----------------------------------------------------------------------
  // Round 2
  //----------------------------------------------------------------------
  std::cout << "\n==================== Round 2 ====================\n"
            << "[Keep concurrency <= 2*userThreads. Start half triple+sin at barrier, "
               "then spawn tasks ASAP as soon as any thread finishes, favoring sin.]\n";

  {
    int64_t tripleCount2 = 2 * userThreads;
    int64_t sinCount2 = 2 * userThreads;

    auto pool = memory::memoryManager()->addLeafPool();
    std::vector<RowVectorPtr> tripleData2(tripleCount2);
    std::vector<RowVectorPtr> sinData2(sinCount2);

    for (int i = 0; i < tripleCount2; ++i) {
      tripleData2[i] = generateBigIntData(numRows, 3 * numCols, pool.get(), 3000 + i);
    }
    for (int i = 0; i < sinCount2; ++i) {
      sinData2[i] = generateDoubleData(numRows, numCols, pool.get(), 4000 + i);
    }

    std::vector<ThreadResult> tripleResults2(tripleCount2);
    std::vector<ThreadResult> sinResults2(sinCount2);

    // All round2 threads go here so we can join them
    std::mutex round2ThreadsMutex;
    std::vector<std::thread> round2Threads;

    // Track how many are done
    std::atomic<int> doneTriple2{0}, doneSin2{0};
    // next indexes to spawn
    std::atomic<int> nextTripleIdx{(int)userThreads}; // first batch: [0..userThreads-1]
    std::atomic<int> nextSinIdx{(int)userThreads};

    // concurrency limit
    int64_t maxConcurrency = 2 * userThreads;
    // how many are currently RUNNING
    std::atomic<int> activeThreads{0};

    // barrier for first half triple + sin
    SimpleBarrier round2Barrier((int)userThreads + (int)userThreads);

    // We'll define worker lambdas for triple & sin that:
    //  - do optional barrier
    //  - run
    //  - on finish, decrement activeThreads, then call spawnIfPossible() to fill up concurrency
    std::function<void(int,bool)> doTriple2;
    std::function<void(int,bool)> doSin2;

    // We'll define a function to spawn as many tasks as possible to keep concurrency saturated,
    // favoring sin first. Called after a worker finishes.
    std::mutex spawnMutex; // protect spawn logic

    auto spawnIfPossible = [&](bool fromThreadFinish) {
      // If multiple threads finish nearly at once, we want them all to call spawnIfPossible.
      // We lock to avoid overlapping spawns.
      std::lock_guard<std::mutex> lk(spawnMutex);

      while (true) {
        // how many are active?
        int currentActive = (int)activeThreads.load();
        if (currentActive >= (int)maxConcurrency) {
          // we're at concurrency limit
          break;
        }
        // if we can spawn more, do we have sin left?
        int s = nextSinIdx.load();
        if (s < (int)sinCount2) {
          // we attempt to claim sin s
          if (nextSinIdx.compare_exchange_strong(s, s + 1)) {
            // spawn sin s
            activeThreads.fetch_add(1);
            std::thread newT([&, s]() {
              doSin2(s, false);
            });
            {
              std::lock_guard<std::mutex> lk2(round2ThreadsMutex);
              round2Threads.push_back(std::move(newT));
            }
            continue; // see if we can spawn more in same loop
          }
        }
        // else try triple
        int t = nextTripleIdx.load();
        if (t < (int)tripleCount2) {
          if (nextTripleIdx.compare_exchange_strong(t, t + 1)) {
            activeThreads.fetch_add(1);
            std::thread newT([&, t]() {
              doTriple2(t, false);
            });
            {
              std::lock_guard<std::mutex> lk2(round2ThreadsMutex);
              round2Threads.push_back(std::move(newT));
            }
            continue;
          }
        }
        // no more sin or triple left
        break;
      }
    };

    // Implementation of triple2
    doTriple2 = [&](int idx, bool isFirstBatch) {
      // optional barrier
      SimpleBarrier* b = (isFirstBatch ? &round2Barrier : nullptr);
      tripleWorker(tripleData2[idx], numCols, &tripleResults2[idx], b);

      // done
      doneTriple2.fetch_add(1);
      activeThreads.fetch_sub(1);

      // attempt to spawn more tasks
      spawnIfPossible(/*fromThreadFinish=*/true);
    };

    // Implementation of sin2
    doSin2 = [&](int idx, bool isFirstBatch) {
      SimpleBarrier* b = (isFirstBatch ? &round2Barrier : nullptr);
      sinWorker(sinData2[idx], numCols, &sinResults2[idx], b);

      // done
      doneSin2.fetch_add(1);
      activeThreads.fetch_sub(1);

      // try spawn
      spawnIfPossible(/*fromThreadFinish=*/true);
    };

    // 1) Start initial half triple + half sin
    //   we do userThreads triple, userThreads sin => total 2*userThreads => saturate concurrency immediately.
    for (int i = 0; i < (int)userThreads; ++i) {
      activeThreads.fetch_add(1);
      round2Threads.emplace_back([&, i]() {
        doTriple2(i, true);
      });
    }
    for (int i = 0; i < (int)userThreads; ++i) {
      activeThreads.fetch_add(1);
      round2Threads.emplace_back([&, i]() {
        doSin2(i, true);
      });
    }

    // 2) Wait until all tasks are done
    //    We'll do a loop, then join all threads after that.
    while ((int)doneTriple2.load() < (int)tripleCount2 ||
           (int)doneSin2.load() < (int)sinCount2) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 3) Now join everything
    for (auto &th : round2Threads) {
      if (th.joinable()) {
        th.join();
      }
    }

    // Print Round 2 summary
    std::cout << "\n[Round2 summary]\nTRIPLE:\n";
    for (int i = 0; i < (int)tripleCount2; ++i) {
      double dur = tripleResults2[i].endTimeMs - tripleResults2[i].startTimeMs;
      std::cout << "  triple[" << i << "] start=" << tripleResults2[i].startTimeMs
                << " end=" << tripleResults2[i].endTimeMs
                << " dur=" << dur << " ms\n";
    }
    std::cout << "\nSIN:\n";
    for (int i = 0; i < (int)sinCount2; ++i) {
      double dur = sinResults2[i].endTimeMs - sinResults2[i].startTimeMs;
      std::cout << "  sin[" << i << "] start=" << sinResults2[i].startTimeMs
                << " end=" << sinResults2[i].endTimeMs
                << " dur=" << dur << " ms\n";
    }
  }

  std::cout << "\nAll done. Exiting main.\n";
  return 0;
}