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
#include <limits>
#include <atomic>

// We comment out hrperf usage.
/*
extern "C" {
  #include "hrperf_api.h"
}
*/

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

// Global reference point (previously was set inside hrperf_start).
static std::chrono::time_point<std::chrono::high_resolution_clock> gStartTime;

// Returns milliseconds since gStartTime.
static double msSinceStart() {
  auto now = std::chrono::high_resolution_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - gStartTime).count();
  return static_cast<double>(diff);
}

struct ThreadResult {
  double startTimeMs{0.0};
  double endTimeMs{0.0};
};

/////////////////////////////////////////////////////////////////
// Data generation for three-col worker: BIGINT columns
/////////////////////////////////////////////////////////////////
RowVectorPtr generateBigIntData(
    int64_t numRows,
    int64_t numColumns,
    memory::MemoryPool* pool,
    int64_t seedOffset) {
  std::vector<VectorPtr> columns(numColumns);
  std::vector<std::string> columnNames(numColumns);

  // Limit the range to avoid 64-bit overflow in multiplication a*b.
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

  auto rowType = ROW(std::move(columnNames), std::vector<TypePtr>(numColumns, BIGINT()));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), numRows, columns);
}

/////////////////////////////////////////////////////////////////
// Data generation for sin worker: DOUBLE columns
/////////////////////////////////////////////////////////////////
RowVectorPtr generateDoubleData(
    int64_t numRows,
    int64_t numColumns,
    memory::MemoryPool* pool,
    int64_t seedOffset) {
  std::vector<VectorPtr> columns(numColumns);
  std::vector<std::string> columnNames(numColumns);

  // Let's also keep a limited range, but they are double values:
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

  auto rowType = ROW(std::move(columnNames), std::vector<TypePtr>(numColumns, DOUBLE()));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), numRows, columns);
}

/////////////////////////////////////////////////////////////////
// Worker threads
/////////////////////////////////////////////////////////////////

/**
 * Three-col projection:
 *   - Input data has 3*numCols columns: c0, c1, c2, c3, c4, c5, ...
 *   - We produce 'numCols' output columns, each is c(3i) * c(3i+1) + c(3i+2).
 *   - All columns are BIGINT in this data.
 */
void threeColProjectionThread(
    RowVectorPtr data,    // Data with 3 * numCols columns, all BIGINT.
    int64_t numCols,      // The user's "number of columns" (not 3 * numCols).
    ThreadResult* result) {

  // Record "start" time (in ms).
  result->startTimeMs = msSinceStart();

  // Create memory pool & context.
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  // For i in [0..(numCols-1)] => c(3i)*c(3i+1) + c(3i+2).
  std::vector<std::string> projections;
  projections.reserve(numCols);
  for (int64_t i = 0; i < numCols; ++i) {
    projections.push_back(
        "c" + std::to_string(3 * i) + " * c" + std::to_string(3 * i + 1) +
        " + c" + std::to_string(3 * i + 2) + " as expr_" + std::to_string(i));
  }

  auto plan = PlanBuilder()
      .values({data}, true)
      .project(projections)
      .planFragment();

  auto task = exec::Task::create(
      "three_col_proj_task",
      std::move(plan),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res; 
  }

  // Record "end" time (in ms).
  result->endTimeMs = msSinceStart();

  if (resultVector) {
    std::cout << "[3-Col Projection] Thread " << std::this_thread::get_id()
              << " first row:\n"
              << resultVector->toString(0, std::min<int64_t>(1, resultVector->size()))
              << std::endl;
  }
}

/**
 * Sin projection:
 *   - Input data has numCols columns, all DOUBLE type: c0, c1, ...
 *   - For each column c(i), we compute sin(c(i)).
 */
void sinProjectionThread(
    RowVectorPtr data,   // Data with numCols columns, all DOUBLE.
    int64_t numCols,
    ThreadResult* result) {

  // Record "start" time (in ms).
  result->startTimeMs = msSinceStart();

  // Create memory pool & context.
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  // Build expressions: sin(c0), sin(c1), ...
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

  auto task = exec::Task::create(
      "sin_proj_task",
      std::move(plan),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }

  // Record "end" time (in ms).
  result->endTimeMs = msSinceStart();

  if (resultVector) {
    std::cout << "[Sin Projection] Thread " << std::this_thread::get_id()
              << " first row:\n"
              << resultVector->toString(0, std::min<int64_t>(1, resultVector->size()))
              << std::endl;
  }
}

/////////////////////////////////////////////////////////////////
// Main
/////////////////////////////////////////////////////////////////
int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  memory::initializeMemoryManager({});

  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols> <numThreads>\n";
    std::cerr << "  numThreads is how many (a*b+c) or sin(a) workers in total.\n";
    return 1;
  }

  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);
  int64_t userThreads = std::stoll(argv[3]);

  if (numRows <= 0 || numCols <= 0 || userThreads <= 0) {
    std::cerr << "Rows, cols, threads must be positive.\n";
    return 1;
  }

  // // Commenting out hrperf code:
  // hrperf_start();
  gStartTime = std::chrono::high_resolution_clock::now();

  // Register Velox functions.
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  // Print data size info (BIGINT perspective for 3-col, but let's just do a rough one).
  double perValue = sizeof(int64_t);
  double colBytes = numRows * perValue;
  double totalBytes = colBytes * numCols;
  double colGB = colBytes / (1024 * 1024 * 1024.0);
  double totalGB = totalBytes / (1024 * 1024 * 1024.0);
  std::cout << "Rows=" << numRows << ", Cols=" << numCols
            << ", UserThreads=" << userThreads << "\n";
  std::cout << "Data size/column (as INT64): " << colGB << " GB\n"
            << "Data size total (for sin worker if it were INT64) " << totalGB << " GB\n"
            << "Data size total (for 3-col worker) = 3x => " << (3.0 * totalGB)
            << " GB\n\n";

  /////////////////////////////////////////////////////////////////////////
  // Round 1:
  /////////////////////////////////////////////////////////////////////////
  std::cout << "==================== Round 1 ====================" << std::endl;

  // For demonstration, we do 2 "a*b+c" threads and 2 "sin" threads,
  // ignoring userThreads for simplicity. 
  // If you want exactly userThreads in practice, you can adapt similarly.

  int64_t threeColWorkers = 2;
  int64_t sinWorkers = 2;

  // 1) Generate BIGINT data for the "a*b+c" workers (3 * numCols columns).
  std::vector<RowVectorPtr> threeColData(threeColWorkers);
  {
    auto pool = memory::memoryManager()->addLeafPool();
    for (int64_t i = 0; i < threeColWorkers; ++i) {
      threeColData[i] = generateBigIntData(numRows, 3 * numCols, pool.get(), 1000 + i);
    }
  }

  // 2) Generate DOUBLE data for the "sin(a)" workers (numCols columns).
  std::vector<RowVectorPtr> sinData(sinWorkers);
  {
    auto pool = memory::memoryManager()->addLeafPool();
    for (int64_t i = 0; i < sinWorkers; ++i) {
      sinData[i] = generateDoubleData(numRows, numCols, pool.get(), 2000 + i);
    }
  }

  // Now all data is generated for Round 1.
  // We do the "capacity=2" scheduling approach:
  //   - Start two "threeColProjection" threads in parallel.
  //   - As soon as the FIRST of them finishes, we start the FIRST sin-projection.
  //   - As soon as the SECOND finishes, we start the SECOND sin-projection.

  std::vector<ThreadResult> threeColResults(threeColWorkers);
  std::vector<ThreadResult> sinResults(sinWorkers);

  std::atomic<int> threeColFinished{0};

  // The worker function for "threeCol" that, on completion, triggers sin if needed.
  auto threeColWorker = [&](int idx) {
    threeColProjectionThread(threeColData[idx], numCols, &threeColResults[idx]);
    int finishedCount = ++threeColFinished;
    if (finishedCount <= sinWorkers) {
      int sinIdx = finishedCount - 1;
      sinProjectionThread(sinData[sinIdx], numCols, &sinResults[sinIdx]);
    }
  };

  std::vector<std::thread> round1Threads;
  round1Threads.reserve(threeColWorkers);
  for (int i = 0; i < threeColWorkers; ++i) {
    round1Threads.emplace_back(threeColWorker, i);
  }
  for (auto& t : round1Threads) {
    t.join();
  }

  /////////////////////////////////////////////////////////////////////////
  // Round 2:
  /////////////////////////////////////////////////////////////////////////
  std::cout << "\n==================== Round 2 ====================" << std::endl;

  threeColFinished.store(0);

  // Again, 2 "threeCol" and 2 "sin".
  std::vector<RowVectorPtr> threeColData2(threeColWorkers);
  std::vector<RowVectorPtr> sinData2(sinWorkers);

  {
    auto pool = memory::memoryManager()->addLeafPool();
    for (int64_t i = 0; i < threeColWorkers; ++i) {
      threeColData2[i] = generateBigIntData(numRows, 3 * numCols, pool.get(), 3000 + i);
    }
    for (int64_t i = 0; i < sinWorkers; ++i) {
      sinData2[i] = generateDoubleData(numRows, numCols, pool.get(), 4000 + i);
    }
  }

  std::vector<ThreadResult> threeColResults2(threeColWorkers);
  std::vector<ThreadResult> sinResults2(sinWorkers);

  auto threeColWorker2 = [&](int idx) {
    threeColProjectionThread(threeColData2[idx], numCols, &threeColResults2[idx]);
  };
  auto sinWorker2 = [&](int idx) {
    sinProjectionThread(sinData2[idx], numCols, &sinResults2[idx]);
  };

  // Step 1: run (threeColWorker2(0), sinWorker2(0)) in parallel
  {
    std::thread t1(threeColWorker2, 0);
    std::thread t2(sinWorker2, 0);
    t1.join();
    t2.join();
  }

  // Step 2: run (threeColWorker2(1), sinWorker2(1)) in parallel
  {
    std::thread t1(threeColWorker2, 1);
    std::thread t2(sinWorker2, 1);
    t1.join();
    t2.join();
  }

  /////////////////////////////////////////////////////////////////////////
  // Print summary:
  /////////////////////////////////////////////////////////////////////////
  std::cout << "\n========== Round 1 Results ==========\n";
  for (int i = 0; i < threeColWorkers; ++i) {
    std::cout << "[Round1 A*B+C " << i << "] Start: " << threeColResults[i].startTimeMs
              << " ms, End: " << threeColResults[i].endTimeMs
              << " ms, Duration: "
              << (threeColResults[i].endTimeMs - threeColResults[i].startTimeMs)
              << " ms\n";
  }
  for (int i = 0; i < sinWorkers; ++i) {
    std::cout << "[Round1 Sin " << i << "]    Start: " << sinResults[i].startTimeMs
              << " ms, End: " << sinResults[i].endTimeMs
              << " ms, Duration: "
              << (sinResults[i].endTimeMs - sinResults[i].startTimeMs)
              << " ms\n";
  }

  std::cout << "\n========== Round 2 Results ==========\n";
  for (int i = 0; i < threeColWorkers; ++i) {
    std::cout << "[Round2 A*B+C " << i << "] Start: " << threeColResults2[i].startTimeMs
              << " ms, End: " << threeColResults2[i].endTimeMs
              << " ms, Duration: "
              << (threeColResults2[i].endTimeMs - threeColResults2[i].startTimeMs)
              << " ms\n";
  }
  for (int i = 0; i < sinWorkers; ++i) {
    std::cout << "[Round2 Sin " << i << "]    Start: " << sinResults2[i].startTimeMs
              << " ms, End: " << sinResults2[i].endTimeMs
              << " ms, Duration: "
              << (sinResults2[i].endTimeMs - sinResults2[i].startTimeMs)
              << " ms\n";
  }

  // hrperf_pause();  // commented out
  return 0;
}
