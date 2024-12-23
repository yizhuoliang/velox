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

extern "C" {
  #include "hrperf_api.h"
}

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class Barrier {
public:
  Barrier(int num_threads) : num_threads_(num_threads), count_(0), generation_(0) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    int gen = generation_;

    if (++count_ == num_threads_) {
      generation_++;
      count_ = 0;
      cond_.notify_all();
    } else {
      cond_.wait(lock, [this, gen]() { return gen != generation_; });
    }
  }

private:
  std::mutex mutex_;
  std::condition_variable cond_;
  int num_threads_;
  int count_;
  int generation_;
};

struct ThreadResult {
  double executionTime;
};

std::mutex printMutex;

void workerThread(
    int64_t numRows,
    int64_t numCols,
    ThreadResult* result,
    Barrier& barrier,
    int64_t threadIndex) {
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // Initialize Executor and Execution Context.
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  // Generate data.
  std::vector<VectorPtr> columns;
  std::vector<std::string> columnNames;
  columns.reserve(numCols);
  columnNames.reserve(numCols);

  std::mt19937_64 rng(42 + threadIndex);  // Seed the RNG with a fixed seed plus thread index
  std::uniform_int_distribution<int64_t> dist;

  for (int64_t col = 0; col < numCols; ++col) {
    std::string colName = "c" + std::to_string(col);
    columnNames.push_back(colName);

    auto vectorPtr = BaseVector::create(BIGINT(), numRows, pool.get());
    auto flatVector = vectorPtr->asFlatVector<int64_t>();
    auto rawValues = flatVector->mutableRawValues();

    for (int64_t row = 0; row < numRows; ++row) {
      rawValues[row] = dist(rng);
    }

    columns.push_back(vectorPtr);
  }

  auto rowType = ROW(std::move(columnNames), std::vector<TypePtr>(numCols, BIGINT()));

  auto data = std::make_shared<RowVector>(
      pool.get(), rowType, BufferPtr(nullptr), numRows, std::move(columns));

  // Build the query plan to compute average of each column.
  std::vector<std::string> aggregates;
  aggregates.reserve(numCols);
  for (int64_t col = 0; col < numCols; ++col) {
    std::string agg = "avg(c" + std::to_string(col) + ") as avg_c" + std::to_string(col);
    aggregates.push_back(agg);
  }

  auto planFragment = PlanBuilder()
                          .values({data})
                          .singleAggregation({}, aggregates)
                          .planFragment();

  // Create the task.
  auto task = exec::Task::create(
      "task_id",
      std::move(planFragment),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);  // Use kSerial as specified

  // Synchronize before execution.
  barrier.Wait();

  // Measure execution time.
  auto start = std::chrono::high_resolution_clock::now();

  // Execute the task and collect the result.
  RowVectorPtr resultVector;

  while (auto res = task->next()) {
    resultVector = res;  // There should be only one result row.
  }

  auto end = std::chrono::high_resolution_clock::now();

  // Compute execution time.
  std::chrono::duration<double> diff = end - start;
  result->executionTime = diff.count();

  // Optionally, print per-thread results.
  {
    std::lock_guard<std::mutex> lock(printMutex);
    if (resultVector) {
      std::cout << "Thread " << std::this_thread::get_id()
                << " average of each column:" << std::endl;
      std::cout << resultVector->toString(0, resultVector->size()) << std::endl;
    } else {
      std::cout << "Thread " << std::this_thread::get_id() << " no results." << std::endl;
    }
  }
    volatile int a = 0;
    for (int i = 0; i < 10000000; i++) {
        i + a;
        a * i;
        a + a;
    }
    std::cout << a << std::endl;
}

int main(int argc, char** argv) {
  // Initialize Folly.
  folly::Init init{&argc, &argv, false};

  // Initialize the process-wide memory-manager with the default options.
  memory::initializeMemoryManager({});

  // Parse command-line arguments.
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols> <numThreads>" << std::endl;
    return 1;
  }

  hrperf_start();

  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);
  int64_t numThreads = std::stoll(argv[3]);

  if (numRows <= 0 || numCols <= 0 || numThreads <= 0) {
    std::cerr << "Number of rows, columns, and threads must be positive integers." << std::endl;
    return 1;
  }

  std::cout << "Generating data with " << numRows << " rows, " << numCols
            << " columns, across " << numThreads << " threads." << std::endl;

  // Register Velox functions.
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  // Compute data sizes.
  const int64_t bytesPerValue = sizeof(int64_t);
  double columnBytes = numRows * bytesPerValue;
  double totalBytes = columnBytes * numCols;
  double columnGB = columnBytes / (1024 * 1024 * 1024.0);
  double totalGB = totalBytes / (1024 * 1024 * 1024.0);

  std::cout << "Data size per column: " << columnGB << " GB" << std::endl;
  std::cout << "Total data size per thread: " << totalGB << " GB" << std::endl;
  std::cout << "Total data size across all threads: " << totalGB * numThreads << " GB" << std::endl;

  // Create a barrier for thread synchronization.
  Barrier barrier(numThreads);

  // Create a vector to store execution times.
  std::vector<ThreadResult> threadResults(numThreads);

  // Create and start threads.
  std::vector<std::thread> threads;
  threads.reserve(numThreads);

  for (int64_t i = 0; i < numThreads; ++i) {
    threads.emplace_back(workerThread, numRows, numCols, &threadResults[i], std::ref(barrier), i);
  }

  // Wait for threads to finish.
  for (auto& t : threads) {
    t.join();
  }

  // Compute min, max, and average execution times.
  double minTime = std::numeric_limits<double>::max();
  double maxTime = std::numeric_limits<double>::lowest();
  double sumTime = 0.0;

  for (const auto& result : threadResults) {
    minTime = std::min(minTime, result.executionTime);
    maxTime = std::max(maxTime, result.executionTime);
    sumTime += result.executionTime;
  }

  double avgTime = sumTime / numThreads;

  std::cout << "Min execution time: " << minTime << " seconds" << std::endl;
  std::cout << "Max execution time: " << maxTime << " seconds" << std::endl;
  std::cout << "Average execution time: " << avgTime << " seconds" << std::endl;

  hrperf_pause();
  std::this_thread::sleep_for(std::chrono::seconds(3));

  return 0;
}
