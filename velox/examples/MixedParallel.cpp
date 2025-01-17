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

extern "C" {
  #include "hrperf_api.h"
}

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class Barrier {
public:
  Barrier(int n) : numThreads_(n), count_(0), generation_(0) {}
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    int gen = generation_;
    if (++count_ == numThreads_) {
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
  int numThreads_;
  int count_;
  int generation_;
};

struct ThreadResult {
  double executionTime;
};

std::mutex printMutex;

void generateData(
    int64_t numRows,
    int64_t numCols,
    memory::MemoryPool* pool,
    int64_t seedOffset,
    RowVectorPtr& dataOut) {
  std::vector<VectorPtr> columns(numCols);
  std::vector<std::string> columnNames(numCols);
  std::mt19937_64 rng(42 + seedOffset);
  std::uniform_int_distribution<int64_t> dist;

  for (int64_t col = 0; col < numCols; ++col) {
    columnNames[col] = "c" + std::to_string(col);
    auto vectorPtr = BaseVector::create(BIGINT(), numRows, pool);
    auto flat = vectorPtr->asFlatVector<int64_t>();
    auto rawValues = flat->mutableRawValues();
    for (int64_t row = 0; row < numRows; ++row) {
      rawValues[row] = dist(rng);
    }
    columns[col] = vectorPtr;
  }
  auto rowType = ROW(std::move(columnNames), std::vector<TypePtr>(numCols, BIGINT()));
  dataOut = std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), numRows, columns);
}

void aggregatorThread(
    int64_t numRows,
    int64_t numCols,
    ThreadResult* result,
    Barrier& barrier,
    int64_t threadIndex) {
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  RowVectorPtr data;
  generateData(numRows, numCols, pool.get(), threadIndex, data);

  std::vector<std::string> aggregates;
  aggregates.reserve(numCols);
  for (int64_t col = 0; col < numCols; ++col) {
    aggregates.push_back("avg(c" + std::to_string(col) + ") as avg_c" + std::to_string(col));
  }

  auto plan = PlanBuilder()
      .values({data})
      .singleAggregation({}, aggregates)
      .planFragment();

  auto task = exec::Task::create("agg_task", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);

  barrier.Wait();

  auto start = std::chrono::high_resolution_clock::now();
  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }
  auto end = std::chrono::high_resolution_clock::now();
  result->executionTime = std::chrono::duration<double>(end - start).count();

  {
    std::lock_guard<std::mutex> lock(printMutex);
    if (resultVector) {
      std::cout << "[Aggregator] Thread " << std::this_thread::get_id() << " first row:\n"
                << resultVector->toString(0, std::min<int64_t>(1, resultVector->size()))
                << std::endl;
    }
  }

  volatile int a = 0;
  for (int i = 0; i < 10000000; i++) { i + a; a * i; a + a; }
  (void)a;
}

void projectionThread(
    int64_t numRows,
    int64_t numCols,
    ThreadResult* result,
    Barrier& barrier,
    int64_t threadIndex) {
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  RowVectorPtr data;
  generateData(numRows, numCols, pool.get(), threadIndex + 10000, data);

  std::vector<std::string> projections;
  projections.reserve(numCols);
  for (int64_t col = 0; col < numCols; ++col) {
    projections.push_back("power(c" + std::to_string(col) + ", 4) as c" + std::to_string(col) + "_pow4");
  }

  auto plan = PlanBuilder()
      .values({data})
      .project(projections)
      .planFragment();

  auto task = exec::Task::create("proj_task", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);

  barrier.Wait();

  auto start = std::chrono::high_resolution_clock::now();
  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }
  auto end = std::chrono::high_resolution_clock::now();
  result->executionTime = std::chrono::duration<double>(end - start).count();

  {
    std::lock_guard<std::mutex> lock(printMutex);
    if (resultVector) {
      std::cout << "[Projection] Thread " << std::this_thread::get_id() << " first row:\n"
                << resultVector->toString(0, std::min<int64_t>(1, resultVector->size()))
                << std::endl;
    }
  }

  volatile int a = 0;
  for (int i = 0; i < 10000000; i++) { i + a; a * i; a + a; }
  (void)a;
}

void orderByThread(
    int64_t numRows,
    int64_t numCols,
    ThreadResult* result,
    Barrier& barrier,
    int64_t threadIndex) {
  auto pool = memory::memoryManager()->addLeafPool();
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  RowVectorPtr data;
  generateData(numRows, numCols, pool.get(), threadIndex + 20000, data);

  auto plan = PlanBuilder()
      .values({data})
      .orderBy({"c0 ASC"}, false)
      .planFragment();

  auto task = exec::Task::create("order_task", std::move(plan), 0, queryCtx, exec::Task::ExecutionMode::kSerial);

  barrier.Wait();

  auto start = std::chrono::high_resolution_clock::now();
  RowVectorPtr resultVector;
  while (auto res = task->next()) {
    resultVector = res;
  }
  auto end = std::chrono::high_resolution_clock::now();
  result->executionTime = std::chrono::duration<double>(end - start).count();

  {
    std::lock_guard<std::mutex> lock(printMutex);
    if (resultVector) {
      std::cout << "[OrderBy] Thread " << std::this_thread::get_id() << " first row:\n"
                << resultVector->toString(0, std::min<int64_t>(1, resultVector->size()))
                << std::endl;
    }
  }

  volatile int a = 0;
  for (int i = 0; i < 10000000; i++) { i + a; a * i; a + a; }
  (void)a;
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  memory::initializeMemoryManager({});

  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols> <numThreads>\n";
    return 1;
  }

  hrperf_start();

  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);
  int64_t userThreads = std::stoll(argv[3]);

  if (numRows <= 0 || numCols <= 0 || userThreads <= 0) {
    std::cerr << "Rows, cols, threads must be positive.\n";
    return 1;
  }

  std::cout << "Rows=" << numRows << ", Cols=" << numCols
            << ", Threads=" << userThreads << " => total " << (3 * userThreads)
            << " worker threads.\n";

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  double perValue = sizeof(int64_t);
  double colBytes = numRows * perValue;
  double totalBytes = colBytes * numCols;
  double colGB = colBytes / (1024 * 1024 * 1024.0);
  double totalGB = totalBytes / (1024 * 1024 * 1024.0);
  std::cout << "Data size/column: " << colGB << " GB\n"
            << "Data size/thread: " << totalGB << " GB\n"
            << "Data size total: " << totalGB * userThreads << " GB\n";

  Barrier barrier(3 * userThreads);

  std::vector<ThreadResult> aggResults(userThreads);
  std::vector<ThreadResult> projResults(userThreads);
  std::vector<ThreadResult> orderResults(userThreads);

  std::vector<std::thread> threads;
  threads.reserve(3 * userThreads);

  for (int64_t i = 0; i < userThreads; ++i) {
    threads.emplace_back(aggregatorThread, numRows, numCols, &aggResults[i], std::ref(barrier), i);
  }
  for (int64_t i = 0; i < userThreads; ++i) {
    threads.emplace_back(projectionThread, numRows, numCols, &projResults[i], std::ref(barrier), i);
  }
  for (int64_t i = 0; i < userThreads; ++i) {
    threads.emplace_back(orderByThread, numRows, numCols, &orderResults[i], std::ref(barrier), i);
  }

  for (auto& t : threads) {
    t.join();
  }

  auto computeStats = [&](const std::vector<ThreadResult>& results, const std::string& name) {
    double mn = std::numeric_limits<double>::max();
    double mx = std::numeric_limits<double>::lowest();
    double sum = 0.0;
    for (auto& r : results) {
      mn = std::min(mn, r.executionTime);
      mx = std::max(mx, r.executionTime);
      sum += r.executionTime;
    }
    double avg = sum / results.size();
    std::cout << name << " Min: " << mn << "s, Max: " << mx << "s, Avg: " << avg << "s\n";
  };

  computeStats(aggResults, "[Aggregator]");
  computeStats(projResults, "[Projection]");
  computeStats(orderResults, "[OrderBy]");

  hrperf_pause();
  std::this_thread::sleep_for(std::chrono::seconds(3));
  return 0;
}