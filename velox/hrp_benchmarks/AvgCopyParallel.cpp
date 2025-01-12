#include <folly/init/Init.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/TypeResolver.h"

#include <iostream>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <random>
#include <numeric>
#include <thread>

extern "C" {
  #include "hrperf_api.h"
}

using namespace facebook::velox;
using exec::test::HiveConnectorTestBase;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols> <numThreads>" << std::endl;
    return 1;
  }

  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);
  int64_t numThreads = std::stoll(argv[3]);

  if (numRows <= 0 || numCols <= 0 || numThreads <= 0) {
    std::cerr << "Number of rows, columns, and threads must be positive integers." << std::endl;
    return 1;
  }

  // Initialize memory and register standard Velox functions.
  memory::initializeMemoryManager({});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  // Create a memory pool.
  auto pool = memory::memoryManager()->addLeafPool();

  // Register Hive connector.
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());

  static const std::string kHiveConnectorId = "test-hive";
  {
    auto hiveConnectorFactory = connector::getConnectorFactory("hive");
    auto hiveConnector = hiveConnectorFactory->newConnector(
        kHiveConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
  }

  // Create query context and executor.
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());

  // Define row type for input data.
  std::vector<std::string> columnNames;
  columnNames.reserve(numCols);
  for (int64_t i = 0; i < numCols; ++i) {
    columnNames.push_back("c" + std::to_string(i));
  }

  std::vector<TypePtr> columnTypes(numCols, BIGINT());
  auto rowType = ROW(std::move(columnNames), std::move(columnTypes));

  // Generate random data for the input. We'll create a single RowVector with numRows rows.
  std::vector<VectorPtr> columns;
  columns.reserve(numCols);
  {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<int64_t> dist(0, 1000);

    for (int64_t c = 0; c < numCols; ++c) {
      auto col = BaseVector::create(BIGINT(), numRows, pool.get());
      auto rawValues = col->values()->asMutable<int64_t>();
      for (int64_t r = 0; r < numRows; ++r) {
        rawValues[r] = dist(rng);
      }
      columns.push_back(col);
    }
  }

  auto inputVector = std::make_shared<RowVector>(
      pool.get(),
      rowType,
      BufferPtr(nullptr),
      numRows,
      columns);

  std::vector<std::string> aggregates;
  aggregates.reserve(numCols);
  for (int64_t col = 0; col < numCols; ++col) {
    std::string agg = "avg(c" + std::to_string(col) + ") as avg_c" + std::to_string(col);
    aggregates.push_back(agg);
  }

    auto planFragment = exec::test::PlanBuilder()
                        .values({inputVector}, true)
                        .singleAggregation({}, aggregates)
                        .planFragment();
  
  exec::Consumer consumer = [numCols](
      RowVectorPtr result,
      ContinueFuture* /*future*/) -> facebook::velox::exec::BlockingReason {
    if (!result) {
      return facebook::velox::exec::BlockingReason::kNotBlocked;
    }
    for (int64_t i = 0; i < numCols; ++i) {
      auto avgVector = result->childAt(i)->asFlatVector<double>();
      double val = avgVector->valueAt(0);
      std::cout << "avg_c" << i << " = " << val << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  auto task = exec::Task::create(
        "task_id",
        std::move(planFragment),
        0,
        queryCtx,
        exec::Task::ExecutionMode::kParallel,
        consumer);

  std::cout << "Press ENTER to start the parallel aggregation task...";
  std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  hrperf_start();
  auto start = std::chrono::high_resolution_clock::now();

  // Start execution with the specified number of threads.
  task->start(numThreads, numThreads);

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Press ENTER to stop recording performance metrics...";
  std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  hrperf_pause();
  std::this_thread::sleep_for(std::chrono::seconds(3));

  std::chrono::duration<double> diff = end - start;
  double executionTime = diff.count();
  std::cout << "Execution time: " << executionTime << " seconds" << std::endl;

  return 0;
}
