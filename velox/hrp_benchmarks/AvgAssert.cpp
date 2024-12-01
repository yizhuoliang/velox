/*
 * Simple benchmark to generate random int64 data and use Velox to compute the average of each column.
 * The number of rows and columns can be specified via command-line arguments.
 * Before executing the query, the program prints the data size per column and total size in Gigabytes.
 * After the query, it prints the results and reports the execution time.
 */

#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
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

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

int main(int argc, char** argv) {
  // Initialize Folly.
  folly::Init init{&argc, &argv, false};

  // Initialize the process-wide memory-manager with the default options.
  memory::initializeMemoryManager({});

  // Parse command-line arguments.
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <numRows> <numCols>" << std::endl;
    return 1;
  }

  int64_t numRows = std::stoll(argv[1]);
  int64_t numCols = std::stoll(argv[2]);

  if (numRows <= 0 || numCols <= 0) {
    std::cerr << "Number of rows and columns must be positive integers." << std::endl;
    return 1;
  }

  std::cout << "Generating data with " << numRows << " rows and " << numCols
            << " columns." << std::endl;

  // Register Velox functions.
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

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

  std::mt19937_64 rng;
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

  // Compute data sizes.
  const int64_t bytesPerValue = sizeof(int64_t);
  double columnBytes = numRows * bytesPerValue;
  double totalBytes = columnBytes * numCols;
  double columnGB = columnBytes / (1024 * 1024 * 1024.0);
  double totalGB = totalBytes / (1024 * 1024 * 1024.0);

  std::cout << "Data size per column: " << columnGB << " GB" << std::endl;
  std::cout << "Total data size: " << totalGB << " GB" << std::endl;

  // Build the query plan to compute average of each column.
  std::vector<std::string> aggregates;
  aggregates.reserve(numCols);
  for (int64_t col = 0; col < numCols; ++col) {
    std::string agg = "avg(c" + std::to_string(col) + ") as avg_c" + std::to_string(col);
    aggregates.push_back(agg);
  }

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, aggregates)
                  .planNode();

  // Measure execution time.
  auto start = std::chrono::high_resolution_clock::now();

  auto result = AssertQueryBuilder(plan).copyResults(pool.get());

  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> diff = end - start;

  // Print results.
  std::cout << "Average of each column:" << std::endl;
  std::cout << result->toString(0, result->size()) << std::endl;

  // Report execution time.
  std::cout << "Execution time: " << diff.count() << " seconds" << std::endl;

  std::this_thread::sleep_for(std::chrono::seconds(3));

  return 0;
}
