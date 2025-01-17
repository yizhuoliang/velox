#include <folly/init/Init.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/TypeResolver.h"

#include <iostream>
#include <limits>
#include <memory>
#include <vector>
#include <unordered_map>
#include <chrono>

using namespace facebook::velox;
using namespace facebook::velox::connector::tpch;
using exec::test::PlanBuilder;

// Utility to get the leaf node ID.
// If your plan is more complex with multiple leaf nodes, you'll need to adapt this.
core::PlanNodeId getOnlyLeafPlanNodeId(const core::PlanNodePtr& root) {
  const auto& sources = root->sources();
  if (sources.empty()) {
    return root->id();
  }
  VELOX_CHECK_EQ(1, sources.size(), "Multiple leaf nodes found, which is unexpected.");
  return getOnlyLeafPlanNodeId(sources[0]);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  // Initialize memory and register standard Velox functions.
  memory::initializeMemoryManager({});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  auto pool = memory::memoryManager()->addLeafPool();

  // Register Tpch connector.
  connector::registerConnectorFactory(
      std::make_shared<connector::tpch::TpchConnectorFactory>());

  const std::string kTpchConnectorId = "test-tpch";
  {
    auto tpchConnector = connector::getConnectorFactory(
                             connector::tpch::TpchConnectorFactory::kTpchConnectorName)
                             ->newConnector(
                                 kTpchConnectorId,
                                 std::make_shared<config::ConfigBase>(
                                     std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpchConnector);
  }

  // Create query context and executor.
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());

  // We'll run a simple test: scan first 5 rows from TBL_NATION.
  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder()
      .tpchTableScan(
          facebook::velox::tpch::Table::TBL_NATION,
          {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
      .capturePlanNodeId(scanNodeId)
      .limit(0, 5, false)
      .planNode();

  // Create a consumer that prints rows to stdout.
  exec::Consumer consumer = [](RowVectorPtr result,
                               ContinueFuture* /*unused*/) -> exec::BlockingReason {
    std::cout << "Consumer triggered." << std::endl;
    if (!result) {
      return exec::BlockingReason::kNotBlocked;
    }

    // Print the rows.
    const auto& type = result->type()->asRow();
    auto numRows = result->size();
    std::cout << "Received " << numRows << " rows:" << std::endl;
    for (vector_size_t i = 0; i < numRows; ++i) {
      std::cout << "Row " << i << ": ";
      for (int32_t col = 0; col < type.size(); ++col) {
        auto child = result->childAt(col);
        if (child->isNullAt(i)) {
          std::cout << "NULL";
        } else {
          std::cout << child->toString(i);
        }
        if (col < type.size() - 1) {
          std::cout << ", ";
        }
      }
      std::cout << "\n";
    }

    return exec::BlockingReason::kNotBlocked;
  };

  auto fragment = facebook::velox::core::PlanFragment(plan);
  auto task = exec::Task::create(
      "tpch_task",
      fragment,
      0,
      queryCtx,
      exec::Task::ExecutionMode::kParallel,
      consumer);

  // Add a single Tpch split to the task so it has input data.
  task->addSplit(
      scanNodeId,
      exec::Split(
          std::make_shared<TpchConnectorSplit>(kTpchConnectorId, /*totalParts=*/1, /*partNumber=*/0)));
  // Signal that there are no more splits.
  task->noMoreSplits(scanNodeId);

  std::cout << "Press ENTER to start the query..." << std::endl;
  std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  // Start the task with a certain number of threads.
  task->start(std::thread::hardware_concurrency(), 1);

  // Wait for completion to ensure you see results before exiting.
  task->taskCompletionFuture().wait();

  std::cout << "Press ENTER to exit..." << std::endl;
  std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  // Clean up.
  connector::unregisterConnector(kTpchConnectorId);
  connector::unregisterConnectorFactory(
      connector::tpch::TpchConnectorFactory::kTpchConnectorName);

  return 0;
}
