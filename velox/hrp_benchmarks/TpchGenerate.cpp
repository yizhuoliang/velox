/******************************************************************************
 * GenerateTpchData.cpp
 *
 * A minimal example that generates TPC-H data from TpchConnector and
 * writes it out in Hive/DWRF format using parallel execution with splits.
 *
 * The crucial change here is prefixing the absolute path with "file:" so that
 * TpchBenchmark (or other Velox code) recognizes the local file system.
 *
 * Usage:
 *   ./GenerateTpchData <scaleFactor>
 *
 * Example:
 *   ./GenerateTpchData 1.0
 *   ./velox_tpch_benchmark --data_path="file:/absolute/path/to/data"
 *
 ******************************************************************************/

#include <folly/init/Init.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// Velox common/utility
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/config/Config.h"

// TpchConnector
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"

// HiveConnector
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"

// DWIO readers/writers
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"

// Core execution structures
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

// Functions
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

// Parsing
#include "velox/parse/Expressions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::tpch;
using exec::test::PlanBuilder;

namespace fs = std::filesystem;
namespace ctpch = facebook::velox::connector::tpch;

//------------------------------------------------------------------------
// Utility to return the single leaf node ID (assuming exactly one leaf).
// This matches your Tpch example code.
//------------------------------------------------------------------------
core::PlanNodeId getOnlyLeafPlanNodeId(const core::PlanNodePtr& root) {
  const auto& sources = root->sources();
  if (sources.empty()) {
    return root->id();
  }
  VELOX_CHECK_EQ(
      1,
      sources.size(),
      "Multiple leaf nodes found, which is unexpected for this example.");
  return getOnlyLeafPlanNodeId(sources[0]);
}

/**
 * Return the lowercase name of the TPC-H table enum
 * as needed by the TpchBenchmark for folder names.
 */
std::string tableName(facebook::velox::tpch::Table table) {
  switch (table) {
    case facebook::velox::tpch::Table::TBL_CUSTOMER: return "customer";
    case facebook::velox::tpch::Table::TBL_LINEITEM: return "lineitem";
    case facebook::velox::tpch::Table::TBL_NATION:   return "nation";
    case facebook::velox::tpch::Table::TBL_ORDERS:   return "orders";
    case facebook::velox::tpch::Table::TBL_PART:     return "part";
    case facebook::velox::tpch::Table::TBL_PARTSUPP: return "partsupp";
    case facebook::velox::tpch::Table::TBL_REGION:   return "region";
    case facebook::velox::tpch::Table::TBL_SUPPLIER: return "supplier";
    default:
      VELOX_FAIL("Unsupported TPC-H table in tableName()");
  }
}

/**
 * Return the list of column names for a given TPC-H table,
 * matching what TpchBenchmark expects.
 */
std::vector<std::string> tableColumns(facebook::velox::tpch::Table table) {
  switch (table) {
    case facebook::velox::tpch::Table::TBL_NATION:
      return {"n_nationkey", "n_name", "n_regionkey", "n_comment"};

    case facebook::velox::tpch::Table::TBL_REGION:
      return {"r_regionkey", "r_name", "r_comment"};

    case facebook::velox::tpch::Table::TBL_PART:
      return {"p_partkey",
              "p_name",
              "p_mfgr",
              "p_brand",
              "p_type",
              "p_size",
              "p_container",
              "p_retailprice",
              "p_comment"};

    case facebook::velox::tpch::Table::TBL_SUPPLIER:
      return {"s_suppkey",
              "s_name",
              "s_address",
              "s_nationkey",
              "s_phone",
              "s_acctbal",
              "s_comment"};

    case facebook::velox::tpch::Table::TBL_PARTSUPP:
      return {"ps_partkey",
              "ps_suppkey",
              "ps_availqty",
              "ps_supplycost",
              "ps_comment"};

    case facebook::velox::tpch::Table::TBL_CUSTOMER:
      return {"c_custkey",
              "c_name",
              "c_address",
              "c_nationkey",
              "c_phone",
              "c_acctbal",
              "c_mktsegment",
              "c_comment"};

    case facebook::velox::tpch::Table::TBL_ORDERS:
      return {"o_orderkey",
              "o_custkey",
              "o_orderstatus",
              "o_totalprice",
              "o_orderdate",
              "o_orderpriority",
              "o_clerk",
              "o_shippriority",
              "o_comment"};

    case facebook::velox::tpch::Table::TBL_LINEITEM:
      return {"l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment"};

    default:
      VELOX_FAIL("Unsupported TPC-H table in tableColumns()");
  }
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  //-----------------------------------------------------------------------
  // 1) Parse scaleFactor from command line.
  //-----------------------------------------------------------------------
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <scaleFactor>\n"
              << "Example: " << argv[0] << " 1.0\n";
    return 1;
  }
  double scaleFactor = std::stod(argv[1]);
  std::cout << "Generating TPC-H data with scaleFactor=" << scaleFactor << std::endl;

  //-----------------------------------------------------------------------
  // 2) Initialize Velox: memory, register functions, etc.
  //-----------------------------------------------------------------------
  memory::initializeMemoryManager({});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  auto pool = memory::memoryManager()->addLeafPool();

  //-----------------------------------------------------------------------
  // 3) Register TpchConnector (reads TPC-H data in memory).
  //-----------------------------------------------------------------------
  connector::registerConnectorFactory(
      std::make_shared<connector::tpch::TpchConnectorFactory>());

  // We'll provide a config specifying scale_factor to TpchConnector.
  const std::string kTpchConnectorId = "test-tpch";
  {
    std::unordered_map<std::string, std::string> conf{
        {"scale_factor", std::to_string(scaleFactor)}};
    auto tpchConnector = connector::getConnectorFactory(
                             connector::tpch::TpchConnectorFactory::kTpchConnectorName)
                             ->newConnector(
                                 kTpchConnectorId,
                                 std::make_shared<config::ConfigBase>(std::move(conf)));
    connector::registerConnector(tpchConnector);
  }

  //-----------------------------------------------------------------------
  // 4) Register HiveConnector (writes data to local DWRF).
  //-----------------------------------------------------------------------
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());

  const std::string kHiveConnectorId = "test-hive";
  {
    auto hiveConnector = connector::getConnectorFactory(
                             connector::hive::HiveConnectorFactory::kHiveConnectorName)
                             ->newConnector(
                                 kHiveConnectorId,
                                 std::make_shared<config::ConfigBase>(
                                     std::unordered_map<std::string, std::string>{}));
    connector::registerConnector(hiveConnector);
  }

  //-----------------------------------------------------------------------
  // 5) Create query context + executor (for parallel tasks).
  //-----------------------------------------------------------------------
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = core::QueryCtx::create(executor.get());

  //-----------------------------------------------------------------------
  // 6) Table list for TPC-H. We'll create and write each in turn.
  //-----------------------------------------------------------------------
  std::vector<facebook::velox::tpch::Table> tables = {
      facebook::velox::tpch::Table::TBL_CUSTOMER,
      facebook::velox::tpch::Table::TBL_LINEITEM,
      facebook::velox::tpch::Table::TBL_NATION,
      facebook::velox::tpch::Table::TBL_ORDERS,
      facebook::velox::tpch::Table::TBL_PART,
      facebook::velox::tpch::Table::TBL_PARTSUPP,
      facebook::velox::tpch::Table::TBL_REGION,
      facebook::velox::tpch::Table::TBL_SUPPLIER,
  };

  fs::create_directories("./data"); // ensure ./data exists

  // For writing, we typically get no data back from the TableWrite operator.
  // But we still need a "Consumer" callback to pass to Task::create.
  exec::Consumer consumer = [](RowVectorPtr result,
                               ContinueFuture* /*unused*/) -> exec::BlockingReason {
    // TableWriter typically doesn't produce row outputs (just final stats).
    if (result) {
      LOG(INFO) << "Received a vector of size: " << result->size();
    }
    return exec::BlockingReason::kNotBlocked;
  };

  //-----------------------------------------------------------------------
  // 7) Generate + write each table
  //-----------------------------------------------------------------------
  for (auto table : tables) {
    // Build the local path: e.g. "./data/supplier"
    auto localOutDir = fs::path("data") / tableName(table);
    fs::create_directories(localOutDir);

    // Convert to absolute path:
    auto absLocalOutDir = fs::absolute(localOutDir).string();

    // **Prefix** with "file:" so that Velox sees a local filesystem URI:
    // i.e. "file:/home/cc/nvelox/data/supplier"
    auto finalOutDir = std::string("file:") + absLocalOutDir;

    std::cout << "Generating " << tableName(table)
              << " => " << finalOutDir << "...\n";

    // We'll build a plan:
    //   TpchTableScan(...) -> TableWrite(...)
    // Then we'll create a Task in kParallel mode, add splits, start the task,
    // and wait for it to finish.
    core::PlanNodeId scanNodeId;
    auto planNode = PlanBuilder()
        .tpchTableScan(
            table,
            tableColumns(table), // if needed, std::move() this
            scaleFactor)
        .capturePlanNodeId(scanNodeId)
        .tableWrite(finalOutDir, dwio::common::FileFormat::DWRF)
        .planNode();

    auto fragment = core::PlanFragment(planNode);

    // Create Task in kParallel mode, using our 'consumer' for results (if any).
    auto task = exec::Task::create(
        "tpch_write_task_" + tableName(table),
        fragment,
        /*destination=*/0,
        queryCtx,
        exec::Task::ExecutionMode::kParallel,
        consumer);

    // For TpchConnector, we can create splits by specifying how many totalParts
    // we want. For small scale factors, we might do 1 part. For large scale
    // factors, do more. We'll do 1 part for simplicity.
    const int totalParts = 1;
    for (int part = 0; part < totalParts; ++part) {
      // Create TpchConnectorSplit referencing our Tpch connector id.
      auto tpchSplit = std::make_shared<connector::tpch::TpchConnectorSplit>(
          kTpchConnectorId, totalParts, part);

      // Add that to the Task, wrapped in an exec::Split with the scan node id.
      task->addSplit(scanNodeId, exec::Split{tpchSplit});
    }
    // Signal no more splits for this scan node.
    task->noMoreSplits(scanNodeId);

    // Now we launch the task in parallel with a certain # of drivers
    // and # of pipelines. Typical usage uses (hardware_concurrency, 1).
    task->start(std::thread::hardware_concurrency(), 1);

    // Wait for it to finish.
    task->taskCompletionFuture().wait();

    std::cout << "Finished " << tableName(table) << ".\n";
  }

  //-----------------------------------------------------------------------
  // 8) Cleanup connectors
  //-----------------------------------------------------------------------
  connector::unregisterConnector(kTpchConnectorId);
  connector::unregisterConnector(kHiveConnectorId);

  connector::unregisterConnectorFactory(
      connector::tpch::TpchConnectorFactory::kTpchConnectorName);
  connector::unregisterConnectorFactory(
      connector::hive::HiveConnectorFactory::kHiveConnectorName);

  std::cout << "\nAll TPC-H tables generated under 'file:" << fs::absolute("data").string()
            << "'\n(Use --data_path=\"file:/path/to/data\" for TpchBenchmark).\n";
  return 0;
}