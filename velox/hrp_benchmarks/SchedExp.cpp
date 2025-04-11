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
#include <optional>

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

    // Task queue for thread reuse
    struct TaskInfo {
      enum TaskType { TRIPLE, SIN } type;
      int index;
    };

    std::mutex taskQueueMutex;
    std::condition_variable taskQueueCV;
    std::vector<TaskInfo> taskQueue;
    std::atomic<bool> allTasksEnqueued{false};

    // Thread tracking for join
    std::mutex threadsMutex;
    std::vector<std::thread> workerThreads;

    // Track how many are done
    std::atomic<int> doneTriple1{0};
    std::atomic<int> doneSin1{0};

    // Barrier for triple only
    SimpleBarrier tripleBarrier1(tripleCount1);

    // Function to get the next task from the queue
    auto getNextTask = [&]() -> std::optional<TaskInfo> {
      std::unique_lock<std::mutex> lock(taskQueueMutex);
      // Wait for a task if queue is empty but not all tasks are enqueued yet
      if (taskQueue.empty()) {
        if (allTasksEnqueued.load()) {
          return std::nullopt; // No more tasks
        }
        // Wait for a task to be added or all tasks to be enqueued
        taskQueueCV.wait(lock, [&]() { 
          return !taskQueue.empty() || allTasksEnqueued.load(); 
        });
        if (taskQueue.empty()) {
          return std::nullopt; // Still no task after waking up
        }
      }
      
      // Get task from queue
      TaskInfo task = taskQueue.back();
      taskQueue.pop_back();
      return task;
    };

    // Function to add a task to the queue
    auto addTaskToQueue = [&](TaskInfo task) {
      {
        std::lock_guard<std::mutex> lock(taskQueueMutex);
        taskQueue.push_back(task);
      }
      taskQueueCV.notify_one();
    };
    
    // Worker thread function that processes tasks from the queue
    auto workerThreadFunc = [&]() {
      while (true) {
        // Get next task from queue
        auto taskOpt = getNextTask();
        if (!taskOpt) {
          // No more tasks
          break;
        }

        auto task = *taskOpt;
        
        // Execute the task
        if (task.type == TaskInfo::TRIPLE) {
          // Triple task will use the barrier
          tripleWorker(tripleData1[task.index], numCols, &tripleResults1[task.index], &tripleBarrier1);
          
          // Mark as done
          int finishedCount = doneTriple1.fetch_add(1);
          
          // Each finished triple task spawns a sin task if we haven't spawned all sins yet
          if (finishedCount < (int)sinCount1) {
            // Add a sin task to the queue
            int sinIdx = finishedCount;
            addTaskToQueue({TaskInfo::SIN, sinIdx});
          }
        } else {
          // Sin task - no barrier
          sinWorker(sinData1[task.index], numCols, &sinResults1[task.index], nullptr);
          
          // Mark as done
          doneSin1.fetch_add(1);
        }
      }
    };

    // 1. Enqueue all triple tasks
    for (int i = 0; i < tripleCount1; ++i) {
      addTaskToQueue({TaskInfo::TRIPLE, i});
    }

    // 2. Start worker threads (2*userThreads of them to match maxConcurrency)
    int64_t maxConcurrency = 2 * userThreads;
    for (int i = 0; i < maxConcurrency; ++i) {
      workerThreads.emplace_back(workerThreadFunc);
    }

    // 3. Wait until all triple and sin tasks are done
    while (doneTriple1.load() < tripleCount1 || doneSin1.load() < sinCount1) {
      // Check if any triple tasks have completed
      if (doneSin1.load() < sinCount1) {
        // Sleep a bit to avoid busy-waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      } else {
        // All sin tasks are done, just need to wait for any remaining triple tasks
        break;
      }
    }

    // Mark that all tasks have been enqueued
    allTasksEnqueued.store(true);
    taskQueueCV.notify_all();

    // 4. Join all worker threads
    for (auto &th : workerThreads) {
      if (th.joinable()) {
        th.join();
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
    // Track which type of task was last spawned (to alternate)
    std::atomic<bool> lastSpawnedSin{false};

    // Task queue for thread reuse
    struct TaskInfo {
      enum TaskType { TRIPLE, SIN } type;
      int index;
      bool isFirstBatch;
    };
    
    std::mutex taskQueueMutex;
    std::condition_variable taskQueueCV;
    std::vector<TaskInfo> taskQueue;
    std::atomic<bool> allTasksEnqueued{false};

    // barrier for first half triple + sin
    SimpleBarrier round2Barrier((int)userThreads + (int)userThreads);

    // Function to get the next task from the queue
    auto getNextTask = [&]() -> std::optional<TaskInfo> {
      std::unique_lock<std::mutex> lock(taskQueueMutex);
      // Wait for a task if queue is empty but not all tasks are enqueued yet
      if (taskQueue.empty()) {
        if (allTasksEnqueued.load()) {
          return std::nullopt; // No more tasks
        }
        // Wait for a task to be added or all tasks to be enqueued
        taskQueueCV.wait(lock, [&]() { 
          return !taskQueue.empty() || allTasksEnqueued.load(); 
        });
        if (taskQueue.empty()) {
          return std::nullopt; // Still no task after waking up
        }
      }
      
      // Get task from queue
      TaskInfo task = taskQueue.back();
      taskQueue.pop_back();
      return task;
    };

    // Function to add a task to the queue
    auto addTaskToQueue = [&](TaskInfo task) {
      {
        std::lock_guard<std::mutex> lock(taskQueueMutex);
        taskQueue.push_back(task);
      }
      taskQueueCV.notify_one();
    };

    // We'll define a function to enqueue more tasks as needed
    std::mutex spawnMutex; // protect spawn logic

    auto enqueueNextTaskIfPossible = [&]() {
      // If multiple threads finish nearly at once, we want them all to call this.
      // We lock to avoid overlapping enqueues.
      std::lock_guard<std::mutex> lk(spawnMutex);

      // Only enqueue a new task if we have capacity
      int currentActive = (int)activeThreads.load();
      if (currentActive >= (int)maxConcurrency) {
        return false; // at concurrency limit
      }

      bool taskEnqueued = false;
      bool trySinFirst = !lastSpawnedSin.load();

      // Try to enqueue the first task type (alternating sin/triple)
      if (trySinFirst) {
        int s = nextSinIdx.load();
        if (s < (int)sinCount2) {
          if (nextSinIdx.compare_exchange_strong(s, s + 1)) {
            // Enqueue sin task
            activeThreads.fetch_add(1);
            addTaskToQueue({TaskInfo::SIN, s, false});
            lastSpawnedSin.store(true);
            taskEnqueued = true;
          }
        }
      } else {
        int t = nextTripleIdx.load();
        if (t < (int)tripleCount2) {
          if (nextTripleIdx.compare_exchange_strong(t, t + 1)) {
            // Enqueue triple task
            activeThreads.fetch_add(1);
            addTaskToQueue({TaskInfo::TRIPLE, t, false});
            lastSpawnedSin.store(false);
            taskEnqueued = true;
          }
        }
      }

      // If we couldn't enqueue the preferred type, try the other type
      if (!taskEnqueued) {
        if (trySinFirst) {
          // We tried sin first but couldn't enqueue, now try triple
          int t = nextTripleIdx.load();
          if (t < (int)tripleCount2) {
            if (nextTripleIdx.compare_exchange_strong(t, t + 1)) {
              // Enqueue triple task
              activeThreads.fetch_add(1);
              addTaskToQueue({TaskInfo::TRIPLE, t, false});
              lastSpawnedSin.store(false);
              taskEnqueued = true;
            }
          }
        } else {
          // We tried triple first but couldn't enqueue, now try sin
          int s = nextSinIdx.load();
          if (s < (int)sinCount2) {
            if (nextSinIdx.compare_exchange_strong(s, s + 1)) {
              // Enqueue sin task
              activeThreads.fetch_add(1);
              addTaskToQueue({TaskInfo::SIN, s, false});
              lastSpawnedSin.store(true);
              taskEnqueued = true;
            }
          }
        }
      }

      return taskEnqueued;
    };

    // Worker thread function that processes tasks from the queue
    auto workerThreadFunc = [&]() {
      while (true) {
        // Get next task from queue
        auto taskOpt = getNextTask();
        if (!taskOpt) {
          // No more tasks
          break;
        }

        auto task = *taskOpt;
        
        // Execute the task
        if (task.type == TaskInfo::TRIPLE) {
          // Execute triple task
          SimpleBarrier* b = (task.isFirstBatch ? &round2Barrier : nullptr);
          tripleWorker(tripleData2[task.index], numCols, &tripleResults2[task.index], b);
          
          // Mark as done
          doneTriple2.fetch_add(1);
          activeThreads.fetch_sub(1);
        } else {
          // Execute sin task
          SimpleBarrier* b = (task.isFirstBatch ? &round2Barrier : nullptr);
          sinWorker(sinData2[task.index], numCols, &sinResults2[task.index], b);
          
          // Mark as done
          doneSin2.fetch_add(1);
          activeThreads.fetch_sub(1);
        }

        // Try to enqueue next task
        enqueueNextTaskIfPossible();
      }
    };

    // 1) Enqueue initial batch of tasks
    // First, enqueue userThreads triple and userThreads sin tasks (first batch)
    // in an alternating pattern for consistency with later task spawning
    for (int i = 0; i < (int)userThreads; ++i) {
      // Add a triple task
      activeThreads.fetch_add(1);
      addTaskToQueue({TaskInfo::TRIPLE, i, true});
      
      // Add a sin task
      activeThreads.fetch_add(1);
      addTaskToQueue({TaskInfo::SIN, i, true});
    }

    // 2) Start worker threads (2*userThreads of them to match maxConcurrency)
    for (int i = 0; i < maxConcurrency; ++i) {
      round2Threads.emplace_back(workerThreadFunc);
    }

    // 3) Keep trying to enqueue tasks until all are enqueued
    while (nextTripleIdx.load() < tripleCount2 || nextSinIdx.load() < sinCount2) {
      enqueueNextTaskIfPossible();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Mark that all tasks have been enqueued
    allTasksEnqueued.store(true);
    taskQueueCV.notify_all();

    // 4) Wait for all threads to finish
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