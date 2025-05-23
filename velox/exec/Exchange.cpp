/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/exec/Exchange.h"

#include "velox/exec/Task.h"

namespace facebook::velox::exec {

void Exchange::addTaskIds(std::vector<std::string>& taskIds) {
  std::shuffle(std::begin(taskIds), std::end(taskIds), rng_);
  for (const std::string& taskId : taskIds) {
    exchangeClient_->addRemoteTaskId(taskId);
  }
  stats_.wlock()->numSplits += taskIds.size();
}

bool Exchange::getSplits(ContinueFuture* future) {
  if (!processSplits_) {
    return false;
  }
  if (noMoreSplits_) {
    return false;
  }
  std::vector<std::string> taskIds;
  for (;;) {
    exec::Split split;
    auto reason = operatorCtx_->task()->getSplitOrFuture(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId(), split, *future);
    if (reason == BlockingReason::kNotBlocked) {
      if (split.hasConnectorSplit()) {
        auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
            split.connectorSplit);
        VELOX_CHECK(remoteSplit, "Wrong type of split");
        taskIds.push_back(remoteSplit->taskId);
      } else {
        addTaskIds(taskIds);
        exchangeClient_->noMoreRemoteTasks();
        noMoreSplits_ = true;
        if (atEnd_) {
          operatorCtx_->task()->multipleSplitsFinished(
              false, stats_.rlock()->numSplits, 0);
          recordExchangeClientStats();
        }
        return false;
      }
    } else {
      addTaskIds(taskIds);
      return true;
    }
  }
}

BlockingReason Exchange::isBlocked(ContinueFuture* future) {
  if (!currentPages_.empty() || atEnd_) {
    return BlockingReason::kNotBlocked;
  }

  // Start fetching data right away. Do not wait for all
  // splits to be available.

  if (!splitFuture_.valid()) {
    getSplits(&splitFuture_);
  }

  ContinueFuture dataFuture;
  currentPages_ =
      exchangeClient_->next(preferredOutputBatchBytes_, &atEnd_, &dataFuture);
  if (!currentPages_.empty() || atEnd_) {
    if (atEnd_ && noMoreSplits_) {
      const auto numSplits = stats_.rlock()->numSplits;
      operatorCtx_->task()->multipleSplitsFinished(false, numSplits, 0);
    }
    recordExchangeClientStats();
    return BlockingReason::kNotBlocked;
  }

  // We have a dataFuture and we may also have a splitFuture_.

  if (splitFuture_.valid()) {
    // Block until data becomes available or more splits arrive.
    std::vector<ContinueFuture> futures;
    futures.push_back(std::move(splitFuture_));
    futures.push_back(std::move(dataFuture));
    *future = folly::collectAny(futures).unit();
    return BlockingReason::kWaitForSplit;
  }

  // Block until data becomes available.
  *future = std::move(dataFuture);
  return BlockingReason::kWaitForProducer;
}

bool Exchange::isFinished() {
  return atEnd_ && currentPages_.empty();
}

RowVectorPtr Exchange::getOutput() {
  if (currentPages_.empty()) {
    return nullptr;
  }

  uint64_t rawInputBytes{0};
  vector_size_t resultOffset = 0;
  if (getSerde()->supportsAppendInDeserialize()) {
    for (const auto& page : currentPages_) {
      rawInputBytes += page->size();

      auto inputStream = page->prepareStreamForDeserialize();

      while (!inputStream->atEnd()) {
        getSerde()->deserialize(
            inputStream.get(),
            pool(),
            outputType_,
            &result_,
            resultOffset,
            &options_);
        resultOffset = result_->size();
      }
    }
  } else {
    VELOX_CHECK(
        getSerde()->kind() == VectorSerde::Kind::kCompactRow ||
        getSerde()->kind() == VectorSerde::Kind::kUnsafeRow);

    std::unique_ptr<folly::IOBuf> mergedBufs;
    for (const auto& page : currentPages_) {
      rawInputBytes += page->size();
      if (mergedBufs == nullptr) {
        mergedBufs = page->getIOBuf()->clone();
      } else {
        mergedBufs->appendToChain(page->getIOBuf()->clone());
      }
    }
    VELOX_CHECK_NOT_NULL(mergedBufs);
    auto mergedPages = std::make_unique<SerializedPage>(std::move(mergedBufs));
    auto inputStream = mergedPages->prepareStreamForDeserialize();
    getSerde()->deserialize(
        inputStream.get(),
        pool(),
        outputType_,
        &result_,
        resultOffset,
        &options_);
    // We expect the row-wise deserialization to consume all the input into one
    // output vector.
    VELOX_CHECK(inputStream->atEnd());
  }
  currentPages_.clear();

  {
    auto lockedStats = stats_.wlock();
    lockedStats->rawInputBytes += rawInputBytes;
    lockedStats->rawInputPositions += result_->size();
    lockedStats->addInputVector(result_->estimateFlatSize(), result_->size());
  }

  return result_;
}

void Exchange::close() {
  SourceOperator::close();
  currentPages_.clear();
  result_ = nullptr;
  if (exchangeClient_) {
    recordExchangeClientStats();
    exchangeClient_->close();
  }
  exchangeClient_ = nullptr;
  stats_.wlock()->addRuntimeStat(
      Operator::kShuffleSerdeKind,
      RuntimeCounter(static_cast<int64_t>(serdeKind_)));
}

void Exchange::recordExchangeClientStats() {
  if (!processSplits_) {
    return;
  }

  auto lockedStats = stats_.wlock();
  const auto exchangeClientStats = exchangeClient_->stats();
  for (const auto& [name, value] : exchangeClientStats) {
    lockedStats->runtimeStats.erase(name);
    lockedStats->runtimeStats.insert({name, value});
  }

  auto backgroundCpuTimeMs =
      exchangeClientStats.find(ExchangeClient::kBackgroundCpuTimeMs);
  if (backgroundCpuTimeMs != exchangeClientStats.end()) {
    const CpuWallTiming backgroundTiming{
        static_cast<uint64_t>(backgroundCpuTimeMs->second.count),
        0,
        static_cast<uint64_t>(backgroundCpuTimeMs->second.sum) *
            Timestamp::kNanosecondsInMillisecond};
    lockedStats->backgroundTiming.clear();
    lockedStats->backgroundTiming.add(backgroundTiming);
  }
}

VectorSerde* Exchange::getSerde() {
  return getNamedVectorSerde(serdeKind_);
}

} // namespace facebook::velox::exec
