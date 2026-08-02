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

#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/KvikioS3FileSystem.h"
#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/KvikioS3ReadFile.h"
#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/RegisterKvikioS3FileSystem.h"

#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"
#include "velox/experimental/cudf/CudfConfig.h"

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <numeric>

using namespace facebook::velox;
using namespace facebook::velox::cudf_velox::connector::hive;

namespace {

// MinIO ignores the region but KvikIO's SigV4 signing requires one.
constexpr const char* kTestRegion = "us-east-1";

class KvikioS3ReadTest : public S3Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    S3Test::SetUp();
    hiveConfig_ = minioServer_->hiveConfig(
        {{"hive.s3.endpoint.region", kTestRegion}});
  }

  void TearDown() override {
    filesystems::finalizeS3FileSystem();
    S3Test::TearDown();
  }

  /// Writes 'contents' to bucket/key through the local MinIO data directory.
  void putObject(
      const char* bucket,
      const char* key,
      const std::string& contents) {
    addBucket(bucket);
    const std::string path = localPath(bucket) + "/" + key;
    LocalWriteFile writeFile(path);
    writeFile.append(contents);
    writeFile.close();
  }

  /// A deterministic blob whose every byte position is identifiable, so a
  /// misplaced range shows up as a content mismatch rather than passing by
  /// accident on repeated bytes.
  static std::string makeBlob(size_t size) {
    std::string blob(size, '\0');
    for (size_t i = 0; i < size; ++i) {
      blob[i] = static_cast<char>('A' + (i % 26));
    }
    return blob;
  }

  std::shared_ptr<const config::ConfigBase> hiveConfig_;
};

/// Builds the (offset, buffers) pair for a preadv covering 'spans' within
/// 'blob', with gap ranges between them. Destination storage is owned by
/// 'storage'; 'buffers' points into it.
struct ScatterPlan {
  uint64_t offset;
  std::vector<folly::Range<char*>> buffers;
  std::vector<std::vector<char>> storage;
  uint64_t totalSize;
};

ScatterPlan makeScatterPlan(
    const std::vector<std::pair<uint64_t, uint64_t>>& spans) {
  ScatterPlan plan;
  plan.offset = spans.front().first;
  // Storage must not reallocate after buffers point into it.
  plan.storage.reserve(spans.size());
  for (const auto& span : spans) {
    plan.storage.emplace_back(span.second, '\0');
  }

  uint64_t cursor = plan.offset;
  for (size_t i = 0; i < spans.size(); ++i) {
    const auto [start, length] = spans[i];
    if (start > cursor) {
      const auto gap = start - cursor;
      // The gap encoding Velox uses: null data, size carried as the end
      // pointer. See cache::readPins and DirectCoalescedLoad::loadData.
      plan.buffers.emplace_back(
          static_cast<char*>(nullptr),
          reinterpret_cast<char*>(static_cast<uint64_t>(gap)));
      cursor += gap;
    }
    plan.buffers.emplace_back(plan.storage[i].data(), length);
    cursor += length;
  }
  plan.totalSize = cursor - plan.offset;
  return plan;
}

/// The multi-range shape with gaps is what a coalesced load produces and what a
/// plain table scan never exercises, so it gets a direct test.
TEST_F(KvikioS3ReadTest, preadvWithGaps) {
  const char* kBucket = "kvikio-preadv";
  const char* kKey = "blob.bin";
  constexpr size_t kBlobSize = 512 * 1024;
  const auto blob = makeBlob(kBlobSize);
  putObject(kBucket, kKey, blob);

  const auto s3Path = std::string(kBucket) + "/" + kKey;
  filesystems::S3Config s3Config(kBucket, hiveConfig_);

  KvikioS3ReadFile kvikioFile(s3Path, s3Config);
  kvikioFile.initialize({});
  ASSERT_EQ(kvikioFile.size(), kBlobSize);

  // Non-adjacent spans, so the buffer list carries real gaps -- the shape a
  // coalesced load produces and a plain table scan never exercises.
  const std::vector<std::pair<uint64_t, uint64_t>> spans{
      {1000, 4096}, {8192, 100}, {200000, 131072}};
  auto plan = makeScatterPlan(spans);

  const auto returned = kvikioFile.preadv(plan.offset, plan.buffers);

  // The return value must count gap bytes: ReadFileInputStream::read asserts
  // preadv() == totalBufferSize(buffers), which sums every range including
  // gaps.
  const auto expectedTotal = std::accumulate(
      plan.buffers.begin(),
      plan.buffers.end(),
      uint64_t{0},
      [](uint64_t acc, const auto& range) { return acc + range.size(); });
  EXPECT_EQ(returned, expectedTotal);
  EXPECT_EQ(returned, plan.totalSize);

  for (size_t i = 0; i < spans.size(); ++i) {
    const auto [start, length] = spans[i];
    const std::string got(plan.storage[i].data(), length);
    EXPECT_EQ(got, blob.substr(start, length))
        << "span " << i << " at offset " << start;
  }
}

TEST_F(KvikioS3ReadTest, preadvSingleRangeAndPread) {
  const char* kBucket = "kvikio-basic";
  const char* kKey = "blob.bin";
  constexpr size_t kBlobSize = 64 * 1024;
  const auto blob = makeBlob(kBlobSize);
  putObject(kBucket, kKey, blob);

  filesystems::S3Config s3Config(kBucket, hiveConfig_);
  KvikioS3ReadFile file(std::string(kBucket) + "/" + kKey, s3Config);
  file.initialize({});

  // pread into a caller buffer.
  std::string buf(1024, '\0');
  const auto view = file.pread(2048, 1024, buf.data());
  EXPECT_EQ(std::string(view), blob.substr(2048, 1024));

  // pread returning a string.
  EXPECT_EQ(file.pread(0, 16), blob.substr(0, 16));

  // Single-range preadv, the shape a small column chunk produces.
  std::vector<char> dest(4096, '\0');
  std::vector<folly::Range<char*>> buffers{
      folly::Range<char*>(dest.data(), dest.size())};
  EXPECT_EQ(file.preadv(100, buffers), dest.size());
  EXPECT_EQ(std::string(dest.data(), dest.size()), blob.substr(100, 4096));
}

TEST_F(KvikioS3ReadTest, fileSizeHintSkipsHead) {
  const char* kBucket = "kvikio-size-hint";
  const char* kKey = "blob.bin";
  constexpr size_t kBlobSize = 8192;
  putObject(kBucket, kKey, makeBlob(kBlobSize));

  filesystems::S3Config s3Config(kBucket, hiveConfig_);
  KvikioS3ReadFile file(std::string(kBucket) + "/" + kKey, s3Config);

  filesystems::FileOptions options;
  options.fileSize = kBlobSize;
  file.initialize(options);
  EXPECT_EQ(file.size(), kBlobSize);

  // initialize() is a no-op when invoked twice.
  file.initialize(options);
  EXPECT_EQ(file.size(), kBlobSize);
}

TEST_F(KvikioS3ReadTest, readsAreCountedForCacheAssertions) {
  const char* kBucket = "kvikio-counter";
  const char* kKey = "blob.bin";
  putObject(kBucket, kKey, makeBlob(4096));

  filesystems::S3Config s3Config(kBucket, hiveConfig_);
  KvikioS3ReadFile file(std::string(kBucket) + "/" + kKey, s3Config);
  file.initialize({});

  EXPECT_EQ(file.testingNumReads(), 0);
  EXPECT_EQ(file.pread(0, 128).size(), 128);
  EXPECT_EQ(file.testingNumReads(), 1);
  EXPECT_EQ(file.pread(128, 128).size(), 128);
  EXPECT_EQ(file.testingNumReads(), 2);
}

/// Registration is last-writer-wins until the first S3 file is opened, so the
/// KvikIO factory only survives if it is installed after any plain
/// registerS3FileSystem(). This mirrors prestissimo's ordering, where
/// PrestoServer::registerFileSystems() runs before registerVeloxCudf().
TEST_F(KvikioS3ReadTest, kvikioFactoryWinsWhenRegisteredLast) {
  const char* kBucket = "kvikio-registry";
  const char* kKey = "blob.bin";
  putObject(kBucket, kKey, makeBlob(1024));

  auto& cudfConfig = cudf_velox::CudfConfig::getInstance();
  const auto savedFlag = cudfConfig.s3UseKvikio;
  cudfConfig.s3UseKvikio = true;
  SCOPE_EXIT {
    cudfConfig.s3UseKvikio = savedFlag;
  };

  filesystems::registerS3FileSystem();
  registerKvikioS3FileSystem();

  const auto uri = filesystems::s3URI(kBucket, kKey);
  auto fs = filesystems::getFileSystem(uri, hiveConfig_);
  ASSERT_EQ(fs->name(), "KvikioS3");

  auto readFile = fs->openFileForRead(uri);
  EXPECT_EQ(readFile->size(), 1024);
  EXPECT_EQ(readFile->pread(0, 8), makeBlob(1024).substr(0, 8));
}

TEST_F(KvikioS3ReadTest, disabledFlagLeavesAwsSdkInPlace) {
  const char* kBucket = "kvikio-disabled";
  const char* kKey = "blob.bin";
  putObject(kBucket, kKey, makeBlob(1024));

  auto& cudfConfig = cudf_velox::CudfConfig::getInstance();
  const auto savedFlag = cudfConfig.s3UseKvikio;
  cudfConfig.s3UseKvikio = false;
  SCOPE_EXIT {
    cudfConfig.s3UseKvikio = savedFlag;
  };

  filesystems::registerS3FileSystem();
  // No-op: the flag is off.
  registerKvikioS3FileSystem();

  auto fs =
      filesystems::getFileSystem(filesystems::s3URI(kBucket, kKey), hiveConfig_);
  EXPECT_EQ(fs->name(), "S3");
}

/// A missing region must be reported once, at filesystem construction, with an
/// actionable message -- not per split as a raw KvikIO std::invalid_argument.
/// Exercised through validateKvikioS3Config directly so the test needs no
/// initialized AWS SDK.
TEST_F(KvikioS3ReadTest, missingRegionIsReportedEagerly) {
  const std::string savedDefaultRegion =
      std::getenv("AWS_DEFAULT_REGION") ? std::getenv("AWS_DEFAULT_REGION") : "";
  const std::string savedRegion =
      std::getenv("AWS_REGION") ? std::getenv("AWS_REGION") : "";
  ::unsetenv("AWS_DEFAULT_REGION");
  ::unsetenv("AWS_REGION");
  SCOPE_EXIT {
    if (!savedDefaultRegion.empty()) {
      ::setenv("AWS_DEFAULT_REGION", savedDefaultRegion.c_str(), 1);
    }
    if (!savedRegion.empty()) {
      ::setenv("AWS_REGION", savedRegion.c_str(), 1);
    }
  };

  // minioServer_->hiveConfig() supplies keys but no region, and the endpoint is
  // a bare host:port from which no AWS region can be inferred.
  filesystems::S3Config configWithoutRegion(
      "some-bucket", minioServer_->hiveConfig());
  VELOX_ASSERT_THROW(
      validateKvikioS3Config(configWithoutRegion),
      "KvikIO S3 reads require an AWS region");

  // With a region present, validation passes.
  filesystems::S3Config configWithRegion("some-bucket", hiveConfig_);
  EXPECT_NO_THROW(validateKvikioS3Config(configWithRegion));
}

} // namespace
