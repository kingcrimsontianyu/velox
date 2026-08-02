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

#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/KvikioS3ReadFile.h"

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

#ifdef KVIKIO_LIBCURL_FOUND
#include <kvikio/remote_handle.hpp>
#endif

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>

namespace facebook::velox::cudf_velox::connector::hive {

namespace {

std::optional<std::string> envOpt(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  return std::string(value);
}

/// The region has to be resolved eagerly rather than left to KvikIO, because it
/// is part of the virtual-hosted URL we compose below.
std::optional<std::string> resolveRegion(const filesystems::S3Config& config) {
  if (auto region = config.endpointRegion(); region.has_value()) {
    return region;
  }
  if (auto region = envOpt("AWS_DEFAULT_REGION"); region.has_value()) {
    return region;
  }
  return envOpt("AWS_REGION");
}

std::optional<std::string> resolveAccessKey(
    const filesystems::S3Config& config) {
  if (auto key = config.accessKey(); key.has_value()) {
    return key;
  }
  return envOpt("AWS_ACCESS_KEY_ID");
}

std::optional<std::string> resolveSecretKey(
    const filesystems::S3Config& config) {
  if (auto key = config.secretKey(); key.has_value()) {
    return key;
  }
  return envOpt("AWS_SECRET_ACCESS_KEY");
}

#ifdef KVIKIO_LIBCURL_FOUND

bool hasScheme(const std::string& endpoint) {
  return endpoint.rfind("http://", 0) == 0 || endpoint.rfind("https://", 0) == 0;
}

/// Composes the object URL by hand rather than using KvikIO's
/// S3Endpoint::url_from_bucket_and_object, which hard-couples "endpoint set" to
/// path-style addressing and cannot express virtual-hosted access against a
/// custom endpoint.
///
/// Note useVirtualAddressing() is the negation of hive.s3.path-style-access.
std::string composeUrl(
    const std::string& bucket,
    const std::string& key,
    const filesystems::S3Config& config,
    const std::optional<std::string>& region) {
  const std::string scheme = config.useSSL() ? "https" : "http";
  const auto endpoint = config.endpoint();

  if (!endpoint.has_value() || endpoint.value().empty()) {
    VELOX_USER_CHECK(
        region.has_value(),
        "KvikIO S3 reads require a region when hive.s3.endpoint is unset. "
        "Set hive.s3.endpoint.region or the AWS_DEFAULT_REGION environment "
        "variable.");
    return fmt::format(
        "{}://{}.s3.{}.amazonaws.com/{}", scheme, bucket, region.value(), key);
  }

  const auto& host = endpoint.value();
  if (hasScheme(host)) {
    // Endpoint already carries a scheme; honor it and ignore hive.s3.ssl.enabled
    // so the two cannot disagree.
    const auto sep = host.find("://");
    const auto endpointScheme = host.substr(0, sep);
    const auto endpointHost = host.substr(sep + 3);
    return config.useVirtualAddressing()
        ? fmt::format("{}://{}.{}/{}", endpointScheme, bucket, endpointHost, key)
        : fmt::format("{}://{}/{}/{}", endpointScheme, endpointHost, bucket, key);
  }

  return config.useVirtualAddressing()
      ? fmt::format("{}://{}.{}/{}", scheme, bucket, host, key)
      : fmt::format("{}://{}/{}/{}", scheme, host, bucket, key);
}

#endif // KVIKIO_LIBCURL_FOUND

} // namespace

void validateKvikioS3Config(const filesystems::S3Config& config) {
  const auto region = resolveRegion(config);
  VELOX_USER_CHECK(
      region.has_value(),
      "KvikIO S3 reads require an AWS region. Set hive.s3.endpoint.region or "
      "the AWS_DEFAULT_REGION environment variable.");

  const auto accessKey = resolveAccessKey(config);
  VELOX_USER_CHECK(
      accessKey.has_value(),
      "KvikIO S3 reads require an access key. Set hive.s3.aws-access-key or "
      "the AWS_ACCESS_KEY_ID environment variable.");

  VELOX_USER_CHECK(
      resolveSecretKey(config).has_value(),
      "KvikIO S3 reads require a secret key. Set hive.s3.aws-secret-key or "
      "the AWS_SECRET_ACCESS_KEY environment variable.");

  // STS temporary credentials are unusable without their session token, and
  // this is the documented credential flow for these benchmarks
  // (aws configure export-credentials issues ASIA* keys).
  if (accessKey.value().rfind("ASIA", 0) == 0) {
    VELOX_USER_CHECK(
        envOpt("AWS_SESSION_TOKEN").has_value(),
        "Access key '{}...' is an STS temporary credential but "
        "AWS_SESSION_TOKEN is not set. Re-run "
        "'eval \"$(aws configure export-credentials --format env)\"'.",
        accessKey.value().substr(0, 8));
  }
}

#ifdef KVIKIO_LIBCURL_FOUND

class KvikioS3ReadFile::Impl {
 public:
  Impl(std::string_view path, const filesystems::S3Config& config) {
    filesystems::getBucketAndKeyFromPath(path, bucket_, key_);
    const auto region = resolveRegion(config);
    url_ = composeUrl(bucket_, key_, config, region);
    region_ = region;
    accessKey_ = resolveAccessKey(config);
    secretKey_ = resolveSecretKey(config);
    sessionToken_ = envOpt("AWS_SESSION_TOKEN");
  }

  void initialize(const filesystems::FileOptions& options) {
    // Make it a no-op if invoked twice.
    if (handle_ != nullptr) {
      return;
    }

    auto endpoint = std::make_unique<kvikio::S3Endpoint>(
        url_, region_, accessKey_, secretKey_, sessionToken_);

    if (options.fileSize.has_value()) {
      VELOX_CHECK_GE(
          options.fileSize.value(), 0, "File size must be non-negative");
      // Skips the HEAD request that RemoteHandle would otherwise issue.
      handle_ = std::make_unique<kvikio::RemoteHandle>(
          std::move(endpoint),
          static_cast<std::size_t>(options.fileSize.value()));
    } else {
      handle_ = std::make_unique<kvikio::RemoteHandle>(std::move(endpoint));
    }
    length_ = static_cast<int64_t>(handle_->nbytes());
    VELOX_CHECK_GE(length_, 0);
    VLOG(1) << "KvikIO S3 read file opened: " << url_ << " (" << length_
            << " bytes)";
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      const FileIoContext& /*context*/) const {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& /*context*/) const {
    std::string result(length, 0);
    preadInternal(offset, length, result.data());
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& /*context*/) const {
    // Deliberately mirrors S3ReadFile::preadv byte for byte, including the
    // whole-span bounce buffer and the scatter memcpy, so that this change is a
    // pure transport swap and any measured delta is attributable to KvikIO
    // rather than to a different request shape.
    //
    // 'buffers' covers one contiguous span starting at 'offset'; ranges with a
    // null data pointer are gaps that must be fetched (they are part of the
    // span) but not copied out. The return value counts them.
    size_t length = 0;
    for (const auto& range : buffers) {
      length += range.size();
    }
    std::string result(length, 0);
    preadInternal(offset, length, result.data());
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data() != nullptr) {
        std::memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }
    return length;
  }

  uint64_t size() const {
    return length_;
  }

  uint64_t memoryUsage() const {
    return sizeof(kvikio::RemoteHandle) + url_.size() + bucket_.size() +
        key_.size();
  }

  bool shouldCoalesce() const {
    return false;
  }

  std::string getName() const {
    return fmt::format("s3://{}/{}", bucket_, key_);
  }

  uint64_t testingNumReads() const {
    return numReads_.load(std::memory_order_relaxed);
  }

 private:
  /// The assumption here is that 'position' has space for at least 'length'
  /// bytes.
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    VELOX_CHECK_NOT_NULL(
        handle_, "KvikioS3ReadFile::initialize() was not called");
    if (length == 0) {
      return;
    }
    numReads_.fetch_add(1, std::memory_order_relaxed);

    // pread() splits the range into KVIKIO_TASK_SIZE sub-requests fetched in
    // parallel. The future is consumed in scope so it never outlives the
    // handle, as its contract requires.
    const auto bytesRead = handle_->pread(position, length, offset).get();
    VELOX_CHECK_EQ(
        bytesRead,
        length,
        "Should read exactly as requested. File: {}, offset: {}, length: {}, read: {}",
        url_,
        offset,
        length,
        bytesRead);
  }

  std::string bucket_;
  std::string key_;
  std::string url_;
  std::optional<std::string> region_;
  std::optional<std::string> accessKey_;
  std::optional<std::string> secretKey_;
  std::optional<std::string> sessionToken_;
  // RemoteHandle is move-only. Concurrent reads on one handle are safe: each
  // call builds its own curl handle and S3Endpoint::setopt only reads state
  // fixed at construction.
  std::unique_ptr<kvikio::RemoteHandle> handle_;
  int64_t length_ = -1;
  mutable std::atomic<uint64_t> numReads_{0};
};

#else // !KVIKIO_LIBCURL_FOUND

/// KvikIO was built without remote I/O support. registerKvikioS3FileSystem()
/// fails loudly before anything can construct this, so these are unreachable;
/// they exist only so the translation unit links.
class KvikioS3ReadFile::Impl {
 public:
  Impl(std::string_view, const filesystems::S3Config&) {
    VELOX_UNSUPPORTED(
        "KvikIO was built without remote I/O support (KvikIO_REMOTE_SUPPORT=OFF)");
  }
  void initialize(const filesystems::FileOptions&) {}
  std::string_view pread(uint64_t, uint64_t, void*, const FileIoContext&) const {
    return {};
  }
  std::string pread(uint64_t, uint64_t, const FileIoContext&) const {
    return {};
  }
  uint64_t preadv(
      uint64_t,
      const std::vector<folly::Range<char*>>&,
      const FileIoContext&) const {
    return 0;
  }
  uint64_t size() const {
    return 0;
  }
  uint64_t memoryUsage() const {
    return 0;
  }
  bool shouldCoalesce() const {
    return false;
  }
  std::string getName() const {
    return {};
  }
  uint64_t testingNumReads() const {
    return 0;
  }
};

#endif // KVIKIO_LIBCURL_FOUND

KvikioS3ReadFile::KvikioS3ReadFile(
    std::string_view path,
    const filesystems::S3Config& s3Config) {
  impl_ = std::make_shared<Impl>(path, s3Config);
}

KvikioS3ReadFile::~KvikioS3ReadFile() = default;

void KvikioS3ReadFile::initialize(const filesystems::FileOptions& options) {
  return impl_->initialize(options);
}

std::string_view KvikioS3ReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext& context) const {
  return impl_->pread(offset, length, buf, context);
}

std::string KvikioS3ReadFile::pread(
    uint64_t offset,
    uint64_t length,
    const FileIoContext& context) const {
  return impl_->pread(offset, length, context);
}

uint64_t KvikioS3ReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileIoContext& context) const {
  return impl_->preadv(offset, buffers, context);
}

uint64_t KvikioS3ReadFile::size() const {
  return impl_->size();
}

uint64_t KvikioS3ReadFile::memoryUsage() const {
  return impl_->memoryUsage();
}

bool KvikioS3ReadFile::shouldCoalesce() const {
  return impl_->shouldCoalesce();
}

std::string KvikioS3ReadFile::getName() const {
  return impl_->getName();
}

uint64_t KvikioS3ReadFile::testingNumReads() const {
  return impl_->testingNumReads();
}

} // namespace facebook::velox::cudf_velox::connector::hive
