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

#pragma once

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {
class S3Config;
}

namespace facebook::velox::cudf_velox::connector::hive {

/// Resolves the AWS region and credentials from 's3Config' and the AWS_*
/// environment, throwing VELOX_USER_FAIL naming the missing variable.
///
/// Called once when the filesystem is created so that a misconfiguration fails
/// at startup with an actionable message, rather than surfacing as a raw KvikIO
/// std::invalid_argument once per split in the middle of a query.
void validateKvikioS3Config(const filesystems::S3Config& s3Config);

/// An S3 ReadFile that fetches bytes with KvikIO's libcurl engine instead of
/// the AWS SDK.
///
/// This is a drop-in replacement for velox::filesystems::S3ReadFile with
/// identical request shape: preadv() issues a single spanning read covering all
/// ranges and scatters into the non-null ones, exactly as S3ReadFile does. Only
/// the transport differs. See KvikioS3ReadFile.cpp for why the request shape is
/// preserved rather than optimized.
///
/// Reads are the only S3 operation routed through KvikIO; every other
/// filesystem operation still uses the AWS SDK (see KvikioS3FileSystem).
///
/// Thread safety: concurrent const method calls are safe. Velox opens one
/// ReadFile per file and issues one I/O task per column chunk, so many
/// connector-IO threads call preadv() on the same instance simultaneously.
class KvikioS3ReadFile : public ReadFile {
 public:
  /// 'path' is an S3 path without the scheme (as produced by
  /// filesystems::getPath), e.g. "bucket/key".
  KvikioS3ReadFile(
      std::string_view path,
      const filesystems::S3Config& s3Config);

  ~KvikioS3ReadFile() override;

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const final;

  std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& context = {}) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final;

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

  /// Resolves the file size. Uses options.fileSize when present, otherwise
  /// issues a HEAD via KvikIO. Idempotent.
  void initialize(const filesystems::FileOptions& options);

  /// Number of reads that reached KvikIO on this file. Used by tests to prove
  /// that cache hits never reach the transport.
  uint64_t testingNumReads() const;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::cudf_velox::connector::hive
