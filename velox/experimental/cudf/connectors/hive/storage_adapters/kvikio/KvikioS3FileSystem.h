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

#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

namespace facebook::velox::cudf_velox::connector::hive {

/// An S3FileSystem whose reads go through KvikIO instead of the AWS SDK.
///
/// Only openFileForRead() is overridden; every other operation
/// (openFileForWrite, exists, list, rename, mkdir, remove, rmdir) is inherited
/// from S3FileSystem and still uses the AWS SDK. The base constructor also
/// still builds the S3Client, so credentials, retry and endpoint configuration
/// behave exactly as before.
///
/// Subclassing rather than decorating is deliberate: it is the pattern Velox
/// itself uses for the S3FileSystemFactory seam (see S3FileSystemRegistrationTest),
/// and it means a method added to the FileSystem base later keeps working
/// instead of falling through to VELOX_NYI.
class KvikioS3FileSystem : public filesystems::S3FileSystem {
 public:
  KvikioS3FileSystem(
      std::string_view bucketName,
      std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view s3Path,
      const filesystems::FileOptions& options = {}) override;

 private:
  const filesystems::S3Config s3Config_;
};

} // namespace facebook::velox::cudf_velox::connector::hive
