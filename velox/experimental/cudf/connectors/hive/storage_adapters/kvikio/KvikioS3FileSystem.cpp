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

#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/KvikioS3ReadFile.h"

namespace facebook::velox::cudf_velox::connector::hive {

KvikioS3FileSystem::KvikioS3FileSystem(
    std::string_view bucketName,
    std::shared_ptr<const config::ConfigBase> config)
    : filesystems::S3FileSystem(bucketName, config),
      s3Config_(bucketName, config) {
  // Fail once here rather than once per split: KvikIO resolves credentials at
  // file-open time and reports missing ones as a raw std::invalid_argument.
  validateKvikioS3Config(s3Config_);
}

std::string KvikioS3FileSystem::name() const {
  return "KvikioS3";
}

std::unique_ptr<ReadFile> KvikioS3FileSystem::openFileForRead(
    std::string_view s3Path,
    const filesystems::FileOptions& options) {
  const auto path = filesystems::getPath(s3Path);
  auto file = std::make_unique<KvikioS3ReadFile>(path, s3Config_);
  file->initialize(options);
  return file;
}

} // namespace facebook::velox::cudf_velox::connector::hive
