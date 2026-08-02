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

#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/RegisterKvikioS3FileSystem.h"

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/cudf/CudfConfig.h"

#include <memory>
#include <string>

#ifdef VELOX_ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/experimental/cudf/connectors/hive/storage_adapters/kvikio/KvikioS3FileSystem.h"

#include <glog/logging.h>
#endif

namespace facebook::velox::cudf_velox::connector::hive {

#ifdef VELOX_ENABLE_S3
namespace {
std::shared_ptr<filesystems::FileSystem> kvikioS3FileSystemFactory(
    std::string bucketName,
    std::shared_ptr<const config::ConfigBase> config) {
  return std::make_shared<KvikioS3FileSystem>(bucketName, config);
}
} // namespace
#endif

void registerKvikioS3FileSystem() {
  if (!CudfConfig::getInstance().s3UseKvikio) {
    return;
  }

#ifdef VELOX_ENABLE_S3
#ifndef KVIKIO_LIBCURL_FOUND
  // Falling back to the AWS SDK here would be a silent performance regression,
  // which is the failure mode this feature exists to eliminate.
  VELOX_FAIL(
      "{}=true, but KvikIO was built without remote I/O support "
      "(KvikIO_REMOTE_SUPPORT=OFF or libcurl missing). Rebuild with remote "
      "support or unset the property.",
      CudfConfig::kCudfS3UseKvikio);
#else
  filesystems::registerS3FileSystem(
      /*cacheKeyFunc=*/nullptr, kvikioS3FileSystemFactory);
  LOG(INFO) << "S3 reads will be served by KvikIO ("
            << CudfConfig::kCudfS3UseKvikio << "=true).";
#endif
#else
  VELOX_FAIL(
      "{}=true, but Velox was built without S3 support (VELOX_ENABLE_S3=OFF).",
      CudfConfig::kCudfS3UseKvikio);
#endif
}

} // namespace facebook::velox::cudf_velox::connector::hive
