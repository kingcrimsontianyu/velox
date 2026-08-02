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

namespace facebook::velox::cudf_velox::connector::hive {

/// Routes S3 reads through KvikIO by installing a KvikioS3FileSystem factory on
/// Velox's S3FileSystemFactory seam. No-op unless
/// CudfConfig::getInstance().s3UseKvikio is set, and unless Velox was built
/// with S3 support.
///
/// Registration order matters: registerS3FileSystem() overwrites the factory on
/// every call made before the first S3 file is opened, so this must run after
/// any plain registerS3FileSystem(). In prestissimo that holds -- the only such
/// call is PrestoServer::registerFileSystems(), which runs before
/// registerVeloxCudf(). Tests must check the ordering themselves.
///
/// Called from registerCudf().
void registerKvikioS3FileSystem();

} // namespace facebook::velox::cudf_velox::connector::hive
