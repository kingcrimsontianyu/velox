# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(
  velox_cudf_kvikio_s3fs
  KvikioS3FileSystem.cpp
  KvikioS3ReadFile.cpp
  RegisterKvikioS3FileSystem.cpp
)

target_link_libraries(
  velox_cudf_kvikio_s3fs
  PRIVATE velox_dwio_common velox_s3fs
  # cudf::cudf is needed for CudfConfig.h, which includes <cudf/types.hpp>.
  PUBLIC cudf::cudf kvikio::kvikio velox_file
)

# S3Util.h (needed for getPath/getBucketAndKeyFromPath) includes AWS SDK
# headers, but velox_s3fs keeps AWSSDK_INCLUDE_DIRS PRIVATE so they do not
# propagate to us.
target_include_directories(velox_cudf_kvikio_s3fs PRIVATE ${AWSSDK_INCLUDE_DIRS})
