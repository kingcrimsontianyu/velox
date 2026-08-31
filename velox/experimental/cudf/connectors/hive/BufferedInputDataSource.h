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
#include "velox/dwio/common/BufferedInput.h"

#include <cudf/io/datasource.hpp>

#include <rmm/cuda_stream_view.hpp>

#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace facebook::velox::cudf_velox::connector::hive {

/**
 * @brief Converts Hive-compatible S3 URI schemes to the scheme KvikIO accepts
 *
 * Velox continues to use the original split path for filesystem and metadata
 * operations. This conversion is only for constructing a cuDF/KvikIO data
 * source.
 */
std::string normalizeKvikioUri(std::string_view path);

// A cudf::io::datasource that serves bytes via Velox BufferedInput so that
// reads benefit from AsyncDataCache / SSD cache and are always returned as
// contiguous buffers.
class BufferedInputDataSource : public cudf::io::datasource,
                                public cudf::io::device_read_batch_source {
 public:
  explicit BufferedInputDataSource(
      std::shared_ptr<facebook::velox::dwio::common::BufferedInput> input,
      std::shared_ptr<facebook::velox::IoStats> ioStats = nullptr);

  [[nodiscard]] size_t size() const override;

  std::unique_ptr<datasource::buffer> host_read(size_t offset, size_t size)
      override;

  size_t host_read(size_t offset, size_t size, uint8_t* dst) override;

  std::future<std::unique_ptr<datasource::buffer>> host_read_async(
      size_t offset,
      size_t size) override;

  std::future<size_t> host_read_async(size_t offset, size_t size, uint8_t* dst)
      override;

  [[nodiscard]] bool supports_device_read() const override;

  std::unique_ptr<datasource::buffer> device_read(
      size_t offset,
      size_t size,
      rmm::cuda_stream_view stream) override;

  size_t device_read(
      size_t offset,
      size_t size,
      uint8_t* dst,
      rmm::cuda_stream_view stream) override;

  std::future<size_t> device_read_async(
      size_t offset,
      size_t size,
      uint8_t* dst,
      rmm::cuda_stream_view stream) override;

  std::future<std::vector<size_t>> device_read_batch_async(
      cudf::host_span<cudf::io::datasource::device_read_request const> requests,
      rmm::cuda_stream_view stream) override;

 private:
  void readContiguous(size_t offset, size_t size, uint8_t* dst);

  std::shared_ptr<facebook::velox::dwio::common::BufferedInput> input_;
  std::shared_ptr<facebook::velox::IoStats> ioStats_;
  const size_t fileSize_;
  // BufferedInput enqueue/load state is mutable and must not be interleaved by
  // concurrent datasource reads. The shared ownership lets asynchronous work
  // finish safely after the datasource itself is released.
  std::shared_ptr<std::mutex> inputMutex_{std::make_shared<std::mutex>()};
};

} // namespace facebook::velox::cudf_velox::connector::hive
