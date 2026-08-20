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

#include "velox/experimental/cudf/CudfNoDefaults.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnectorSplit.h"

#include <cudf/io/types.hpp>

#include <folly/Conv.h>

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace facebook::velox::cudf_velox::connector::hive {

namespace {
std::string stripFilePrefix(const std::string& targetPath) {
  const std::string prefix = "file:";
  if (targetPath.rfind(prefix, 0) == 0) {
    return targetPath.substr(prefix.length());
  }
  return targetPath;
}

// Builds a cuDF source_info for 'path', threading through a known file size
// when one is available (via the synthesized "$file_size" info column, e.g.
// populated from the Presto coordinator's S3 LIST). This lets
// remote_file_source skip its blocking HEAD request to query the file size.
std::unique_ptr<cudf::io::source_info> makeCudfSourceInfo(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& infoColumns) {
  std::optional<std::size_t> knownSize;
  if (auto it = infoColumns.find("$file_size"); it != infoColumns.end()) {
    try {
      knownSize = folly::to<std::size_t>(it->second);
    } catch (const std::exception&) {
      // Fall back to no size hint if the value can't be parsed.
    }
  }
  return std::make_unique<cudf::io::source_info>(
      std::vector<cudf::io::filepath_source>{
          cudf::io::filepath_source{path, knownSize}});
}
} // namespace

std::string CudfHiveConnectorSplit::toString() const {
  return fmt::format("CudfHive: {}", filePath);
}

std::string CudfHiveConnectorSplit::getFileName() const {
  const auto i = filePath.rfind('/');
  return i == std::string::npos ? filePath : filePath.substr(i + 1);
}

uint64_t CudfHiveConnectorSplit::size() const {
  return length;
}

CudfHiveConnectorSplit::CudfHiveConnectorSplit(
    const std::string& connectorId,
    const std::string& _filePath,
    uint64_t _start,
    uint64_t _length,
    int64_t _splitWeight,
    const std::unordered_map<std::string, std::string>& _infoColumns)
    : facebook::velox::connector::ConnectorSplit(connectorId, _splitWeight),
      filePath(stripFilePrefix(_filePath)),
      start(_start),
      length(_length),
      cudfSourceInfo(makeCudfSourceInfo(filePath, _infoColumns)),
      infoColumns(_infoColumns) {}

// static
std::shared_ptr<CudfHiveConnectorSplit> CudfHiveConnectorSplit::create(
    const folly::dynamic& obj) {
  const auto connectorId = obj["connectorId"].asString();
  const auto filePath = obj["filePath"].asString();
  const auto start = static_cast<uint64_t>(obj["start"].asInt());
  const auto length = static_cast<uint64_t>(obj["length"].asInt());
  const auto splitWeight = obj["splitWeight"].asInt();

  std::unordered_map<std::string, std::string> infoColumns;
  for (const auto& [key, value] : obj["infoColumns"].items()) {
    infoColumns[key.asString()] = value.asString();
  }

  return std::make_shared<CudfHiveConnectorSplit>(
      connectorId, filePath, start, length, splitWeight, infoColumns);
}

folly::dynamic CudfHiveConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["connectorId"] = connectorId;
  obj["filePath"] = filePath;
  obj["start"] = start;
  obj["length"] = length;
  obj["splitWeight"] = splitWeight;

  folly::dynamic infoColumnsObj = folly::dynamic::object;
  for (const auto& [key, value] : infoColumns) {
    infoColumnsObj[key] = value;
  }
  obj["infoColumns"] = infoColumnsObj;

  return obj;
}

} // namespace facebook::velox::cudf_velox::connector::hive
