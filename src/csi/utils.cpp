// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "csi/utils.hpp"

#include <algorithm>

#include <google/protobuf/util/json_util.h>

#include <stout/strings.hpp>

using std::equal;
using std::ostream;
using std::string;

using google::protobuf::util::MessageToJsonString;

namespace csi {

bool operator==(const csi::Version& left, const csi::Version& right)
{
  return left.major() == right.major() &&
         left.minor() == right.minor() &&
         left.patch() == right.patch();
}


bool operator==(
    const VolumeCapability::BlockVolume& left,
    const VolumeCapability::BlockVolume& right)
{
  return true;
}


bool operator==(
    const VolumeCapability::MountVolume& left,
    const VolumeCapability::MountVolume& right)
{
  return left.fs_type() == right.fs_type() &&
         left.mount_flags().size() == right.mount_flags().size() &&
         equal(
             left.mount_flags().begin(),
             left.mount_flags().end(),
             right.mount_flags().begin());
}


bool operator==(const VolumeCapability& left, const VolumeCapability& right)
{
  if (left.has_block() && right.has_block()) {
    return left.block() == right.block();
  } else if (left.has_mount() && right.has_mount()) {
    return left.mount() == right.mount();
  }

  return false;
}


ostream& operator<<(ostream& stream, const csi::Version& version)
{
  return stream << strings::join(
      ".",
      version.major(),
      version.minor(),
      version.patch());
}


ostream& operator<<(
    ostream& stream,
    const GetPluginInfoResponse::Result& result)
{
  // NOTE: We use Google's JSON utility functions for proto3.
  string output;
  MessageToJsonString(result, &output);
  return stream << output;
}


ostream& operator<<(ostream& stream, const VolumeID& volumeId)
{
  // NOTE: We use Google's JSON utility functions for proto3.
  // TODO(chhsiao): Output the ID string once we switch to the lastest
  // CSI spec.
  string output;
  MessageToJsonString(volumeId, &output);
  return stream << output;
}

} // namespace csi {


namespace mesos {
namespace csi {

Labels volumeMetadataToLabels(const VolumeMetadata& metadata)
{
  Labels labels;

  foreach (const auto& value, metadata.values()) {
    Label* label = labels.mutable_labels()->Add();
    label->set_key(value.first);
    label->set_value(value.second);
  }

  return labels;
}


Try<VolumeMetadata> labelsToVolumeMetadata(const Labels& labels)
{
  VolumeMetadata metadata;

  foreach (const Label& label, labels.labels()) {
    if (metadata.values().count(label.key())) {
      return ::Error("Repeated key '" + label.key() + "' in labels");
    }
    (*metadata.mutable_values())[label.key()] = label.value();
  }

  return metadata;
}

} // namespace csi {
} // namespace mesos {
