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

#ifndef __CSI_UTILS_HPP__
#define __CSI_UTILS_HPP__

#include <ostream>

#include <stout/foreach.hpp>
#include <stout/unreachable.hpp>

#include "csi/spec.hpp"

namespace csi {

bool operator==(const csi::Version& left, const csi::Version& right);
std::ostream& operator<<(std::ostream& stream, const csi::Version& version);

} // namespace csi {


namespace mesos {
namespace csi {

struct ControllerCapabilities
{
  ControllerCapabilities() = default;

  template <typename Iterable>
  ControllerCapabilities(const Iterable& capabilities)
  {
    foreach (const auto& capability, capabilities) {
      if (capability.has_rpc() &&
          ControllerServiceCapability::RPC::Type_IsValid(
              capability.rpc().type())) {
        switch(capability.rpc().type()) {
          case ControllerServiceCapability::RPC::UNKNOWN:
            break;
          case ControllerServiceCapability::RPC::CREATE_DELETE_VOLUME:
            createDeleteVolume = true;
            break;
          case ControllerServiceCapability::RPC::PUBLISH_UNPUBLISH_VOLUME:
            publishUnpublishVolume = true;
            break;
          case ControllerServiceCapability::RPC::LIST_VOLUMES:
            listVolumes = true;
            break;
          case ControllerServiceCapability::RPC::GET_CAPACITY:
            getCapacity = true;
            break;
          case google::protobuf::kint32min:
          case google::protobuf::kint32max:
            UNREACHABLE();
        }
      } else if (capability.volume_capability().has_block()) {
        blockVolumes.Add()->CopyFrom(capability.volume_capability());
      } else {
        mountVolumes.Add()->CopyFrom(capability.volume_capability());
      }
    }
  }

  bool createDeleteVolume = false;
  bool publishUnpublishVolume = false;
  bool listVolumes = false;
  bool getCapacity = false;

  // TODO(chhsiao): Remove these fields once we update the CSI spec.
  google::protobuf::RepeatedPtrField<VolumeCapability> blockVolumes;
  google::protobuf::RepeatedPtrField<VolumeCapability> mountVolumes;
};


// TODO(chhsiao): Remove this structure once we update the CSI spec.
struct NodeCapabilities
{
  NodeCapabilities() = default;

  template <typename Iterable>
  NodeCapabilities(const Iterable& capabilities)
  {
    foreach (const auto& capability, capabilities) {
      if (capability.has_rpc() &&
          NodeServiceCapability::RPC::Type_IsValid(capability.rpc().type())) {
        switch(capability.rpc().type()) {
          case NodeServiceCapability::RPC::UNKNOWN:
            break;
          case google::protobuf::kint32min:
          case google::protobuf::kint32max:
            UNREACHABLE();
        }
      } else if (capability.volume_capability().has_block()) {
        blockVolumes.Add()->CopyFrom(capability.volume_capability());
      } else {
        mountVolumes.Add()->CopyFrom(capability.volume_capability());
      }
    }
  }

  google::protobuf::RepeatedPtrField<VolumeCapability> blockVolumes;
  google::protobuf::RepeatedPtrField<VolumeCapability> mountVolumes;
};

} // namespace csi {
} // namespace mesos {

#endif // __CSI_UTILS_HPP__
