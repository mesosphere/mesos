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

#include <functional>

#include <process/defer.hpp>

#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>

#include "csi/client.hpp"

using process::Failure;
using process::Future;

using process::defer;

namespace mesos {
namespace csi {

static inline bool recoverable(const csi::Error& error)
{
  if (error.has_create_volume_error()) {
    if (error.create_volume_error().error_code() ==
        csi::Error::CreateVolumeError::VOLUME_ALREADY_EXISTS) {
      return true;
    }
  } else if (error.has_delete_volume_error()) {
    if (error.delete_volume_error().error_code() ==
        csi::Error::DeleteVolumeError::VOLUME_DOES_NOT_EXIST) {
      return true;
    }
  }

  return false;
}


template <typename Response>
static inline Future<Response> validate(const Future<Response>& response)
{
  return response.then(defer([](const Response& response) -> Future<Response> {
    // TODO(chhsiao): Validate the required fields of the response.
    if (response.has_error() && !recoverable(response.error())) {
      return Failure(response.GetDescriptor()->name() + " error: " +
                     stringify(JSON::protobuf(response.error())));
    }

    CHECK(response.has_result());

    return response;
  }));
}


Future<GetSupportedVersionsResponse> Client::GetSupportedVersions(
    const GetSupportedVersionsRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Identity, GetSupportedVersions),
      request));
}


Future<GetPluginInfoResponse> Client::GetPluginInfo(
    const GetPluginInfoRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Identity, GetPluginInfo),
      request));
}


Future<CreateVolumeResponse> Client::CreateVolume(
    const CreateVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, CreateVolume),
      request));
}


Future<DeleteVolumeResponse> Client::DeleteVolume(
    const DeleteVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, DeleteVolume),
      request));
}


Future<ControllerPublishVolumeResponse> Client::ControllerPublishVolume(
    const ControllerPublishVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, ControllerPublishVolume),
      request));
}


Future<ControllerUnpublishVolumeResponse> Client::ControllerUnpublishVolume(
    const ControllerUnpublishVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, ControllerUnpublishVolume),
      request));
}


Future<ValidateVolumeCapabilitiesResponse> Client::ValidateVolumeCapabilities(
    const ValidateVolumeCapabilitiesRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, ValidateVolumeCapabilities),
      request));
}


Future<ListVolumesResponse> Client::ListVolumes(
    const ListVolumesRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, ListVolumes),
      request));
}


Future<GetCapacityResponse> Client::GetCapacity(
    const GetCapacityRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, GetCapacity),
      request));
}


Future<ControllerGetCapabilitiesResponse> Client::ControllerGetCapabilities(
    const ControllerGetCapabilitiesRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Controller, ControllerGetCapabilities),
      request));
}


Future<NodePublishVolumeResponse> Client::NodePublishVolume(
    const NodePublishVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Node, NodePublishVolume),
      request));
}


Future<NodeUnpublishVolumeResponse> Client::NodeUnpublishVolume(
    const NodeUnpublishVolumeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Node, NodeUnpublishVolume),
      request));
}


Future<GetNodeIDResponse> Client::GetNodeID(
    const GetNodeIDRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Node, GetNodeID),
      request));
}


Future<ProbeNodeResponse> Client::ProbeNode(
    const ProbeNodeRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Node, ProbeNode),
      request));
}


Future<NodeGetCapabilitiesResponse> Client::NodeGetCapabilities(
    const NodeGetCapabilitiesRequest& request)
{
  return validate(runtime.call(
      channel,
      GRPC_RPC(Node, NodeGetCapabilities),
      request));
}

} // namespace csi {
} // namespace mesos {
