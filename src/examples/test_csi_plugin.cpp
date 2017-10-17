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

#include <algorithm>

#include <google/protobuf/util/json_util.h>

#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>

#include <mesos/version.hpp>

#include <process/http.hpp>

#include <stout/flags.hpp>
#include <stout/fs.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/uuid.hpp>

#include "csi/spec.hpp"

#include "logging/logging.hpp"

using std::equal;
using std::list;
using std::max;
using std::min;
using std::string;
using std::unique_ptr;

using google::protobuf::RepeatedPtrField;

using google::protobuf::util::MessageToJsonString;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;


#define CHECK_REQUIREMENT(req)                                              \
  do {                                                                      \
    if (!(req)) {                                                           \
      response->mutable_error()->mutable_general_error()                    \
        ->set_error_code(csi::Error::GeneralError::MISSING_REQUIRED_FIELD); \
      response->mutable_error()->mutable_general_error()                    \
        ->set_caller_must_not_retry(true);                                  \
      return Status::OK;                                                    \
    }                                                                       \
  } while(0)

#define CHECK_REQUIRED_FIELD(base, field)                                   \
  CHECK_REQUIREMENT((base)->has_##field())

#define CHECK_REQUIRED_STRING_FIELD(base, field)                            \
  CHECK_REQUIREMENT(!(base)->field().empty())

#define CHECK_REQUIRED_MAP_FIELD(base, field)                               \
  CHECK_REQUIREMENT(!(base)->field().empty())

#define CHECK_REQUIRED_ONEOF_FIELD(base, field, not_set)                    \
  CHECK_REQUIREMENT((base)->field##_case() != not_set)

#define CHECK_REQUIRED_REPEATED_FIELD(base, field)                          \
  CHECK_REQUIREMENT((base)->field##_size() > 0)


const string PLUGIN_NAME = "test-csi-plugin";
const Bytes DEFAULT_VOLUME_CAPACITY = Megabytes(64);
const string DEFAULT_VOLUME_CAPABILITY =
  "{\"mount\":{\"mount_flags\":[\"--bind\"]}}";

class Flags : public virtual flags::FlagsBase
{
public:
  Flags()
  {
    add(&Flags::endpoint,
        "endpoint",
        "Path to the Unix domain socket the plugin should bind to.");

    add(&Flags::work_dir,
        "work_dir",
        "Path to the work directory of the plugin.");

    add(&Flags::total_capacity,
        "total_capacity",
        "The total disk capacity managed by the plugin.");
  }

  string endpoint;
  string work_dir;
  Bytes total_capacity;
};


template <typename Request>
static inline void log(Request&& request)
{
  string out;
  MessageToJsonString(request, &out);
  LOG(INFO) << request.GetDescriptor()->name() << ": " << out;
}


// Idempotently mount a volume.
template<typename Iterable>
Try<Nothing> mount(
    const string& source,
    const string& target,
    const Option<string>& fsType,
    const Iterable flags)
{
  Try<string> test = os::shell("mountpoint -q %s", target.c_str());
  if (test.isError()) {
    Try<string> mount = os::shell(
        "mount %s %s %s %s",
        fsType.isSome() ? ("-t " + fsType.get()).c_str() : "",
        strings::join(" ", flags).c_str(),
        source.c_str(),
        target.c_str());
    if (mount.isError()) {
      return Error(mount.error());
    }
  }

  return Nothing();
}


// Idempotently unmount a volume.
Try<Nothing> unmount(const string& target)
{
  Try<string> test = os::shell("mountpoint -q %s", target.c_str());
  if (test.isSome()) {
    Try<string> unmount = os::shell("umount %s", target.c_str());
    if (unmount.isError()) {
      return Error(unmount.error());
    }
  }

  return Nothing();
}


class TestCSIPlugin : public csi::Identity::Service,
                      public csi::Controller::Service,
                      public csi::Node::Service
{
public:
  TestCSIPlugin(const Flags& _flags)
    : flags(_flags)
  {
    addSupportedVersion(0, 1, 0);

    addCapability(csi::ControllerServiceCapability::RPC::CREATE_DELETE_VOLUME);
    addCapability(csi::ControllerServiceCapability::RPC::GET_CAPACITY);

    Try<JSON::Value> json = JSON::parse(DEFAULT_VOLUME_CAPABILITY);
    Try<csi::VolumeCapability> protobuf =
      protobuf::parse<csi::VolumeCapability>(json.get());
    addCapability(protobuf.get());

    availableCapacity = flags.total_capacity;

    Try<Nothing> mkdir = os::mkdir(flags.work_dir);
    if (mkdir.isError()) {
      LOG(ERROR)
        << "Failed to create work directory '" << flags.work_dir << "': "
        << mkdir.error();
    }

    Try<list<string>> paths = fs::list(path::join(flags.work_dir, "*", "*"));
    if (paths.isError()) {
      LOG(ERROR) << "Failed to list volumes: " << paths.error();
    }

    foreach (const string& path, paths.get()) {
      Try<Bytes> capacity = Bytes::parse(Path(path).basename());
      if (capacity.isError()) {
        LOG(ERROR)
          << "Failed to get capacity of volume '" << path << "': "
          << capacity.error();
      } else {
        availableCapacity -= capacity.get();
      }
    }

    ServerBuilder builder;
    builder.AddListeningPort(flags.endpoint, InsecureServerCredentials());
    builder.RegisterService(static_cast<csi::Identity::Service*>(this));
    builder.RegisterService(static_cast<csi::Controller::Service*>(this));
    builder.RegisterService(static_cast<csi::Node::Service*>(this));
    server = std::move(builder.BuildAndStart());
  }

  void wait()
  {
    if (server) {
      server->Wait();
    }
  }

  virtual Status GetSupportedVersions(
      ServerContext* context,
      const csi::GetSupportedVersionsRequest* request,
      csi::GetSupportedVersionsResponse* response) override
  {
    log(*request);

    response->mutable_result()->mutable_supported_versions()
      ->CopyFrom(supportedVersions);

    return Status::OK;
  }

  virtual Status GetPluginInfo(
      ServerContext* context,
      const csi::GetPluginInfoRequest* request,
      csi::GetPluginInfoResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      response->mutable_result()->set_name(PLUGIN_NAME);
      response->mutable_result()->set_vendor_version(MESOS_VERSION);
    }

    return Status::OK;
  }

  virtual Status CreateVolume(
      ServerContext* context,
      const csi::CreateVolumeRequest* request,
      csi::CreateVolumeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::CREATE_DELETE_VOLUME)) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(csi::Error::CreateVolumeError::CALL_NOT_IMPLEMENTED);
        return Status::OK;
      }

      CHECK_REQUIRED_REPEATED_FIELD(request, volume_capabilities);
      for (auto it = request->volume_capabilities().begin();
           it != request->volume_capabilities().end();
           ++it) {
        CHECK_REQUIRED_ONEOF_FIELD(
            it, value, csi::VolumeCapability::VALUE_NOT_SET);
      }

      if (request->name().empty() ||
          request->name().find_first_of(os::PATH_SEPARATOR) != string::npos) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(csi::Error::CreateVolumeError::INVALID_VOLUME_NAME);
        return Status::OK;
      }

      foreach (const auto& capability, request->volume_capabilities()) {
        if (!hasCapability(capability)) {
          response->mutable_error()->mutable_general_error()
            ->set_error_code(csi::Error::GeneralError::UNDEFINED);
          response->mutable_error()->mutable_general_error()
            ->set_caller_must_not_retry(true);
          response->mutable_error()->mutable_general_error()
            ->set_error_description(
                "Volume capability '" + stringify(JSON::protobuf(capability)) +
                "' is not supported");
          return Status::OK;
        }
      }

      if (os::exists(path::join(flags.work_dir, request->name()))) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(
              csi::Error::CreateVolumeError::VOLUME_ALREADY_EXISTS);
        return Status::OK;
      }

      Bytes capacity = min(DEFAULT_VOLUME_CAPACITY, availableCapacity);

      if (request->has_capacity_range()) {
        const csi::CapacityRange& range = request->capacity_range();
        if ((!range.required_bytes() && !range.limit_bytes()) ||
            (range.required_bytes() > range.limit_bytes())) {
          response->mutable_error()->mutable_create_volume_error()
            ->set_error_code(
                csi::Error::CreateVolumeError::UNSUPPORTED_CAPACITY_RANGE);
          return Status::OK;
        }

        capacity = min(
            max(capacity, Bytes(range.required_bytes())),
            Bytes(range.limit_bytes()));
      }

      if (capacity == Bytes(0) || capacity > availableCapacity) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(
              csi::Error::CreateVolumeError::UNSUPPORTED_CAPACITY_RANGE);
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_description("Insufficient capacity");
      }

      const string id = path::join(
          flags.work_dir, request->name(), stringify(capacity));

      Try<Nothing> mkdir = os::mkdir(id);
      if (mkdir.isError()) {
        response->mutable_error()->mutable_general_error()
          ->set_error_code(csi::Error::GeneralError::UNDEFINED);
        response->mutable_error()->mutable_general_error()
          ->set_caller_must_not_retry(false);
        response->mutable_error()->mutable_general_error()
          ->set_error_description(
              "Failed to create volume '" + id + "': " + mkdir.error());
        return Status::OK;
      }

      Try<mode_t> mode = os::stat::mode(id);
      CHECK(mode.isSome())
        << "Failed to get mode of volume '" << id << "': " << mode.error();

      availableCapacity -= capacity;

      csi::VolumeInfo* volume =
        response->mutable_result()->mutable_volume_info();
      volume->set_capacity_bytes(capacity.bytes());
      volume->mutable_access_mode()->set_mode(
          os::Permissions(mode.get()).owner.w
            ? csi::AccessMode::SINGLE_NODE_WRITER
            : csi::AccessMode::SINGLE_NODE_READER_ONLY);
      (*volume->mutable_id()->mutable_values())["id"] = id;
    }

    return Status::OK;
  }

  virtual Status DeleteVolume(
      ServerContext* context,
      const csi::DeleteVolumeRequest* request,
      csi::DeleteVolumeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::CREATE_DELETE_VOLUME)) {
        response->mutable_error()->mutable_delete_volume_error()
          ->set_error_code(csi::Error::DeleteVolumeError::CALL_NOT_IMPLEMENTED);
        return Status::OK;
      }

      CHECK_REQUIRED_FIELD(request, volume_id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);

      auto it = request->volume_id().values().find("id");
      if (it == request->volume_id().values().end() || it->second.empty()) {
        response->mutable_error()->mutable_delete_volume_error()
          ->set_error_code(csi::Error::DeleteVolumeError::INVALID_VOLUME_ID);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_delete_volume_error()
          ->set_error_code(
              csi::Error::DeleteVolumeError::VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      Path path(id);

      Try<Nothing> rmdir = os::rmdir(path.dirname());
      if (rmdir.isError()) {
        response->mutable_error()->mutable_general_error()
          ->set_error_code(csi::Error::GeneralError::UNDEFINED);
        response->mutable_error()->mutable_general_error()
          ->set_caller_must_not_retry(false);
        response->mutable_error()->mutable_general_error()
          ->set_error_description(
              "Failed to delete volume '" + id + "': " + rmdir.error());
        return Status::OK;
      }

      Try<Bytes> capacity = Bytes::parse(path.basename());
      CHECK(capacity.isSome())
        << "Failed to get capacity of volume '" << path << "': "
        << capacity.error();

      availableCapacity += capacity.get();

      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status ControllerPublishVolume(
      ServerContext* context,
      const csi::ControllerPublishVolumeRequest* request,
      csi::ControllerPublishVolumeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::PUBLISH_UNPUBLISH_VOLUME)) {
        response->mutable_error()->mutable_controller_publish_volume_error()
          ->set_error_code(
              csi::Error::ControllerPublishVolumeError::CALL_NOT_IMPLEMENTED);
        return Status::OK;
      }

      CHECK_REQUIRED_FIELD(request, volume_id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
      CHECK_REQUIRED_FIELD(request, node_id);
      CHECK_REQUIRED_MAP_FIELD(&request->node_id(), values);

      auto it = request->volume_id().values().find("id");
      if (it == request->volume_id().values().end() || it->second.empty()) {
        response->mutable_error()->mutable_controller_publish_volume_error()
          ->set_error_code(
              csi::Error::ControllerPublishVolumeError::INVALID_VOLUME_ID);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_controller_publish_volume_error()
          ->set_error_code(
              csi::Error::ControllerPublishVolumeError::VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      if (request->has_node_id()) {
        response->mutable_error()->mutable_controller_publish_volume_error()
          ->set_error_code(
              csi::Error::ControllerPublishVolumeError::INVALID_NODE_ID);
        return Status::OK;
      }

      // Do nothing.
      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status ControllerUnpublishVolume(
      ServerContext* context,
      const csi::ControllerUnpublishVolumeRequest* request,
      csi::ControllerUnpublishVolumeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::PUBLISH_UNPUBLISH_VOLUME)) {
        response->mutable_error()->mutable_controller_unpublish_volume_error()
          ->set_error_code(
              csi::Error::ControllerUnpublishVolumeError::CALL_NOT_IMPLEMENTED);
        return Status::OK;
      }

      CHECK_REQUIRED_FIELD(request, volume_id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
      CHECK_REQUIRED_FIELD(request, node_id);
      CHECK_REQUIRED_MAP_FIELD(&request->node_id(), values);

      auto it = request->volume_id().values().find("id");
      if (it == request->volume_id().values().end() || it->second.empty()) {
        response->mutable_error()->mutable_controller_unpublish_volume_error()
          ->set_error_code(
              csi::Error::ControllerUnpublishVolumeError::INVALID_VOLUME_ID);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_controller_unpublish_volume_error()
          ->set_error_code(
              csi::Error::ControllerUnpublishVolumeError::
              VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      if (request->has_node_id()) {
        response->mutable_error()->mutable_controller_unpublish_volume_error()
          ->set_error_code(
              csi::Error::ControllerUnpublishVolumeError::INVALID_NODE_ID);
        return Status::OK;
      }

      // Do nothing.
      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status ValidateVolumeCapabilities(
      ServerContext* context,
      const csi::ValidateVolumeCapabilitiesRequest* request,
      csi::ValidateVolumeCapabilitiesResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      CHECK_REQUIRED_FIELD(request, version);
      CHECK_REQUIRED_FIELD(request, volume_info);
      CHECK_REQUIRED_FIELD(&request->volume_info(), access_mode);
      CHECK_REQUIRED_FIELD(&request->volume_info(), id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_info().id(), values);
      CHECK_REQUIRED_REPEATED_FIELD(request, volume_capabilities);
      for (auto it = request->volume_capabilities().begin();
           it != request->volume_capabilities().end();
           ++it) {
        CHECK_REQUIRED_ONEOF_FIELD(
            it, value, csi::VolumeCapability::VALUE_NOT_SET);
      }

      auto it = request->volume_info().id().values().find("id");
      if (it == request->volume_info().id().values().end() ||
          it->second.empty()) {
        response->mutable_error()->mutable_validate_volume_capabilities_error()
          ->set_error_code(
              csi::Error::ValidateVolumeCapabilitiesError::INVALID_VOLUME_INFO);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_validate_volume_capabilities_error()
          ->set_error_code(csi::Error::ValidateVolumeCapabilitiesError::
                           VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      foreach (const auto& capability, request->volume_capabilities()) {
        if (!hasCapability(capability)) {
          response->mutable_error()
            ->mutable_validate_volume_capabilities_error()
            ->set_error_code(!capability.has_mount()
              ? csi::Error::ValidateVolumeCapabilitiesError::
                UNSUPPORTED_VOLUME_TYPE
              : (!capability.mount().fs_type().empty()
                   ? csi::Error::ValidateVolumeCapabilitiesError::
                     UNSUPPORTED_FS_TYPE
                   : csi::Error::ValidateVolumeCapabilitiesError::
                     UNSUPPORTED_MOUNT_FLAGS));
          return Status::OK;
        }
      }

      response->mutable_result()->set_supported(true);
    }

    return Status::OK;
  }

  virtual Status ListVolumes(
      ServerContext* context,
      const csi::ListVolumesRequest* request,
      csi::ListVolumesResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::LIST_VOLUMES)) {
        // TODO(chhsiao): Use `ListVolumesError` once CSI supports it.
        response->mutable_error()->mutable_general_error()
          ->set_error_code(csi::Error::GeneralError::UNDEFINED);
        response->mutable_error()->mutable_general_error()
          ->set_caller_must_not_retry(true);
        response->mutable_error()->mutable_general_error()
          ->set_error_description("Not implemented");
        return Status::OK;
      }

      Try<list<string>> paths = fs::list(path::join(flags.work_dir, "*", "*"));
      if (paths.isError()) {
        // We don't return the error since it could be caused by no volume.
        LOG(ERROR) << "Failed to list volumes: " << paths.error();
      } else {
        RepeatedPtrField<csi::ListVolumesResponse::Result::Entry> entries;
        auto it = paths->begin();

        if (!request->starting_token().empty()) {
          it = find(paths->begin(), paths->end(), request->starting_token());
          if (it == paths->end()) {
            // TODO(chhsiao): Use `ListVolumesError` once CSI supports it.
            response->mutable_error()->mutable_general_error()
              ->set_error_code(csi::Error::GeneralError::UNDEFINED);
            response->mutable_error()->mutable_general_error()
              ->set_caller_must_not_retry(true);
            response->mutable_error()->mutable_general_error()
              ->set_error_description("Invalid starting token");
            return Status::OK;
          }
        }

        for (; it != paths->end(); it++) {
          Try<Bytes> capacity = Bytes::parse(Path(*it).basename());
          if (capacity.isError()) {
            LOG(ERROR)
              << "Failed to get capacity of volume '" << *it << "': "
              << capacity.error();
            continue;
          }

          Try<mode_t> mode = os::stat::mode(*it);
          if (mode.isError()) {
            LOG(ERROR)
              << "Failed to get mode of volume '" << *it << "': "
              << mode.error();
            continue;
          }

          csi::VolumeInfo* volume = entries.Add()->mutable_volume_info();
          volume->set_capacity_bytes(capacity->bytes());
          volume->mutable_access_mode()->set_mode(
              os::Permissions(mode.get()).owner.w
                ? csi::AccessMode::SINGLE_NODE_WRITER
                : csi::AccessMode::SINGLE_NODE_READER_ONLY);
          (*volume->mutable_id()->mutable_values())["id"] = *it;

          if (request->max_entries() &&
              static_cast<uint32_t>(entries.size()) == request->max_entries()) {
            break;
          }
        }

        response->mutable_result()->mutable_entries()->Swap(&entries);
        if (it != paths->end()) {
          response->mutable_result()->set_next_token(*it);
        }
      }
    }

    return Status::OK;
  }

  virtual Status GetCapacity(
      ServerContext* context,
      const csi::GetCapacityRequest* request,
      csi::GetCapacityResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      if (!hasCapability(csi::ControllerServiceCapability::
                         RPC::GET_CAPACITY)) {
        // TODO(chhsiao): Use `GetCapacityError` once CSI supports it.
        response->mutable_error()->mutable_general_error()
          ->set_error_code(csi::Error::GeneralError::UNDEFINED);
        response->mutable_error()->mutable_general_error()
          ->set_caller_must_not_retry(true);
        response->mutable_error()->mutable_general_error()
          ->set_error_description("Not implemented");
        return Status::OK;
      }

      // TODO(chhsiao): Here we report the available capacity instead of
      // the total capacity to follow the latest CSI spec.
      response->mutable_result()->set_total_capacity(availableCapacity.bytes());
    }

    return Status::OK;
  }

  virtual Status ControllerGetCapabilities(
      ServerContext* context,
      const csi::ControllerGetCapabilitiesRequest* request,
      csi::ControllerGetCapabilitiesResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      response->mutable_result()->mutable_capabilities()
        ->CopyFrom(controllerCapabilities);
    }

    return Status::OK;
  }

  virtual Status NodePublishVolume(
      ServerContext* context,
      const csi::NodePublishVolumeRequest* request,
      csi::NodePublishVolumeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      CHECK_REQUIRED_FIELD(request, volume_id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
      CHECK_REQUIRED_STRING_FIELD(request, target_path);
      CHECK_REQUIRED_FIELD(request, volume_capability);
      CHECK_REQUIRED_ONEOF_FIELD(
          &request->volume_capability(),
          value,
          csi::VolumeCapability::VALUE_NOT_SET);

      auto it = request->volume_id().values().find("id");
      if (it == request->volume_id().values().end() || it->second.empty()) {
        response->mutable_error()->mutable_node_publish_volume_error()
          ->set_error_code(
              csi::Error::NodePublishVolumeError::INVALID_VOLUME_ID);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_node_publish_volume_error()
          ->set_error_code(
              csi::Error::NodePublishVolumeError::VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      const csi::VolumeCapability& capability = request->volume_capability();
      if (!hasCapability(capability)) {
        response->mutable_error()->mutable_node_publish_volume_error()
          ->set_error_code(!capability.has_mount()
            ? csi::Error::NodePublishVolumeError::UNSUPPORTED_VOLUME_TYPE
            : (!capability.mount().fs_type().empty()
                 ? csi::Error::NodePublishVolumeError::UNSUPPORTED_FS_TYPE
                 : csi::Error::NodePublishVolumeError::
                   UNSUPPORTED_MOUNT_FLAGS));
          return Status::OK;
      }

      Try<Nothing> _mount = mount(
          id,
          request->target_path(),
          capability.mount().fs_type().empty()
            ? Option<string>::none() : capability.mount().fs_type(),
          capability.mount().mount_flags());

      if (_mount.isError()) {
        response->mutable_error()->mutable_node_publish_volume_error()
          ->set_error_code(csi::Error::NodePublishVolumeError::MOUNT_ERROR);
        return Status::OK;
      }

      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status NodeUnpublishVolume(
      ServerContext* context,
      const csi::NodeUnpublishVolumeRequest* request,
      csi::NodeUnpublishVolumeResponse* response)
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      CHECK_REQUIRED_FIELD(request, volume_id);
      CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
      CHECK_REQUIRED_STRING_FIELD(request, target_path);

      auto it = request->volume_id().values().find("id");
      if (it == request->volume_id().values().end() || it->second.empty()) {
        response->mutable_error()->mutable_node_unpublish_volume_error()
          ->set_error_code(
              csi::Error::NodeUnpublishVolumeError::INVALID_VOLUME_ID);
        return Status::OK;
      }

      const string& id = it->second;
      if (!os::exists(id)) {
        response->mutable_error()->mutable_node_unpublish_volume_error()
          ->set_error_code(
              csi::Error::NodeUnpublishVolumeError::VOLUME_DOES_NOT_EXIST);
        return Status::OK;
      }

      Try<Nothing> _unmount = unmount(request->target_path());

      if (_unmount.isError()) {
        response->mutable_error()->mutable_node_unpublish_volume_error()
          ->set_error_code(csi::Error::NodeUnpublishVolumeError::UNMOUNT_ERROR);
        return Status::OK;
      }

      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status GetNodeID(
      ServerContext* context,
      const csi::GetNodeIDRequest* request,
      csi::GetNodeIDResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      // No NodeID.
      response->mutable_result();;
    }

    return Status::OK;
  }

  virtual Status ProbeNode(
      ServerContext* context,
      const csi::ProbeNodeRequest* request,
      csi::ProbeNodeResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status NodeGetCapabilities(
      ServerContext* context,
      const csi::NodeGetCapabilitiesRequest* request,
      csi::NodeGetCapabilitiesResponse* response) override
  {
    log(*request);

    if (isSupportedVersion(request, response)) {
      response->mutable_result()->mutable_capabilities()
        ->CopyFrom(nodeCapabilities);
    }

    return Status::OK;
  }

private:
  inline void addSupportedVersion(
      uint32_t major, uint32_t minor, uint32_t patch)
  {
    csi::Version* ver = supportedVersions.Add();
    ver->set_major(major);
    ver->set_minor(minor);
    ver->set_patch(patch);
  }

  // Checks if the `version` of the request exists and is supported. If
  // not, the response will be set with an appropriate error.
  template <typename Request, typename Response>
  bool isSupportedVersion(const Request* request, Response* response)
  {
    if (!request->has_version()) {
      response->mutable_error()->mutable_general_error()
        ->set_error_code(csi::Error::GeneralError::MISSING_REQUIRED_FIELD);
      response->mutable_error()->mutable_general_error()
        ->set_caller_must_not_retry(true);
      response->mutable_error()->mutable_general_error()
        ->set_error_description(
            "'" + request->GetDescriptor()->name() + ".version' is not set");

      return false;
    }

    for(const auto& ver : supportedVersions) {
      if (ver.major() == request->version().major() &&
          ver.minor() == request->version().minor() &&
          ver.patch() == request->version().patch()) {
        return true;
      }
    }

    response->mutable_error()->mutable_general_error()
      ->set_error_code(csi::Error::GeneralError::UNSUPPORTED_REQUEST_VERSION);
    response->mutable_error()->mutable_general_error()
      ->set_caller_must_not_retry(true);

    return false;
  }

  inline void addCapability(csi::ControllerServiceCapability::RPC::Type type)
  {
    controllerCapabilities.Add()->mutable_rpc()->set_type(type);
  }

  inline void addCapability(csi::NodeServiceCapability::RPC::Type type)
  {
    nodeCapabilities.Add()->mutable_rpc()->set_type(type);
  }

  inline void addCapability(const csi::VolumeCapability& capability)
  {
    volumeCapabilities.Add()->CopyFrom(capability);
    controllerCapabilities.Add()->mutable_volume_capability()
      ->CopyFrom(capability);
    nodeCapabilities.Add()->mutable_volume_capability()->CopyFrom(capability);
  }

  inline bool hasCapability(csi::ControllerServiceCapability::RPC::Type type)
  {
    foreach (const auto& capability, controllerCapabilities) {
      if (capability.has_rpc() && capability.rpc().type() == type) {
        return true;
      }
    }

    return false;
  }

  inline bool hasCapability(csi::NodeServiceCapability::RPC::Type type)
  {
    foreach (const auto& capability, nodeCapabilities) {
      if (capability.has_rpc() && capability.rpc().type() == type) {
        return true;
      }
    }

    return false;
  }

  inline bool hasCapability(const csi::VolumeCapability& capability)
  {
    foreach (const auto& _capability, volumeCapabilities) {
      if (stringify(JSON::protobuf(_capability)) ==
          stringify(JSON::protobuf(capability))) {
        return true;
      }
    }

    return false;
  }

  const Flags flags;
  unique_ptr<Server> server;
  RepeatedPtrField<csi::Version> supportedVersions;
  RepeatedPtrField<csi::ControllerServiceCapability> controllerCapabilities;
  RepeatedPtrField<csi::NodeServiceCapability> nodeCapabilities;
  RepeatedPtrField<csi::VolumeCapability> volumeCapabilities;
  Bytes availableCapacity;
};


int main(int argc, char** argv)
{
  Flags flags;
  Try<flags::Warnings> load = flags.load("CSI_", argc, argv);

  if (flags.help) {
    std::cout << flags.usage() << std::endl;
    return EXIT_SUCCESS;
  }

  if (load.isError()) {
    std::cerr << flags.usage(load.error()) << std::endl;
    return EXIT_SUCCESS;
  }

  mesos::internal::logging::initialize(argv[0], false);

  // Log any flag warnings.
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  unique_ptr<TestCSIPlugin> plugin(new TestCSIPlugin(flags));

  plugin->wait();

  return 0;
}
