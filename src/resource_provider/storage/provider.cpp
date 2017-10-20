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

#include "resource_provider/storage/provider.hpp"

#include <google/protobuf/util/json_util.h>

#include <glog/logging.h>

#include <process/after.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/process.hpp>
#include <process/timeout.hpp>

#include <mesos/resources.hpp>

#include <mesos/resource_provider/resource_provider.hpp>

#include <mesos/v1/resource_provider.hpp>

#include <stout/foreach.hpp>
#include <stout/fs.hpp>
#include <stout/os.hpp>

#include "common/http.hpp"

#include "csi/client.hpp"
#include "csi/utils.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "resource_provider/detector.hpp"

#include "slave/paths.hpp"

namespace http = process::http;

namespace paths = mesos::internal::slave::paths;

using std::list;
using std::queue;
using std::shared_ptr;
using std::string;

using google::protobuf::util::JsonStringToMessage;

using process::Break;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::Promise;
using process::Timeout;

using process::after;
using process::await;
using process::defer;
using process::loop;
using process::spawn;
using process::terminate;
using process::wait;

using process::http::authentication::Principal;

using mesos::ResourceProviderInfo;
using mesos::Resources;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using mesos::v1::resource_provider::Driver;

namespace mesos {
namespace internal {

static const string SLRP_NAME_PREFIX = "mesos-rp-local-storage-";

// We use percent-encoding to escape '.' (reserved for standalone
// containers) and '-' (reserved as a separator) for names, in addition
// to the characters reserved in RFC 3986.
static const string SLRP_NAME_RESERVED = ".-";

// Timeout for a CSI plugin to create its endpoint socket.
// TODO(chhsiao): Make the timeout configurable.
static const Duration CSI_ENDPOINT_CREATION_TIMEOUT = Seconds(5);
static const string CSI_SYMLINK = "csi";
static const uint32_t CSI_MAX_ENTRIES = 100;


// Returns a prefix for naming components of the resource provider. This
// prefix can be used in various places, such as container IDs for or
// socket paths for CSI plugins.
static inline string getPrefix(const ResourceProviderInfo& info)
{
  return SLRP_NAME_PREFIX + http::encode(info.name(), SLRP_NAME_RESERVED) + "-";
}


// Returns the path to the socket file for the plugin.
static inline string getSocketPath(const string& csiDir, const string& plugin)
{
  return path::join(csiDir, http::encode(plugin, SLRP_NAME_RESERVED) + ".sock");
}


// Returns the path to the mount point for the volume.
static inline string getVolumeMountPath(
    const string& csiDir,
    const string& volumeId)
{
  return path::join(
      csiDir,
      "volumes",
      http::encode(volumeId, SLRP_NAME_RESERVED));
}


// Returns the container ID for the plugin.
static inline ContainerID getContainerID(
    const ResourceProviderInfo& info,
    const string& plugin)
{
  ContainerID containerId;

  containerId.set_value(
      getPrefix(info) + http::encode(plugin, SLRP_NAME_RESERVED));

  return containerId;
}


// Returns the parent endpoint as a URL.
static inline http::URL extractParentEndpoint(const http::URL& url)
{
  http::URL parent = url;

  parent.path = Path(url.path).dirname();

  return parent;
}


// Returns the 'Bearer' credential as a header for calling the V1 agent
// API if the `authToken` is presented, or empty otherwise.
static inline http::Headers getAuthHeader(const Option<string>& authToken)
{
  http::Headers headers;

  if (authToken.isSome()) {
    headers["Authorization"] = "Bearer " + authToken.get();
  }

  return headers;
}


// Convenient function to print an error message with `std::bind`.
static inline void err(const string& message, const string& error)
{
  LOG(ERROR) << message << ": " << error;
}


// Convenient function to check if a container contains a value.
template <typename Iterable, typename Value>
static inline bool contains(const Iterable& iterable, const Value& value)
{
  foreach (const auto& item, iterable) {
    if (item == value) {
      return true;
    }
  }

  return false;
}


class StorageLocalResourceProviderProcess
  : public Process<StorageLocalResourceProviderProcess>
{
public:
  explicit StorageLocalResourceProviderProcess(
      const http::URL& _url,
      const string& _workDir,
      const ResourceProviderInfo& _info,
      const Option<string>& _authToken)
    : ProcessBase(process::ID::generate("storage-local-resource-provider")),
      url(_url),
      workDir(_workDir),
      contentType(ContentType::PROTOBUF),
      info(_info),
      authToken(_authToken) {}

  StorageLocalResourceProviderProcess(
      const StorageLocalResourceProviderProcess& other) = delete;

  StorageLocalResourceProviderProcess& operator=(
      const StorageLocalResourceProviderProcess& other) = delete;

  void connected();
  void disconnected();
  void received(const Event& event);

private:
  void initialize() override;

  void subscribed(const Event::Subscribed& subscribed);
  void operation(const Event::Operation& operation);
  void publish(const Event::Publish& publish);
  void unpublish(const Event::Unpublish& unpublish);

  Future<csi::Client> loadController();
  Future<csi::Client> loadNode();
  Future<Nothing> loadResources();
  Future<Nothing> publishResource(const Resource& volume);
  Future<Nothing> unpublishResource(const Resource& volume);

  Future<csi::Client> connect(const string& plugin);
  Future<csi::Client> launch(const string& plugin);
  Future<Nothing> kill(const string& plugin);

  const http::URL url;
  const string workDir;
  const ContentType contentType;
  ResourceProviderInfo info;
  const Option<string> authToken;

  string csiDir;
  csi::Version csiVersion;
  process::grpc::client::Runtime runtime;
  Owned<v1::resource_provider::Driver> driver;

  Option<csi::GetPluginInfoResponse::Result> controllerInfo;
  Option<csi::GetPluginInfoResponse::Result> nodeInfo;
  Option<csi::ControllerCapabilities> controllerCapabilities;
  Option<csi::NodeCapabilities> nodeCapabilities;
  Option<csi::GetNodeIDResponse::Result> nodeId;
  Future<csi::Client> controllerClient;
  Future<csi::Client> nodeClient;
  Resources resources;
};


void StorageLocalResourceProviderProcess::connected()
{
  const string error =
    "Failed to subscribe resource provider with type '" + info.type() +
    "' and name '" + info.name();

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();
  subscribe->mutable_resource_provider_info()->CopyFrom(info);

  driver->send(evolve(call))
    .onFailed(std::bind(err, error, lambda::_1))
    .onDiscarded(std::bind(err, error, "future discarded"));
}


void StorageLocalResourceProviderProcess::disconnected()
{
}


void StorageLocalResourceProviderProcess::received(const Event& event)
{
  LOG(INFO) << "Received " << event.type() << " event";

  switch (event.type()) {
    case Event::SUBSCRIBED: {
      CHECK(event.has_subscribed());
      subscribed(event.subscribed());
      break;
    }
    case Event::OPERATION: {
      CHECK(event.has_operation());
      operation(event.operation());
      break;
    }
    case Event::PUBLISH: {
      CHECK(event.has_publish());
      publish(event.publish());
      break;
    }
    case Event::UNPUBLISH: {
      CHECK(event.has_unpublish());
      unpublish(event.unpublish());
      break;
    }
    case Event::UNKNOWN: {
      LOG(WARNING) << "Received an UNKNOWN event and ignored";
      break;
    }
  }
}


void StorageLocalResourceProviderProcess::initialize()
{
  const string latestDir = paths::getLatestResourceProviderPath(workDir, info);

  // Recover the resource provider ID from the latest symlink.
  Result<string> _latestDir = os::realpath(latestDir);
  if (_latestDir.isSome()) {
    info.mutable_id()->set_value(Path(_latestDir.get()).basename());
  }

  // Recover the directory where CSI sockets are placed.
  Result<string> _csiDir = os::realpath(path::join(latestDir, CSI_SYMLINK));
  if (_csiDir.isSome()) {
    csiDir = _csiDir.get();
  } else {
    Try<string> mkdtemp =
      os::mkdtemp(path::join(os::temp(), getPrefix(info) + "XXXXXX"));
    if (mkdtemp.isError()) {
      LOG(ERROR)
        << "Failed to create directory '" << csiDir << "': "
        << mkdtemp.error();
      terminate(self());
      return;
    }

    csiDir = mkdtemp.get();
  }

  // Set CSI version to 0.1.0.
  csiVersion.set_major(0);
  csiVersion.set_minor(1);
  csiVersion.set_patch(0);

  driver.reset(new Driver(
      Owned<EndpointDetector>(new ConstantEndpointDetector(url)),
      contentType,
      defer(self(), &Self::connected),
      defer(self(), &Self::disconnected),
      defer(self(), [this](queue<v1::resource_provider::Event> events) {
        while(!events.empty()) {
        const v1::resource_provider::Event& event = events.front();
          received(devolve(event));
          events.pop();
        }
      }),
      None())); // TODO(nfnt): Add authentication as part of MESOS-7854.
}


void StorageLocalResourceProviderProcess::subscribed(
    const Event::Subscribed& subscribed)
{
  LOG(INFO) << "Subscribed with ID " << subscribed.provider_id().value();

  if (!info.has_id()) {
    // New subscription.
    info.mutable_id()->CopyFrom(subscribed.provider_id());
    paths::createResourceProviderDirectory(workDir, info);
  }

  // Checkpoint the directory where CSI sockets are placed.
  const string csiSymlink = path::join(
      paths::getResourceProviderPath(workDir, info),
      CSI_SYMLINK);

  Result<string> _csiDir = os::realpath(csiSymlink);
  if (_csiDir.isSome() && _csiDir.get() != csiDir) {
    Try<Nothing> rm = os::rm(csiSymlink);
    if (rm.isError()) {
      LOG(ERROR)
        << "Failed to remove symlink '" << csiSymlink << "': " << rm.error();
      terminate(self());
      return;
    }
  }

  if (!os::exists(csiSymlink)) {
    Try<Nothing> symlink = fs::symlink(csiDir, csiSymlink);
    if (symlink.isError()) {
      LOG(ERROR)
        << "Failed to symlink '" << csiDir << "' to '" << csiSymlink
        << "': " << symlink.error();
    }
  }

  const string error =
    "Failed to update total resource with type '" + info.type() +
    "' and name '" + info.name();

  // NOTE: Currently, CSI does not have a `ProbeController` call, so we
  // we rely on `ProbeNode` to validate the runtime environment. In the
  // future, we would like to deserialize loading of both plugins.
  loadNode()
    .then(defer(self(), &Self::loadController))
    .then(defer(self(), &Self::loadResources))
    .then(defer(self(), [this] {
      Call call;
      call.set_type(Call::UPDATE_STATE);
      call.mutable_resource_provider_id()->CopyFrom(info.id());

      Call::UpdateState* update = call.mutable_update_state();
      update->mutable_resources()->CopyFrom(resources);

      return driver->send(evolve(call));
    }))
    .onFailed(std::bind(err, error, lambda::_1))
    .onDiscarded(std::bind(err, error, "future discarded"));
}


void StorageLocalResourceProviderProcess::operation(
    const Event::Operation& operation)
{
}


void StorageLocalResourceProviderProcess::publish(const Event::Publish& publish)
{
}


void StorageLocalResourceProviderProcess::unpublish(
    const Event::Unpublish& unpublish)
{
}


Future<csi::Client> StorageLocalResourceProviderProcess::loadController()
{
  // Discard the previous loading chain if there is one.
  controllerClient.discard();
  controllerInfo = None();
  controllerCapabilities = None();

  return controllerClient = connect(info.storage().controller_plugin())
    .then(defer(self(), [this](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then([=](const csi::GetPluginInfoResponse& response)
            -> Future<csi::Client> {
          controllerInfo = response.result();

          LOG(INFO)
            << "Plugin '" << info.storage().controller_plugin() << "' loaded: "
            << stringify(controllerInfo.get());

          if (nodeInfo.isSome() &&
              (nodeInfo->name() != controllerInfo->name() ||
               nodeInfo->vendor_version() !=
               controllerInfo->vendor_version())) {
            LOG(WARNING)
              << "Inconsistent controller and node plugins. Please check with "
                 "the plugin vendors to ensure compatibility.";
          }

          return client;
        });
    }))
    .then(defer(self(), [this](csi::Client client) {
      // Get the controller capabilities.
      csi::ControllerGetCapabilitiesRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ControllerGetCapabilities(request)
        .then([=](const csi::ControllerGetCapabilitiesResponse& response)
            -> Future<csi::Client> {
          controllerCapabilities = response.result().capabilities();

          return client;
        });
    }))
    .onAny(defer(self(), [this](const Future<csi::Client>& future) {
      if (!future.isReady()) {
        kill(info.storage().controller_plugin());
      }
    }));
}


Future<csi::Client> StorageLocalResourceProviderProcess::loadNode()
{
  // Discard the previous loading chain if there is one.
  nodeClient.discard();
  nodeInfo = None();
  nodeCapabilities = None();
  nodeId = None();

  return nodeClient = connect(info.storage().node_plugin())
    .then(defer(self(), [this](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then([=](const csi::GetPluginInfoResponse& response)
            -> Future<csi::Client> {
          nodeInfo = response.result();

          LOG(INFO)
            << "Plugin '" << info.storage().node_plugin() << "' loaded: "
            << stringify(nodeInfo.get());

          if (controllerInfo.isSome() &&
              (controllerInfo->name() != nodeInfo->name() ||
               controllerInfo->vendor_version() !=
               nodeInfo->vendor_version())) {
            LOG(WARNING)
              << "Inconsistent controller and node plugins. Please check with "
                 "the plugin vendors to ensure compatibility.";
          }

          return client;
        });
    }))
    .then(defer(self(), [this](csi::Client client) {
      // Probe the plugin to validate the runtime environment.
      csi::ProbeNodeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ProbeNode(request)
        .then([=](const csi::ProbeNodeResponse& response)
            -> Future<csi::Client> {
          return client;
        });
    }))
    .then(defer(self(), [this](csi::Client client) {
      // Get the node capabilities.
      csi::NodeGetCapabilitiesRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.NodeGetCapabilities(request)
        .then([=](const csi::NodeGetCapabilitiesResponse& response)
            -> Future<csi::Client> {
          nodeCapabilities = response.result().capabilities();

          return client;
        });
    }))
    .then(defer(self(), [this](csi::Client client) {
      // Get the node ID.
      csi::GetNodeIDRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetNodeID(request)
        .then([=](const csi::GetNodeIDResponse& response)
            -> Future<csi::Client> {
          nodeId = response.result();

          return client;
        });
    }))
    .onAny(defer(self(), [this](const Future<csi::Client>& future) {
      if (!future.isReady()) {
        kill(info.storage().node_plugin());
      }
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::loadResources()
{
  // NOTE: This can only be called after `loadController`.
  CHECK_SOME(controllerCapabilities);

  return controllerClient
    .then(defer(self(), [this](csi::Client client) {
      list<Future<Resources>> futures;

      if (controllerCapabilities->getCapacity) {
        // TODO(chhsiao): Query the capacity for each profile.
        csi::GetCapacityRequest request;
        request.mutable_version()->CopyFrom(csiVersion);

        futures.emplace_back(client.GetCapacity(request)
          .then(defer(self(), [this](const csi::GetCapacityResponse& response) {
            Resources resources;

            // TODO(chhsiao): Here we assume that `total_capacity` is
            // the available capacity to follow the latest CSI spec.
            Resource resource = Resources::parse(
                "disk",
                stringify(response.result().total_capacity()),
                "*").get();
            resource.mutable_disk()->mutable_source()->set_type(
                Resource::DiskInfo::Source::RAW);

            if (Resources::isEmpty(resource)) {
              LOG(WARNING)
                << "Ignored empty resource "<< resource << "from plugin '"
                << info.storage().controller_plugin() << "'";
            } else if (!controllerCapabilities->createDeleteVolume) {
              LOG(WARNING)
                << "Ignored resource " << resource << " from plugin '"
                << info.storage().controller_plugin()
                << "': Capability 'CREATE_DELETE_VOLUME' is not supported";
            } else {
              resources += resource;
            }

            return resources;
          })));
      }

      if (controllerCapabilities->listVolumes) {
        Owned<list<Future<Resource>>> volumes;
        Owned<string> startingToken(new string);

        futures.emplace_back(loop(
          self(),
          [=]() mutable -> Future<csi::ListVolumesResponse> {
            csi::ListVolumesRequest request;
            request.mutable_version()->CopyFrom(csiVersion);
            request.set_max_entries(CSI_MAX_ENTRIES);
            request.set_starting_token(*startingToken);

            return client.ListVolumes(request);
          },
          [=](const csi::ListVolumesResponse& response)
              -> ControlFlow<list<Future<Resource>>> {
            foreach (const auto& entry, response.result().entries()) {
              const csi::VolumeInfo& volume = entry.volume_info();

              // TODO(chhsiao): Recover volume profiles from checkpoints
              // and validate volume capabilities.
              Resource resource = Resources::parse(
                  "disk", stringify(volume.capacity_bytes()), "*").get();
              Resource::DiskInfo::Source* source =
                resource.mutable_disk()->mutable_source();

              // TODO(chhsiao): Use ID string once we update the CSI spec.
              source->set_id(stringify(volume.id()));
              if (volume.has_metadata()) {
                *source->mutable_metadata() =
                  csi::volumeMetadataToLabels(volume.metadata());
              }

              if (Resources::isEmpty(resource)) {
                LOG(WARNING)
                  << "Ignored empty resource " << resource << " from plugin '"
                  << info.storage().controller_plugin() << "'";
              } else {
                // TODO(chhsiao): Emplace the future of
                // `ValidateVolumeCapabilities`.
                volumes->emplace_back(resource);
              }
            }

            *startingToken = response.result().next_token();
            if (startingToken->empty()) {
              return Break(*volumes);
            }

            return Continue();
          })
          .then(defer(self(), [=](const list<Future<Resource>>& volumes) {
            return await(volumes)
              .then(defer(self(), [](const list<Future<Resource>>& volumes) {
                Resources resources;

                foreach (const Future<Resource>& volume, volumes) {
                  if (volume.isReady()) {
                    resources += volume.get();
                  }
                }

                return resources;
              }));
          })));
      }

      return await(futures)
        .then(defer(self(), [this](const list<Future<Resources>>& futures) {
          foreach (const Future<Resources>& future, futures) {
            if (future.isReady()) {
              resources += future.get();
            } else {
              LOG(ERROR)
                << "Failed to load resources with type '" << info.type()
                << "' and name '" << info.name() << "': "
                << (future.isFailed() ? future.failure() : "future discarded");
            }
          }

          LOG(INFO)
            << "Total resources of type '" << info.type() << "' and name '"
            << info.name() << "': " << resources;

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::publishResource(
    const Resource& volume)
{
  // TODO(chhsiao): Make this a state machine and checkpoint the state!

  // NOTE: This can only be called after `loadController` and
  // `loadNode`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeCapabilities);
  CHECK_SOME(nodeId);

  CHECK(volume.has_disk());
  const Resource::DiskInfo& disk = volume.disk();

  CHECK(disk.has_source());
  CHECK(disk.source().has_id());
  CHECK(disk.has_volume());

  Option<string> mountPath;
  if (disk.source().type() == Resource::DiskInfo::Source::PATH) {
    CHECK(disk.source().has_path());
    CHECK(disk.source().path().has_root());
    mountPath = disk.source().path().root();
  } else if (disk.source().type() == Resource::DiskInfo::Source::MOUNT) {
    CHECK(disk.source().has_mount());
    CHECK(disk.source().mount().has_root());
    mountPath = disk.source().mount().root();
  } else if (disk.source().type() == Resource::DiskInfo::Source::BLOCK) {
    mountPath = getVolumeMountPath(csiDir, disk.source().id());
  }
  CHECK_SOME(mountPath);

  // Pick the first volume capability from those we used to create
  // the volume.
  // TODO(chhsiao): Get the volume capability based on the profile.
  Option<csi::VolumeCapability> volumeCapability;
  if (disk.source().type() == Resource::DiskInfo::Source::PATH ||
      disk.source().type() == Resource::DiskInfo::Source::MOUNT) {
    foreach (const auto& capability, nodeCapabilities->mountVolumes) {
      if (contains(controllerCapabilities->mountVolumes, capability)) {
        volumeCapability = capability;
        break;
      }
    }
  } else if (disk.source().type() == Resource::DiskInfo::Source::BLOCK) {
    foreach (const auto& capability, nodeCapabilities->blockVolumes) {
      if (contains(controllerCapabilities->blockVolumes, capability)) {
        volumeCapability = capability;
        break;
      }
    }
  }
  if (volumeCapability.isNone()) {
    return Failure("Failed to select a volume capability");
  }

  // NOTE: We create the mount point first, so the we don't need to
  // introduce an extra intermediate state between
  // `ControllerPublishVolume` and `NodePublishVolume`.
  Try<Nothing> mkdir = os::mkdir(mountPath.get());
  if (mkdir.isError()) {
    return Failure("Failed to create mount path '" + mountPath.get() + "'");
  }

  return controllerClient
    .then(defer(self(), [=](csi::Client client)
        -> Future<csi::ControllerPublishVolumeResponse> {
      if (controllerCapabilities->publishUnpublishVolume) {
        // TODO(chhsiao): Use ID string once we update the CSI spec.
        csi::ControllerPublishVolumeRequest request;
        request.mutable_version()->CopyFrom(csiVersion);
        JsonStringToMessage(
            volume.disk().source().id(), request.mutable_volume_id());
        if (volume.disk().source().has_metadata()) {
          *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
              volume.disk().source().metadata()).get();
        }
        if (nodeId->has_node_id()) {
          request.mutable_node_id()->CopyFrom(nodeId->node_id());
        }
        request.set_readonly(volume.disk().volume().mode() == Volume::RO);

        return client.ControllerPublishVolume(request);
      }

      return csi::ControllerPublishVolumeResponse::default_instance();
    }))
    .then([=](const csi::ControllerPublishVolumeResponse& response) {
      return nodeClient
        .then(defer(self(), [=](csi::Client client) {
          // TODO(chhsiao): Use ID string once we update the CSI spec.
          csi::NodePublishVolumeRequest request;
          request.mutable_version()->CopyFrom(csiVersion);
          JsonStringToMessage(
              volume.disk().source().id(), request.mutable_volume_id());
          if (volume.disk().source().has_metadata()) {
            *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
                volume.disk().source().metadata()).get();
          }
          if (response.has_result() &&
              response.result().has_publish_volume_info()) {
            request.mutable_publish_volume_info()->CopyFrom(
                response.result().publish_volume_info());
          }
          request.set_target_path(mountPath.get());
          request.mutable_volume_capability()->CopyFrom(volumeCapability.get());
          request.set_readonly(volume.disk().volume().mode() == Volume::RO);

          return client.NodePublishVolume(request);
        }));
    })
    .then([] { return Nothing(); });
}


Future<Nothing> StorageLocalResourceProviderProcess::unpublishResource(
    const Resource& volume)
{
  // TODO(chhsiao): Make this a state machine and checkpoint the state!

  // NOTE: This can only be called after `loadController` and
  // `loadNode`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeId);

  CHECK(volume.has_disk());
  const Resource::DiskInfo& disk = volume.disk();

  CHECK(disk.has_source());
  CHECK(disk.source().has_id());

  Option<string> mountPath;
  if (disk.source().type() == Resource::DiskInfo::Source::PATH) {
    CHECK(disk.source().has_path());
    CHECK(disk.source().path().has_root());
    mountPath = disk.source().path().root();
  } else if (disk.source().type() == Resource::DiskInfo::Source::MOUNT) {
    CHECK(disk.source().has_mount());
    CHECK(disk.source().mount().has_root());
    mountPath = disk.source().mount().root();
  } else if (disk.source().type() == Resource::DiskInfo::Source::BLOCK) {
    mountPath = getVolumeMountPath(csiDir, disk.source().id());
  }
  CHECK_SOME(mountPath);

  if (!os::exists(mountPath.get())) {
    return Failure("Mount path '" + mountPath.get() + "' does not exist");
  }

  return nodeClient
    .then(defer(self(), [=](csi::Client client) {
      // TODO(chhsiao): Use ID string once we update the CSI spec.
      csi::NodeUnpublishVolumeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);
      JsonStringToMessage(
          volume.disk().source().id(), request.mutable_volume_id());
      if (volume.disk().source().has_metadata()) {
        *request.mutable_volume_metadata() =
          csi::labelsToVolumeMetadata(volume.disk().source().metadata()).get();
      }
      request.set_target_path(mountPath.get());

      return client.NodeUnpublishVolume(request);
    }))
    .then([=] {
      return controllerClient
        .then(defer(self(), [=](csi::Client client)
            -> Future<csi::ControllerUnpublishVolumeResponse> {
          if (controllerCapabilities->publishUnpublishVolume) {
            // TODO(chhsiao): Use ID string once we update the CSI spec.
            csi::ControllerUnpublishVolumeRequest request;
            request.mutable_version()->CopyFrom(csiVersion);
            JsonStringToMessage(
                volume.disk().source().id(), request.mutable_volume_id());
            if (volume.disk().source().has_metadata()) {
              *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
                  volume.disk().source().metadata()).get();
            }
            if (nodeId->has_node_id()) {
              request.mutable_node_id()->CopyFrom(nodeId->node_id());
            }

            return client.ControllerUnpublishVolume(request);
          }

          return csi::ControllerUnpublishVolumeResponse::default_instance();
        }));
    })
    .then(defer(self(), [=]() -> Future<Nothing> {
      Try<Nothing> rmdir = os::rmdir(mountPath.get());
      if (rmdir.isError()) {
        return Failure("Failed to remove mount path '" + mountPath.get() + "'");
      }

      return Nothing();
    }));
}


// Connects to the plugin, or launches it if it is not running.
Future<csi::Client> StorageLocalResourceProviderProcess::connect(
    const string& plugin)
{
  const string socketPath = getSocketPath(csiDir, plugin);

  // The plugin bootstrap works as follows:
  // 1. Launch a container with the `CSI_ENDPOINT` environment varible
  //    set to `unix://<socketPath>`.
  // 2. Wait for the socket file to appear. The plugin should create and
  //    bind to the socket file.
  // 3. Establish a connection to the socket file once it exists.
  // However, the above procedure only works if the socket file does not
  // exist at the beginning. If the socket file alreay exists, we call
  // `GetSupportedVersions` to the socket first to check if the plugin
  // is already running. If not, we unlink the socket file before
  // bootstrapping the plugin.

  shared_ptr<Option<csi::Client>> client(new Option<csi::Client>);
  Promise<csi::GetSupportedVersionsResponse> connection;

  if (os::exists(socketPath)) {
    *client = csi::Client("unix://" + socketPath, runtime);
    connection.associate((*client)->GetSupportedVersions(
        csi::GetSupportedVersionsRequest::default_instance()));
  } else {
    connection.fail("File '" + socketPath + "' not found");
  }

  return connection.future()
    .repair(defer(self(), [=](
        const Future<csi::GetSupportedVersionsResponse>& future)
        -> Future<csi::GetSupportedVersionsResponse> {
      Promise<Nothing> bootstrap;

      if (os::exists(socketPath)) {
        // Try to kill the plugin and remove the socket file to ensure
        // that it is not running.
        bootstrap.associate(kill(plugin));
      } else {
        bootstrap.set(Nothing());
      }

      return bootstrap.future()
        .then(defer(self(), &Self::launch, plugin))
        .then(defer(self(), [=](csi::Client _client) {
          *client = _client;
          return _client.GetSupportedVersions(
              csi::GetSupportedVersionsRequest::default_instance());
        }));
    }))
    .then(defer(self(), [=](const csi::GetSupportedVersionsResponse& response)
        -> Future<csi::Client> {
      CHECK_SOME(*client);

      if (contains(response.result().supported_versions(), csiVersion)) {
        return client->get();
      }

      return Failure(
          "CSI version " + stringify(csiVersion) + " is not supported");
    }));
}


// Launches a container for the plugin and waits for its socket file to
// appear, with a timeout specified by `CSI_ENDPOINT_CREATION_TIMEOUT`.
// The socket file should not exist prior to launch.
Future<csi::Client> StorageLocalResourceProviderProcess::launch(
    const string& plugin)
{
  const string socketPath = getSocketPath(csiDir, plugin);

  CHECK(!os::exists(socketPath));

  const ContainerID containerId = getContainerID(info, plugin);

  Option<CSIPluginInfo> config;
  foreach (const CSIPluginInfo& _config, info.storage().csi_plugins()) {
    if (_config.name() == plugin) {
      config = _config;
      break;
    }
  }
  CHECK_SOME(config);

  agent::Call call;
  call.set_type(agent::Call::LAUNCH_CONTAINER);

  agent::Call::LaunchContainer* launch = call.mutable_launch_container();
  launch->mutable_container_id()->CopyFrom(containerId);
  launch->mutable_resources()->CopyFrom(config->resources());

  ContainerInfo* container = launch->mutable_container();

  if (config->has_container()) {
    container->CopyFrom(config->container());
  } else {
    container->set_type(ContainerInfo::MESOS);
  }

  // Prepare a volume where the socket file will be placed.
  Volume* volume = container->add_volumes();
  volume->set_mode(Volume::RW);
  volume->set_container_path(csiDir);
  volume->set_host_path(csiDir);

  CommandInfo* command = launch->mutable_command();

  if (config->has_command()) {
    command->CopyFrom(config->command());
  }

  // Set the `CSI_ENDPOINT` environment variable.
  Environment::Variable* endpoint =
    command->mutable_environment()->add_variables();
  endpoint->set_name("CSI_ENDPOINT");
  endpoint->set_value("unix://" + socketPath);

  // Launch the plugin and wait for the socket file.
  return http::post(
      extractParentEndpoint(url),
      getAuthHeader(authToken),
      internal::serialize(contentType, evolve(call)),
      stringify(contentType))
    .then(defer(self(), [=](
        const http::Response& response) -> Future<csi::Client> {
      if (response.status != http::OK().status &&
          response.status != http::Accepted().status) {
        return Failure(
            "Failed to launch container '" + stringify(containerId) +
            "': Unexpected response '" + response.status + "' (" +
            response.body + ")");
      }

      Timeout timeout = Timeout::in(CSI_ENDPOINT_CREATION_TIMEOUT);

      return loop(
          self(),
          [=]() -> Future<Nothing> {
            if (timeout.expired()) {
              return Failure(
                  "Timed out waiting for plugin to create socket '" +
                  socketPath + "'");
            }

            return after(Milliseconds(10));
          },
          [=](const Nothing&) -> ControlFlow<csi::Client> {
            if (!os::exists(socketPath)) {
              return Continue();
            }

            return Break(csi::Client("unix://" + socketPath, runtime));
          });
    }));
}


// Kills the container for the plugin and remove its socket file after
// its termination, or removes the socket file if the container does not
// exist.
Future<Nothing> StorageLocalResourceProviderProcess::kill(
    const string& plugin)
{
  const ContainerID containerId = getContainerID(info, plugin);

  agent::Call call;
  call.set_type(agent::Call::KILL_CONTAINER);
  call.mutable_kill_container()->mutable_container_id()->CopyFrom(containerId);

  return http::post(
      extractParentEndpoint(url),
      getAuthHeader(authToken),
      internal::serialize(contentType, evolve(call)),
      stringify(contentType))
    .then(defer(self(), [=](const http::Response& response) -> Future<Nothing> {
      if (response.status == http::NotFound().status) {
        return Nothing();
      }

      if (response.status != http::OK().status) {
        return Failure(
            "Failed to kill container '" + stringify(containerId) +
            "': Unexpected response '" + response.status + "' (" +
            response.body + ")");
      }

      // Wait for the termination of the container.
      agent::Call call;
      call.set_type(agent::Call::WAIT_CONTAINER);
      call.mutable_wait_container()->mutable_container_id()
        ->CopyFrom(containerId);

      return http::post(
          extractParentEndpoint(url),
          getAuthHeader(authToken),
          internal::serialize(contentType, evolve(call)),
          stringify(contentType))
        .then(defer(self(), [=](
            const http::Response& response) -> Future<Nothing> {
          if (response.status == http::NotFound().status) {
            return Nothing();
          }

          if (response.status != http::OK().status) {
            return Failure(
                "Failed to wait for container '" + stringify(containerId) +
                "': Unexpected response '" + response.status + "' (" +
                response.body + ")");
          }

          return Nothing();
        }));
    }))
  .then(defer(self(), [=]() -> Future<Nothing> {
    const string socketPath = getSocketPath(csiDir, plugin);
    if (os::exists(socketPath)) {
      Try<Nothing> rm = os::rm(socketPath);
      if (rm.isError()) {
        return Failure(
            "Failed to remove socket file '" + socketPath + "': " +
            rm.error());
      }
    }

    return Nothing();
  }));
}


Try<Owned<LocalResourceProvider>> StorageLocalResourceProvider::create(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const Option<string>& authToken)
{
  if (!info.has_storage()) {
    return Error("'ResourceProviderInfo.storage' must be set");
  }

  bool hasControllerPlugin = false;
  bool hasNodePlugin = false;

  foreach (const CSIPluginInfo& plugin, info.storage().csi_plugins()) {
    hasControllerPlugin = hasControllerPlugin ||
      (plugin.name() == info.storage().controller_plugin());
    hasNodePlugin = hasNodePlugin ||
      (plugin.name() == info.storage().node_plugin());
  }

  if (!hasControllerPlugin) {
    return Error(
        "'" + info.storage().controller_plugin() + "' not found in "
        "'ResourceProviderInfo.storage.csi_plugins'");
  }

  if (!hasNodePlugin) {
    return Error(
        "'" + info.storage().node_plugin() + "' not found in "
        "'ResourceProviderInfo.storage.csi_plugins'");
  }

  return Owned<LocalResourceProvider>(
      new StorageLocalResourceProvider(url, workDir, info, authToken));
}


Try<Principal> StorageLocalResourceProvider::principal(
    const ResourceProviderInfo& info)
{
  return Principal(Option<string>::none(), {{"cid_prefix", getPrefix(info)}});
}


StorageLocalResourceProvider::StorageLocalResourceProvider(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const Option<string>& authToken)
  : process(new StorageLocalResourceProviderProcess(
        url, workDir, info, authToken))
{
  spawn(CHECK_NOTNULL(process.get()));
}


StorageLocalResourceProvider::~StorageLocalResourceProvider()
{
  terminate(process.get());
  wait(process.get());
}

} // namespace internal {
} // namespace mesos {
