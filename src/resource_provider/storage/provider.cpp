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
#include <stout/hashmap.hpp>
#include <stout/os.hpp>

#include "common/http.hpp"

#include "csi/client.hpp"
#include "csi/utils.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "resource_provider/detector.hpp"

#include "resource_provider/storage/paths.hpp"

#include "slave/container_daemon.hpp"
#include "slave/paths.hpp"

namespace http = process::http;

using std::list;
using std::queue;
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

using process::http::authentication::Principal;

using mesos::ResourceProviderInfo;
using mesos::Resources;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using mesos::v1::resource_provider::Driver;

namespace mesos {
namespace internal {

static const string SLRP_NAME_PREFIX = "mesos-slrp-";

// We use percent-encoding to escape '.' (reserved for standalone
// containers) and '-' (reserved as a separator) for names, in addition
// to the characters reserved in RFC 3986.
static const string SLRP_NAME_RESERVED = ".-";

// Timeout for a CSI plugin to create its endpoint socket.
static const Duration CSI_ENDPOINT_CREATION_TIMEOUT = Seconds(5);
static const uint32_t CSI_MAX_ENTRIES = 100;

// Backoff period between connection checks for a CSI plugin.
static const Duration CSI_CONNECTION_BACKOFF_FACTOR = Minutes(1);


// Returns a prefix for naming components of the resource provider. This
// can be used in various places, such as container IDs for CSI plugins.
static inline string getPrefix(const ResourceProviderInfo& info)
{
  return SLRP_NAME_PREFIX + http::encode(info.name(), SLRP_NAME_RESERVED) + "-";
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


// Convenient function to check if an iterable contains a value.
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
      const SlaveID& _slaveId,
      const Option<string>& _authToken)
    : ProcessBase(process::ID::generate("storage-local-resource-provider")),
      url(_url),
      workDir(_workDir),
      metaDir(slave::paths::getMetaRootDir(_workDir)),
      contentType(ContentType::PROTOBUF),
      info(_info),
      slaveId(_slaveId),
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
  void fatal(const string& messsage, const string& failure);

  // Functions for received events.
  void subscribed(const Event::Subscribed& subscribed);
  void operation(const Event::Operation& operation);
  void publish(const Event::Publish& publish);

  Future<csi::Client> connect(const string& endpoint);
  Future<csi::Client> getService(const string& plugin);

  Future<Nothing> prepareControllerPlugin();
  Future<Nothing> prepareNodePlugin();
  Future<Nothing> importResources();
  Future<Nothing> publishResource(const Resource& resource);
  Future<Nothing> unpublishResource(const Resource& resource);

  const http::URL url;
  const string workDir;
  const string metaDir;
  const ContentType contentType;
  ResourceProviderInfo info;
  const SlaveID slaveId;
  const Option<string> authToken;

  csi::Version csiVersion;
  process::grpc::client::Runtime runtime;
  Owned<v1::resource_provider::Driver> driver;

  hashmap<string, Owned<ContainerDaemon>> daemons;
  hashmap<string, Owned<Promise<csi::Client>>> services;
  Option<csi::GetPluginInfoResponse::Result> controllerInfo;
  Option<csi::GetPluginInfoResponse::Result> nodeInfo;
  Option<csi::ControllerCapabilities> controllerCapabilities;
  Option<csi::NodeCapabilities> nodeCapabilities;
  Option<csi::GetNodeIDResponse::Result> nodeId;

  string resourceVersionUuid;
  Resources resources;
};


void StorageLocalResourceProviderProcess::connected()
{
  const string message =
    "Failed to subscribe resource provider with type '" + info.type() +
    "' and name '" + info.name() + "'";

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();
  subscribe->mutable_resource_provider_info()->CopyFrom(info);

  driver->send(evolve(call))
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
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
  // Recover the resource provider ID from the latest symlink. If the
  // symlink cannot be resolved, treat this as a new resource provider.
  // TODO(chhsiao): State recovery.
  const string latest = slave::paths::getLatestResourceProviderPath(
      metaDir,
      slaveId,
      info.type(),
      info.name());
  Result<string> realpath = os::realpath(latest);
  if (realpath.isSome()) {
    info.mutable_id()->set_value(Path(realpath.get()).basename());
  }

  // Set CSI version to 0.1.0.
  csiVersion.set_major(0);
  csiVersion.set_minor(1);
  csiVersion.set_patch(0);

  // Prepare the directory where the mount points will be placed.
  const string volumesDir = storage::paths::getCsiVolumeRootDir(
      slave::paths::getResourceProviderAgentRootDir(
          workDir,
          info.type(),
          info.name()));
  Try<Nothing> mkdir = os::mkdir(volumesDir);
  if (mkdir.isError()) {
    fatal("Failed to create directory '" + volumesDir + "'", mkdir.error());
  }

  const string message =
    "Failed to initialize resource provider with type '" + info.type() +
    "' and name '" + info.name();

  // NOTE: Most resource provider events rely on the plugins being
  // prepared. To avoid race conditions, we connect to the agent after
  // preparing the plugins.
  // NOTE: Currently, CSI does not have a `ProbeController` call, so we
  // we rely on `ProbeNode` to validate the runtime environment.
  prepareNodePlugin()
    .then(defer(self(), &Self::prepareControllerPlugin))
    .then(defer(self(), [=] {
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

      return Nothing();
    }))
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
}


void StorageLocalResourceProviderProcess::fatal(
    const string& message,
    const string& failure)
{
  LOG(ERROR) << message << ": " << failure;
  process::terminate(self());
}


void StorageLocalResourceProviderProcess::subscribed(
    const Event::Subscribed& subscribed)
{
  LOG(INFO) << "Subscribed with ID " << subscribed.provider_id().value();

  if (!info.has_id()) {
    // New subscription.
    info.mutable_id()->CopyFrom(subscribed.provider_id());
    slave::paths::createResourceProviderDirectory(
        metaDir,
        slaveId,
        info.type(),
        info.name(),
        info.id());
  }

  const string message =
    "Failed to update state for resource provider " + stringify(info.id());

  // Import resources after obtaining the resource provider ID.
  // TODO(chhsiao): Resources reconciliation.
  importResources()
    .then(defer(self(), [=] {
      Call call;
      call.set_type(Call::UPDATE_STATE);
      call.mutable_resource_provider_id()->CopyFrom(info.id());

      Call::UpdateState* update = call.mutable_update_state();
      update->mutable_resources()->CopyFrom(resources);
      update->set_resource_version_uuid(resourceVersionUuid);

      return driver->send(evolve(call));
    }))
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
}


void StorageLocalResourceProviderProcess::operation(
    const Event::Operation& operation)
{
}


void StorageLocalResourceProviderProcess::publish(const Event::Publish& publish)
{
  list<Future<Nothing>> futures;

  foreach (const Resource& resource, publish.resources()) {
    futures.push_back(publishResource(resource));
  }

  // TODO(chhsiao): Return a nack for failed resource publication.
  // TODO(chhsiao): Capture `publish` by forwarding once we switch to C++14.
  collect(futures)
    .onReady(defer(self(), [=](const list<Nothing>& future) {
      const string message =
        "Failed to acknowledge resource publication for resource provider " +
        stringify(info.id());

      Call call;
      call.set_type(Call::PUBLISHED);
      call.mutable_resource_provider_id()->CopyFrom(info.id());
      call.mutable_published()->set_uuid(publish.uuid());

      driver->send(evolve(call))
        .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
        .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
    }));
}


// Returns a future of a CSI client that waits for the endpoint socket
// to appear if necessary, then connects to the socket and check its
// supported version.
Future<csi::Client> StorageLocalResourceProviderProcess::connect(
    const string& endpoint)
{
  Future<csi::Client> client;

  if (os::exists(endpoint)) {
    client = csi::Client("unix://" + endpoint, runtime);
  } else {
    // Wait for the endpoint socket to appear until the timeout expires.
    Timeout timeout = Timeout::in(CSI_ENDPOINT_CREATION_TIMEOUT);

    client = loop(
        self(),
        [=]() -> Future<Nothing> {
          if (timeout.expired()) {
            return Failure("Timed out waiting for endpoint '" + endpoint + "'");
          }

          return after(Milliseconds(10));
        },
        [=](const Nothing&) -> ControlFlow<csi::Client> {
          if (os::exists(endpoint)) {
            return Break(csi::Client("unix://" + endpoint, runtime));
          }

          return Continue();
        });
  }

  return client
    .then(defer(self(), [=](csi::Client client) {
      return client.GetSupportedVersions(
          csi::GetSupportedVersionsRequest::default_instance())
        .then(defer(self(), [=](
            const csi::GetSupportedVersionsResponse& response)
            -> Future<csi::Client> {
          if (!contains(response.result().supported_versions(), csiVersion)) {
            return Failure(
                "CSI version " + stringify(csiVersion) + " is not supported");
          }

          return client;
        }));
    }));
}


// Returns a future of the latest CSI client for the specified plugin.
// If the plugin is not already running, this method will start a new
// a new container daemon..
Future<csi::Client> StorageLocalResourceProviderProcess::getService(
    const string& plugin)
{
  if (daemons.contains(plugin)) {
    CHECK(services.contains(plugin));
    return services.at(plugin)->future();
  }

  Option<CSIPluginInfo> config;
  foreach (const CSIPluginInfo& _config, info.storage().csi_plugins()) {
    if (_config.name() == plugin) {
      config = _config;
      break;
    }
  }
  CHECK_SOME(config);

  Try<string> endpoint = storage::paths::getCsiEndpointPath(
      slave::paths::getResourceProviderAgentRootDir(
          workDir,
          info.type(),
          info.name()),
      plugin);

  if (endpoint.isError()) {
    return Failure(
        "Failed to resolve endpoint path for plugin '" + plugin + "': " +
        endpoint.error());
  }

  const string& endpointPath = endpoint.get();
  const string endpointDir = Path(endpointPath).dirname();
  const string mountDir = storage::paths::getCsiVolumeRootDir(
      slave::paths::getResourceProviderAgentRootDir(
          workDir,
          info.type(),
          info.name()));

  CommandInfo commandInfo;

  if (config->has_command()) {
    commandInfo.CopyFrom(config->command());
  }

  // Set the `CSI_ENDPOINT` environment variable.
  Environment::Variable* endpointVar =
    commandInfo.mutable_environment()->add_variables();
  endpointVar->set_name("CSI_ENDPOINT");
  endpointVar->set_value("unix://" + endpointPath);

  ContainerInfo containerInfo;

  if (config->has_container()) {
    containerInfo.CopyFrom(config->container());
  } else {
    containerInfo.set_type(ContainerInfo::MESOS);
  }

  // Prepare a volume where the endpoint socket will be placed.
  Volume* endpointVolume = containerInfo.add_volumes();
  endpointVolume->set_mode(Volume::RW);
  endpointVolume->set_container_path(endpointDir);
  endpointVolume->set_host_path(endpointDir);

  // Prepare a volume where the mount points will be placed.
  Volume* mountVolume = containerInfo.add_volumes();
  mountVolume->set_mode(Volume::RW);
  mountVolume->set_container_path(mountDir);
  mountVolume->mutable_source()->set_type(Volume::Source::HOST_PATH);
  mountVolume->mutable_source()->mutable_host_path()->set_path(mountDir);
  mountVolume->mutable_source()->mutable_host_path()
    ->mutable_mount_propagation()->set_mode(MountPropagation::BIDIRECTIONAL);

  CHECK(!services.contains(plugin));
  services[plugin].reset(new Promise<csi::Client>());

  Try<Owned<ContainerDaemon>> daemon = ContainerDaemon::create(
      extractParentEndpoint(url),
      authToken,
      getContainerID(info, plugin),
      commandInfo,
      config->resources(),
      containerInfo,
      defer(self(), [=] {
        CHECK(services.at(plugin)->future().isPending());

        return connect(endpointPath)
          .then(defer(self(), [=](const csi::Client& client) {
            services.at(plugin)->set(client);
            return Nothing();
          }))
          .onFailed(defer(self(), [=](const string& failure) {
            services.at(plugin)->fail(failure);
          }))
          .onDiscarded(defer(self(), [=] {
            services.at(plugin)->discard();
          }));
      }),
      defer(self(), [=]() -> Future<Nothing> {
        services.at(plugin)->discard();
        services.at(plugin).reset(new Promise<csi::Client>());

        if (os::exists(endpointPath)) {
          Try<Nothing> rm = os::rm(endpoint.get());
          if (rm.isError()) {
            return Failure(
                "Failed to remove endpoint '" + endpoint.get() + "': " +
                rm.error());
          }
        }

        return Nothing();
      }));

  if (daemon.isError()) {
    return Failure(daemon.error());
  }

  daemons[plugin] = daemon.get();

  return services.at(plugin)->future();
}


Future<Nothing> StorageLocalResourceProviderProcess::prepareControllerPlugin()
{
  return getService(info.storage().controller_plugin())
    .then(defer(self(), [=](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then(defer(self(), [=](const csi::GetPluginInfoResponse& response) {
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

          // NOTE: We always get the latest service future before
          // proceeding to the next step.
          return getService(info.storage().controller_plugin());
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Get the controller capabilities.
      csi::ControllerGetCapabilitiesRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ControllerGetCapabilities(request)
        .then(defer(self(), [=](
            const csi::ControllerGetCapabilitiesResponse& response) {
          controllerCapabilities = response.result().capabilities();

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::prepareNodePlugin()
{
  return getService(info.storage().node_plugin())
    .then(defer(self(), [=](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then(defer(self(), [=](const csi::GetPluginInfoResponse& response) {
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

          // NOTE: We always get the latest service future before
          // proceeding to the next step.
          return getService(info.storage().node_plugin());
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Probe the plugin to validate the runtime environment.
      csi::ProbeNodeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ProbeNode(request)
        .then(defer(self(), [=](const csi::ProbeNodeResponse& response) {
          return getService(info.storage().node_plugin());
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Get the node capabilities.
      csi::NodeGetCapabilitiesRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.NodeGetCapabilities(request)
        .then(defer(self(), [=](
            const csi::NodeGetCapabilitiesResponse& response) {
          nodeCapabilities = response.result().capabilities();

          return getService(info.storage().node_plugin());
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Get the node ID.
      csi::GetNodeIDRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetNodeID(request)
        .then(defer(self(), [=](const csi::GetNodeIDResponse& response) {
          nodeId = response.result();

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::importResources()
{
  // NOTE: This can only be called after `prepareControllerPlugin()` and
  // the resource provider ID has been obtained.
  CHECK_SOME(controllerCapabilities);
  CHECK(info.has_id());

  return getService(info.storage().controller_plugin())
    .then(defer(self(), [=](csi::Client client) {
      list<Future<Resources>> futures;

      if (controllerCapabilities->getCapacity) {
        // TODO(chhsiao): Query the capacity for each profile.
        csi::GetCapacityRequest request;
        request.mutable_version()->CopyFrom(csiVersion);

        futures.emplace_back(client.GetCapacity(request)
          .then(defer(self(), [=](const csi::GetCapacityResponse& response) {
            Resources resources;

            // TODO(chhsiao): Here we assume that `total_capacity` is
            // the available capacity to follow the latest CSI spec.
            // TODO(chhsiao): Add support for default role.
            Resource resource = Resources::parse(
                "disk",
                stringify(response.result().total_capacity()),
                "*").get();
            resource.mutable_provider_id()->CopyFrom(info.id());
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
              resource.mutable_provider_id()->CopyFrom(info.id());

              // TODO(chhsiao): Use ID string once we update the CSI spec.
              resource.mutable_disk()->mutable_source()->set_id(
                  stringify(volume.id()));
              if (volume.has_metadata()) {
                *resource.mutable_disk()->mutable_source()->mutable_metadata() =
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
        .then(defer(self(), [=](const list<Future<Resources>>& futures) {
          foreach (const Future<Resources>& future, futures) {
            if (future.isReady()) {
              resources += future.get();
            } else {
              LOG(ERROR)
                << "Failed to get resources for resource provider "
                << info.id() << "': "
                << (future.isFailed() ? future.failure() : "future discarded");
            }
          }

          resourceVersionUuid = UUID::random().toBytes();

          LOG(INFO)
            << "Total resources of type '" << info.type() << "' and name '"
            << info.name() << "': " << resources;

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::publishResource(
    const Resource& resource)
{
  // TODO(chhsiao): Make this a state machine and checkpoint the state!
  // TODO(chhsiao): Check if the resource is in the checkpointed resources.

  // NOTE: This can only be called after `prepareController` and
  // `prepareNode`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeCapabilities);
  CHECK_SOME(nodeId);

  CHECK(resource.has_disk());
  const Resource::DiskInfo& disk = resource.disk();

  CHECK(disk.has_source());
  CHECK(disk.source().has_id());

  // TODO(chhsiao): Add BLOCK support.
  if (disk.source().type() == Resource::DiskInfo::Source::BLOCK) {
    return  Failure("BLOCK disk resource is not supported.");
  }

  Option<string> targetPath;
  if (disk.source().type() == Resource::DiskInfo::Source::PATH) {
    // We expect `disk.source.path.root` relative to agent work dir.
    CHECK(disk.source().has_path());
    CHECK(disk.source().path().has_root());
    CHECK(!path::absolute(disk.source().path().root()));
    targetPath = path::join(workDir, disk.source().path().root());
  } else if (disk.source().type() == Resource::DiskInfo::Source::MOUNT) {
    // We expect `disk.source.mount.root` relative to agent work dir.
    CHECK(disk.source().has_mount());
    CHECK(disk.source().mount().has_root());
    CHECK(!path::absolute(disk.source().mount().root()));
    targetPath = path::join(workDir, disk.source().mount().root());
  }
  CHECK_SOME(targetPath);

  // Pick the first resource capability from those we used to create
  // the resource.
  // TODO(chhsiao): Get the resource capability based on the profile.
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
    return Failure("Failed to select a resource capability");
  }

  // NOTE: We create the mount point first, so the we don't need to
  // introduce an extra intermediate state between
  // `ControllerPublishVolume` and `NodePublishVolume`.
  Try<Nothing> mkdir = os::mkdir(targetPath.get());
  if (mkdir.isError()) {
    return Failure("Failed to create target path '" + targetPath.get() + "'");
  }

  return getService(info.storage().controller_plugin())
    .then(defer(self(), [=](csi::Client client)
        -> Future<csi::ControllerPublishVolumeResponse> {
      if (controllerCapabilities->publishUnpublishVolume) {
        // TODO(chhsiao): Use ID string once we update the CSI spec.
        // TODO(chhsiao): Set the readonly flag properly.
        csi::ControllerPublishVolumeRequest request;
        request.mutable_version()->CopyFrom(csiVersion);
        JsonStringToMessage(
            resource.disk().source().id(), request.mutable_volume_id());
        if (resource.disk().source().has_metadata()) {
          *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
              resource.disk().source().metadata()).get();
        }
        if (nodeId->has_node_id()) {
          request.mutable_node_id()->CopyFrom(nodeId->node_id());
        }
        request.set_readonly(false);

        return client.ControllerPublishVolume(request);
      }

      return csi::ControllerPublishVolumeResponse::default_instance();
    }))
    .then(defer(self(), [=](
        const csi::ControllerPublishVolumeResponse& response) {
      return getService(info.storage().node_plugin())
        .then(defer(self(), [=](csi::Client client) {
          // TODO(chhsiao): Use ID string once we update the CSI spec.
          // TODO(chhsiao): Set the readonly flag properly.
          csi::NodePublishVolumeRequest request;
          request.mutable_version()->CopyFrom(csiVersion);
          JsonStringToMessage(
              resource.disk().source().id(), request.mutable_volume_id());
          if (resource.disk().source().has_metadata()) {
            *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
                resource.disk().source().metadata()).get();
          }
          if (response.has_result() &&
              response.result().has_publish_volume_info()) {
            request.mutable_publish_volume_info()->CopyFrom(
                response.result().publish_volume_info());
          }
          request.set_target_path(targetPath.get());
          request.mutable_volume_capability()->CopyFrom(volumeCapability.get());
          request.set_readonly(false);

          return client.NodePublishVolume(request);
        }));
    }))
    .then([] { return Nothing(); });
}


Future<Nothing> StorageLocalResourceProviderProcess::unpublishResource(
    const Resource& resource)
{
  // TODO(chhsiao): Make this a state machine and checkpoint the state!

  // NOTE: This can only be called after `prepareController` and
  // `prepareNode`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeId);

  CHECK(resource.has_disk());
  const Resource::DiskInfo& disk = resource.disk();

  CHECK(disk.has_source());
  CHECK(disk.source().has_id());

  // TODO(chhsiao): Add BLOCK support.
  if (disk.source().type() == Resource::DiskInfo::Source::BLOCK) {
    return  Failure("BLOCK disk resource is not supported.");
  }

  Option<string> targetPath;
  if (disk.source().type() == Resource::DiskInfo::Source::PATH) {
    // We expect `disk.source.path.root` relative to agent work dir.
    CHECK(disk.source().has_path());
    CHECK(disk.source().path().has_root());
    CHECK(!path::absolute(disk.source().path().root()));
    targetPath = path::join(workDir, disk.source().path().root());
  } else if (disk.source().type() == Resource::DiskInfo::Source::MOUNT) {
    // We expect `disk.source.mount.root` relative to agent work dir.
    CHECK(disk.source().has_mount());
    CHECK(disk.source().mount().has_root());
    CHECK(!path::absolute(disk.source().mount().root()));
    targetPath = path::join(workDir, disk.source().mount().root());
  }
  CHECK_SOME(targetPath);

  if (!os::exists(targetPath.get())) {
    // The resource has not been published yet.
    return Nothing();
  }

  return getService(info.storage().node_plugin())
    .then(defer(self(), [=](csi::Client client) {
      // TODO(chhsiao): Use ID string once we update the CSI spec.
      csi::NodeUnpublishVolumeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);
      JsonStringToMessage(
          resource.disk().source().id(), request.mutable_volume_id());
      if (resource.disk().source().has_metadata()) {
        *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
            resource.disk().source().metadata()).get();
      }
      request.set_target_path(targetPath.get());

      return client.NodeUnpublishVolume(request);
    }))
    .then(defer(self(), [=] {
      return getService(info.storage().controller_plugin())
        .then(defer(self(), [=](csi::Client client)
            -> Future<csi::ControllerUnpublishVolumeResponse> {
          if (controllerCapabilities->publishUnpublishVolume) {
            // TODO(chhsiao): Use ID string once we update the CSI spec.
            csi::ControllerUnpublishVolumeRequest request;
            request.mutable_version()->CopyFrom(csiVersion);
            JsonStringToMessage(
                resource.disk().source().id(), request.mutable_volume_id());
            if (resource.disk().source().has_metadata()) {
              *request.mutable_volume_metadata() = csi::labelsToVolumeMetadata(
                  resource.disk().source().metadata()).get();
            }
            if (nodeId->has_node_id()) {
              request.mutable_node_id()->CopyFrom(nodeId->node_id());
            }

            return client.ControllerUnpublishVolume(request);
          }

          return csi::ControllerUnpublishVolumeResponse::default_instance();
        }));
    }))
    .then(defer(self(), [=]() -> Future<Nothing> {
      Try<Nothing> rmdir = os::rmdir(targetPath.get(), false);
      if (rmdir.isError()) {
        return Failure(
            "Failed to remove target path '" + targetPath.get() + "'");
      }

      return Nothing();
    }));
}


Try<Owned<LocalResourceProvider>> StorageLocalResourceProvider::create(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const SlaveID& slaveId,
    const Option<string>& authToken)
{
  if (!info.has_storage()) {
    return Error("'ResourceProviderInfo.storage' must be set");
  }

  bool hasControllerPlugin = false;
  bool hasNodePlugin = false;

  foreach (const CSIPluginInfo& plugin, info.storage().csi_plugins()) {
    if (plugin.name() == info.storage().controller_plugin()) {
      hasControllerPlugin = true;
    }
    if (plugin.name() == info.storage().node_plugin()) {
      hasNodePlugin = true;
    }
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
      new StorageLocalResourceProvider(url, workDir, info, slaveId, authToken));
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
    const SlaveID& slaveId,
    const Option<string>& authToken)
  : process(new StorageLocalResourceProviderProcess(
        url, workDir, info, slaveId, authToken))
{
  spawn(CHECK_NOTNULL(process.get()));
}


StorageLocalResourceProvider::~StorageLocalResourceProvider()
{
  process::terminate(process.get());
  process::wait(process.get());
}

} // namespace internal {
} // namespace mesos {
