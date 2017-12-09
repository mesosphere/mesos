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

#include <algorithm>
#include <cctype>
#include <numeric>

#include <glog/logging.h>

#include <process/after.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/process.hpp>
#include <process/sequence.hpp>
#include <process/timeout.hpp>

#include <mesos/resources.hpp>
#include <mesos/type_utils.hpp>

#include <mesos/resource_provider/resource_provider.hpp>

#include <mesos/v1/resource_provider.hpp>

#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/linkedhashmap.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include <stout/os/realpath.hpp>

#include "common/http.hpp"
#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"

#include "csi/client.hpp"
#include "csi/paths.hpp"
#include "csi/state.hpp"
#include "csi/utils.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "resource_provider/detector.hpp"
#include "resource_provider/state.hpp"

#include "slave/container_daemon.hpp"
#include "slave/paths.hpp"
#include "slave/state.hpp"

namespace http = process::http;

using std::accumulate;
using std::find;
using std::list;
using std::queue;
using std::shared_ptr;
using std::string;
using std::vector;

using process::Break;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::Promise;
using process::Sequence;
using process::Timeout;

using process::after;
using process::collect;
using process::defer;
using process::loop;
using process::spawn;
using process::undiscardable;

using process::http::authentication::Principal;

using mesos::internal::protobuf::convertLabelsToStringMap;
using mesos::internal::protobuf::convertStringMapToLabels;

using mesos::internal::slave::ContainerDaemon;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using mesos::resource_provider::state::ResourceProviderState;

using mesos::v1::resource_provider::Driver;

namespace mesos {
namespace internal {

// Returns true if the string is a valid Java identifier.
static bool isValidName(const string& s)
{
  if (s.empty()) {
    return false;
  }

  foreach (const char c, s) {
    if (!isalnum(c) && c != '_') {
      return false;
    }
  }

  return true;
}


// Returns true if the string is a valid Java package name.
static bool isValidType(const string& s)
{
  if (s.empty()) {
    return false;
  }

  foreach (const string& token, strings::split(s, ".")) {
    if (!isValidName(token)) {
      return false;
    }
  }

  return true;
}


// Timeout for a CSI plugin component to create its endpoint socket.
static const Duration CSI_ENDPOINT_CREATION_TIMEOUT = Seconds(5);


// Returns a prefix for naming standalone containers to run CSI plugins
// for the resource provider. The prefix is of the following format:
//     <rp_type>-<rp_name>--
// where <rp_type> and <rp_name> are the type and name of the resource
// provider, with dots replaced by dashes. We use a double-dash at the
// end to explicitly mark the end of the prefix.
static inline string getContainerIdPrefix(const ResourceProviderInfo& info)
{
  return strings::join(
      "-",
      strings::replace(info.type(), ".", "-"),
      info.name(),
      "-");
}


// Returns the container ID of the standalone container to run a CSI
// plugin component. The container ID is of the following format:
//     <rp_type>-<rp_name>--<csi_type>-<csi_name>--<list_of_services>
// where <rp_type> and <rp_name> are the type and name of the resource
// provider, and <csi_type> and <csi_name> are the type and name of the
// CSI plugin, with dots replaced by dashes. <list_of_services> lists
// the CSI services provided by the component, concatenated with dashes.
static inline ContainerID getContainerId(
    const ResourceProviderInfo& info,
    const CSIPluginContainerInfo& container)
{
  string value = getContainerIdPrefix(info);

  value += strings::join(
      "-",
      strings::replace(info.storage().plugin().type(), ".", "-"),
      info.storage().plugin().name(),
      "");

  for (int i = 0; i < container.services_size(); i++) {
    value += "-" + stringify(container.services(i));
  }

  ContainerID containerId;
  containerId.set_value(value);

  return containerId;
}


static Option<CSIPluginContainerInfo> getCSIPluginContainerInfo(
    const ResourceProviderInfo& info,
    const ContainerID& containerId)
{
  foreach (const CSIPluginContainerInfo& container,
           info.storage().plugin().containers()) {
    if (getContainerId(info, container) == containerId) {
      return container;
    }
  }

  return None();
}


// Returns the parent endpoint as a URL.
// TODO(jieyu): Consider using a more reliable way to get the agent v1
// operator API endpoint URL.
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


static inline Resource createRawDiskResource(
    const ResourceProviderID& resourceProviderId,
    double capacity,
    const Option<string>& profile,
    const Option<string>& id = None(),
    const Option<Labels>& metadata = None())
{
  Resource resource;
  resource.set_name("disk");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(capacity);
  resource.mutable_provider_id()->CopyFrom(resourceProviderId),
  resource.mutable_disk()->mutable_source()
    ->set_type(Resource::DiskInfo::Source::RAW);

  if (profile.isSome()) {
    resource.mutable_disk()->mutable_source()->set_profile(profile.get());
  }

  if (id.isSome()) {
    resource.mutable_disk()->mutable_source()->set_id(id.get());
  }

  if (metadata.isSome()) {
    resource.mutable_disk()->mutable_source()->mutable_metadata()
      ->CopyFrom(metadata.get());
  }

  return resource;
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
      state(RECOVERING),
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
  struct ProfileData
  {
    csi::VolumeCapability capability;
    google::protobuf::Map<string, string> parameters;
  };

  struct VolumeData
  {
    VolumeData(const csi::state::VolumeState& _state)
      : state(_state), sequence(new Sequence("volume-sequence")) {}

    csi::state::VolumeState state;

    // We run all CSI operations for the same volume on a sequence to
    // ensure that they are processed in a sequential order.
    Owned<Sequence> sequence;
  };

  void initialize() override;
  void fatal(const string& messsage, const string& failure);

  Future<Nothing> recover();
  Future<Nothing> recoverServices();
  Future<Nothing> recoverVolumes();
  void doReliableRegistration();
  Future<Nothing> reconcile();

  // Functions for received events.
  void subscribed(const Event::Subscribed& subscribed);
  void applyOfferOperation(const Event::ApplyOfferOperation& operation);
  void publishResources(const Event::PublishResources& publish);
  void acknowledgeOfferOperation(
      const Event::AcknowledgeOfferOperation& acknowledge);
  void reconcileOfferOperations(
      const Event::ReconcileOfferOperations& reconcile);

  Future<csi::Client> connect(const string& endpoint);
  Future<csi::Client> getService(const ContainerID& containerId);
  Future<Nothing> killService(const ContainerID& containerId);

  Future<Nothing> prepareControllerService();
  Future<Nothing> prepareNodeService();
  Future<Resources> importResources();
  Future<Nothing> controllerPublish(const string& volumeId);
  Future<Nothing> controllerUnpublish(const string& volumeId);
  Future<Nothing> nodePublish(const string& volumeId);
  Future<Nothing> nodeUnpublish(const string& volumeId);

  // Returns a CSI volume ID.
  Future<string> createVolume(
      const string& name,
      const Bytes& capacity,
      const ProfileData& profile);
  Future<Nothing> deleteVolume(const string& volumeId);

  // Applies the offer operation. Conventional operations will be
  // synchoronusly applied.
  Future<Nothing> applyOfferOperation(const UUID& operationUuid);

  Future<vector<ResourceConversion>> applyCreateVolumeOrBlock(
      const Resource& resource,
      const UUID& operationUuid,
      const Resource::DiskInfo::Source::Type& type);
  Future<vector<ResourceConversion>> applyDestroyVolumeOrBlock(
      const Resource& resource);

  // Synchronously update `totalResources` and the offer operation status.
  Try<Nothing> applyResourceConversions(
      const UUID& operationUuid,
      const Try<vector<ResourceConversion>>& conversions);

  void checkpointResourceProviderState();
  void sendResourceProviderStateUpdate();
  void checkpointVolumeState(const string& volumeId);

  enum State
  {
    RECOVERING,
    DISCONNECTED,
    CONNECTED,
    SUBSCRIBED,
    READY
  } state;

  const http::URL url;
  const string workDir;
  const string metaDir;
  const ContentType contentType;
  ResourceProviderInfo info;
  const SlaveID slaveId;
  const Option<string> authToken;

  csi::Version csiVersion;
  string bootId;
  hashmap<string, ProfileData> profiles;
  process::grpc::client::Runtime runtime;
  Owned<v1::resource_provider::Driver> driver;

  ContainerID controllerContainerId;
  ContainerID nodeContainerId;
  hashmap<ContainerID, Owned<ContainerDaemon>> daemons;
  hashmap<ContainerID, Owned<Promise<csi::Client>>> services;

  Option<csi::GetPluginInfoResponse> controllerInfo;
  Option<csi::GetPluginInfoResponse> nodeInfo;
  Option<csi::ControllerCapabilities> controllerCapabilities;
  Option<string> nodeId;

  // NOTE: We store the list of pending operations in a `LinkedHashMap`
  // to preserve the order we receive the operations. This is useful
  // when we replay depending operations during recovery.
  LinkedHashMap<UUID, Event::ApplyOfferOperation> pendingOperations;
  Resources totalResources;
  Option<UUID> resourceVersion;
  hashmap<string, VolumeData> volumes;
};


void StorageLocalResourceProviderProcess::connected()
{
  CHECK_EQ(DISCONNECTED, state);

  state = CONNECTED;

  doReliableRegistration();
}


void StorageLocalResourceProviderProcess::disconnected()
{
  CHECK(state == CONNECTED || state == SUBSCRIBED || state == READY);

  LOG(INFO) << "Disconnected from resource provider manager";

  state = DISCONNECTED;
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
    case Event::APPLY_OFFER_OPERATION: {
      CHECK(event.has_apply_offer_operation());
      applyOfferOperation(event.apply_offer_operation());
      break;
    }
    case Event::PUBLISH_RESOURCES: {
      CHECK(event.has_publish_resources());
      publishResources(event.publish_resources());
      break;
    }
    case Event::ACKNOWLEDGE_OFFER_OPERATION: {
      CHECK(event.has_acknowledge_offer_operation());
      acknowledgeOfferOperation(event.acknowledge_offer_operation());
      break;
    }
    case Event::RECONCILE_OFFER_OPERATIONS: {
      CHECK(event.has_reconcile_offer_operations());
      reconcileOfferOperations(event.reconcile_offer_operations());
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
  // Set CSI version to 0.1.0.
  csiVersion.set_major(0);
  csiVersion.set_minor(1);
  csiVersion.set_patch(0);

  Try<string> _bootId = os::bootId();
  if (_bootId.isError()) {
    return fatal("Failed to get boot ID", _bootId.error());
  }

  bootId = _bootId.get();

  foreach (const CSIPluginContainerInfo& container,
           info.storage().plugin().containers()) {
    auto it = find(
        container.services().begin(),
        container.services().end(),
        CSIPluginContainerInfo::CONTROLLER_SERVICE);
    if (it != container.services().end()) {
      controllerContainerId = getContainerId(info, container);
      break;
    }
  }

  foreach (const CSIPluginContainerInfo& container,
           info.storage().plugin().containers()) {
    auto it = find(
        container.services().begin(),
        container.services().end(),
        CSIPluginContainerInfo::NODE_SERVICE);
    if (it != container.services().end()) {
      nodeContainerId = getContainerId(info, container);
      break;
    }
  }

  // NOTE: The name of the default profile is an empty string, which is
  // the default value for `Resource.disk.source.profile` when unset.
  // TODO(chhsiao): Use the volume profile module.
  ProfileData& defaultProfile = profiles[""];
  defaultProfile.capability.mutable_mount();
  defaultProfile.capability.mutable_access_mode()
    ->set_mode(csi::VolumeCapability::AccessMode::SINGLE_NODE_WRITER);

  const string message =
    "Failed to recover resource provider with type '" + info.type() +
    "' and name '" + info.name() + "'";

  // NOTE: Most resource provider events rely on the plugins being
  // prepared. To avoid race conditions, we connect to the agent after
  // preparing the plugins.
  recover()
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
}


void StorageLocalResourceProviderProcess::fatal(
    const string& message,
    const string& failure)
{
  LOG(ERROR) << message << ": " << failure;

  // Force the disconnection early.
  driver.reset();

  process::terminate(self());
}


Future<Nothing> StorageLocalResourceProviderProcess::recover()
{
  CHECK_EQ(RECOVERING, state);

  return recoverServices()
    .then(defer(self(), &Self::recoverVolumes))
    .then(defer(self(), [=]() -> Future<Nothing> {
      // Recover the resource provider ID and state from the latest
      // symlink. If the symlink cannot be resolved, this is a new
      // resource provider, and `totalResources` and `resourceVersion`
      // will be empty, which is fine since they will be set up during
      // reconciliation.
      Result<string> realpath = os::realpath(
          slave::paths::getLatestResourceProviderPath(
              metaDir, slaveId, info.type(), info.name()));

      if (realpath.isError()) {
        return Failure(
            "Failed to read the latest symlink for resource provider with "
            "type '" + info.type() + "' and name '" + info.name() + "'"
            ": " + realpath.error());
      }

      if (realpath.isSome()) {
        info.mutable_id()->set_value(Path(realpath.get()).basename());

        const string statePath = slave::paths::getResourceProviderStatePath(
            metaDir, slaveId, info.type(), info.name(), info.id());

        Result<ResourceProviderState> resourceProviderState =
          ::protobuf::read<ResourceProviderState>(statePath);

        if (resourceProviderState.isError()) {
          return Failure(
              "Failed to read resource provider state from '" + statePath +
              "': " + resourceProviderState.error());
        }

        if (resourceProviderState.isSome()) {
          foreach (const Event::ApplyOfferOperation& operation,
                   resourceProviderState->operations()) {
            Try<UUID> uuid = UUID::fromBytes(operation.operation_uuid());
            CHECK_SOME(uuid);

            pendingOperations[uuid.get()] = operation;
          }

          totalResources = resourceProviderState->resources();

          Try<UUID> uuid =
            UUID::fromBytes(resourceProviderState->resource_version_uuid());
          CHECK_SOME(uuid);

          resourceVersion = uuid.get();
        }
      }

      // We replay all pending operations here, so that if a volume is
      // actually created before the last failover, it will be reflected
      // in the updated total resources before we do the reconciliation.
      // NOTE: `applyOfferOperation` will remove the applied operation
      // from the list of pending operations, so we make a copy of keys
      // here.
      foreach (const UUID& uuid, pendingOperations.keys()) {
        applyOfferOperation(uuid)
          .onAny(defer(self(), [=](const Future<Nothing>& future) {
            if (!future.isReady()) {
              LOG(ERROR)
                << "Failed to apply operation " << uuid << ": "
                << (future.isFailed() ? future.failure() : "future discarded");
            }
          }));
      }

      state = DISCONNECTED;

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
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::recoverServices()
{
  Try<list<string>> containerPaths = csi::paths::getContainerPaths(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name());

  if (containerPaths.isError()) {
    return Failure(
        "Failed to find plugin containers for CSI plugin type '" +
        info.storage().plugin().type() + "' and name '" +
        info.storage().plugin().name() + ": " +
        containerPaths.error());
  }

  list<Future<Nothing>> futures;

  foreach (const string& path, containerPaths.get()) {
    ContainerID containerId;
    containerId.set_value(Path(path).basename());

    // Do not kill the up-to-date controller or node container.
    // Otherwise, kill them and perform cleanups.
    if (containerId == controllerContainerId ||
        containerId == nodeContainerId) {
      const string configPath = csi::paths::getContainerInfoPath(
          slave::paths::getCsiRootDir(workDir),
          info.storage().plugin().type(),
          info.storage().plugin().name(),
          containerId);

      Result<CSIPluginContainerInfo> config =
        ::protobuf::read<CSIPluginContainerInfo>(configPath);

      if (config.isError()) {
        return Failure(
            "Failed to read plugin container config from '" +
            configPath + "': " + config.error());
      }

      if (config.isSome() &&
          getCSIPluginContainerInfo(info, containerId) == config.get()) {
        continue;
      }
    }

    futures.push_back(killService(containerId)
      .then(defer(self(), [=]() -> Future<Nothing> {
        Result<string> endpointDir =
          os::realpath(csi::paths::getEndpointDirSymlinkPath(
              slave::paths::getCsiRootDir(workDir),
              info.storage().plugin().type(),
              info.storage().plugin().name(),
              containerId));

        if (endpointDir.isSome()) {
          Try<Nothing> rmdir = os::rmdir(endpointDir.get());
          if (rmdir.isError()) {
            return Failure(
                "Failed to remove endpoint directory '" + endpointDir.get() +
                "': " + rmdir.error());
          }
        }

        Try<Nothing> rmdir = os::rmdir(path);
        if (rmdir.isError()) {
          return Failure(
              "Failed to remove plugin container directory '" + path + "': " +
              rmdir.error());
        }

        return Nothing();
      })));
  }

  return collect(futures)
    .then(defer(self(), &Self::prepareNodeService))
    .then(defer(self(), &Self::prepareControllerService));
}


Future<Nothing> StorageLocalResourceProviderProcess::recoverVolumes()
{
  // Recover the states of CSI volumes.
  Try<list<string>> volumePaths = csi::paths::getVolumePaths(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name());

  if (volumePaths.isError()) {
    return Failure(
        "Failed to find volumes for CSI plugin type '" +
        info.storage().plugin().type() + "' and name '" +
        info.storage().plugin().name() + ": " + volumePaths.error());
  }

  list<Future<Nothing>> futures;

  foreach (const string& path, volumePaths.get()) {
    Try<csi::paths::VolumePath> volumePath =
      csi::paths::parseVolumePath(slave::paths::getCsiRootDir(workDir), path);

    if (volumePath.isError()) {
      return Failure(
          "Failed to parse volume path '" + path + "': " + volumePath.error());
    }

    CHECK_EQ(info.storage().plugin().type(), volumePath->type);
    CHECK_EQ(info.storage().plugin().name(), volumePath->name);

    const string& volumeId = volumePath->volumeId;
    const string statePath = csi::paths::getVolumeStatePath(
        slave::paths::getCsiRootDir(workDir),
        info.storage().plugin().type(),
        info.storage().plugin().name(),
        volumeId);

    Result<csi::state::VolumeState> volumeState =
      ::protobuf::read<csi::state::VolumeState>(statePath);

    if (volumeState.isError()) {
      return Failure(
          "Failed to read volume state from '" + statePath + "': " +
          volumeState.error());
    }

    if (volumeState.isSome()) {
      volumes.put(volumeId, std::move(volumeState.get()));

      Future<Nothing> recovered = Nothing();

      switch (volumes.at(volumeId).state.state()) {
        case csi::state::VolumeState::CREATED:
        case csi::state::VolumeState::NODE_READY: {
          break;
        }
        case csi::state::VolumeState::PUBLISHED: {
          if (volumes.at(volumeId).state.boot_id() != bootId) {
            // The node has been restarted since the volume is mounted,
            // so it is no longer in the `PUBLISHED` state.
            volumes.at(volumeId).state.set_state(
                csi::state::VolumeState::NODE_READY);
            volumes.at(volumeId).state.clear_boot_id();
            checkpointVolumeState(volumeId);
          }
          break;
        }
        case csi::state::VolumeState::CONTROLLER_PUBLISH: {
          recovered =
            volumes.at(volumeId).sequence->add(std::function<Future<Nothing>()>(
                defer(self(), &Self::controllerPublish, volumeId)));
          break;
        }
        case csi::state::VolumeState::CONTROLLER_UNPUBLISH: {
          recovered =
            volumes.at(volumeId).sequence->add(std::function<Future<Nothing>()>(
                defer(self(), &Self::controllerUnpublish, volumeId)));
          break;
        }
        case csi::state::VolumeState::NODE_PUBLISH: {
          recovered =
            volumes.at(volumeId).sequence->add(std::function<Future<Nothing>()>(
                defer(self(), &Self::nodePublish, volumeId)));
          break;
        }
        case csi::state::VolumeState::NODE_UNPUBLISH: {
          recovered =
            volumes.at(volumeId).sequence->add(std::function<Future<Nothing>()>(
                defer(self(), &Self::nodeUnpublish, volumeId)));
          break;
        }
        case csi::state::VolumeState::UNKNOWN: {
          recovered = Failure(
              "Volume '" + volumeId + "' is in " +
              stringify(volumes.at(volumeId).state.state()) + " state");
        }

        // NOTE: We avoid using a default clause for the following
        // values in proto3's open enum to enable the compiler to detcet
        // missing enum cases for us. See:
        // https://github.com/google/protobuf/issues/3917
        case google::protobuf::kint32min:
        case google::protobuf::kint32max: {
          UNREACHABLE();
        }
      }

      futures.push_back(recovered);
    }
  }

  return collect(futures).then([] { return Nothing(); });
}


void StorageLocalResourceProviderProcess::doReliableRegistration()
{
  if (state == DISCONNECTED || state == SUBSCRIBED || state == READY) {
    return;
  }

  CHECK_EQ(CONNECTED, state);

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();
  subscribe->mutable_resource_provider_info()->CopyFrom(info);

  auto err = [](const ResourceProviderInfo& info, const string& message) {
    LOG(ERROR)
      << "Failed to subscribe resource provider with type '" << info.type()
      << "' and name '" << info.name() << "': " << message;
  };

  driver->send(evolve(call))
    .onFailed(std::bind(err, info, lambda::_1))
    .onDiscarded(std::bind(err, info, "future discarded"));

  // TODO(chhsiao): Consider doing an exponential backoff.
  delay(Seconds(1), self(), &Self::doReliableRegistration);
}


Future<Nothing> StorageLocalResourceProviderProcess::reconcile()
{
  return importResources()
    .then(defer(self(), [=](Resources imported) {
      // NOTE: We do not support decreasing the total resources for now.
      // Any resource in the checkpointed state will be reported to the
      // resource provider manager, even if it is missing in the
      // imported resources from the plugin. Additional resources from
      // the plugin will be reported with the default reservation.

      Resources stripped;
      foreach (const Resource& resource, totalResources) {
        stripped += createRawDiskResource(
            resource.provider_id(),
            resource.scalar().value(),
            resource.disk().source().has_profile()
              ? resource.disk().source().profile() : Option<string>::none(),
            resource.disk().source().has_id()
              ? resource.disk().source().id() : Option<string>::none(),
            resource.disk().source().has_metadata()
              ? resource.disk().source().metadata() : Option<Labels>::none());
      }

      Resources result = totalResources;
      foreach (Resource resource, imported - stripped) {
        if (resource.disk().source().has_id() &&
            !volumes.contains(resource.disk().source().id())) {
          csi::state::VolumeState volumeState;
          volumeState.set_state(csi::state::VolumeState::CREATED);

          // The default profile is used if `profile` is unset.
          volumeState.mutable_volume_capability()->CopyFrom(
              profiles.at(resource.disk().source().profile()).capability);

          if (resource.disk().source().has_metadata()) {
            volumeState.mutable_volume_attributes()->swap(
                convertLabelsToStringMap(
                    resource.disk().source().metadata()).get());
          }

          volumes.put(resource.disk().source().id(), std::move(volumeState));
          checkpointVolumeState(resource.disk().source().id());
        }

        resource.mutable_reservations()->CopyFrom(info.default_reservations());
        result += resource;

        LOG(INFO) << "Adding new resource '" << resource << "'";
      }

      if (resourceVersion.isNone() || result != totalResources) {
        totalResources = result;
        resourceVersion = UUID::random();
        checkpointResourceProviderState();
      }

      sendResourceProviderStateUpdate();

      state = READY;

      return Nothing();
    }));
}


void StorageLocalResourceProviderProcess::subscribed(
    const Event::Subscribed& subscribed)
{
  CHECK_EQ(CONNECTED, state);

  LOG(INFO) << "Subscribed with ID " << subscribed.provider_id().value();

  state = SUBSCRIBED;

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

  // Reconcile resources after obtaining the resource provider ID.
  // TODO(chhsiao): Do the reconciliation early.
  reconcile()
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));
}


void StorageLocalResourceProviderProcess::applyOfferOperation(
    const Event::ApplyOfferOperation& operation)
{
  Future<Resources> converted;

  if (state == SUBSCRIBED) {
    // TODO(chhsiao): Reject this operation.
    return;
  }

  CHECK_EQ(READY, state);

  LOG(INFO) << "Received " << operation.info().type() << " operation";

  Try<UUID> uuid = UUID::fromBytes(operation.operation_uuid());
  CHECK_SOME(uuid);

  CHECK(!pendingOperations.contains(uuid.get()));
  pendingOperations[uuid.get()] = operation;
  checkpointResourceProviderState();

  applyOfferOperation(uuid.get())
    .onAny(defer(self(), [=](const Future<Nothing>& future) {
      if (!future.isReady()) {
        LOG(ERROR)
          << "Failed to apply " << operation.info().type() << " operation: "
          << (future.isFailed() ? future.failure() : "future discarded");
      }
    }));
}


void StorageLocalResourceProviderProcess::publishResources(
    const Event::PublishResources& publish)
{
  Option<Error> error;
  hashset<string> volumeIds;

  if (state == SUBSCRIBED) {
    error = Error("Cannot publish resources in SUBSCRIBED state");
  } else {
    CHECK_EQ(READY, state);

    Resources resources = publish.resources();
    resources.unallocate();
    foreach (const Resource& resource, resources) {
      if (!totalResources.contains(resource)) {
        error = Error(
            "Cannot publish unknown resource '" + stringify(resource) + "'");
        break;
      }

      switch (resource.disk().source().type()) {
        case Resource::DiskInfo::Source::PATH:
        case Resource::DiskInfo::Source::MOUNT:
        case Resource::DiskInfo::Source::BLOCK: {
          CHECK(resource.disk().source().has_id());
          CHECK(volumes.contains(resource.disk().source().id()));
          volumeIds.insert(resource.disk().source().id());
          break;
        }
        case Resource::DiskInfo::Source::UNKNOWN:
        case Resource::DiskInfo::Source::RAW: {
          error = Error(
              "Cannot publish volume of " +
              stringify(resource.disk().source().type()) + " type");
          break;
        }
      }
    }
  }

  Future<list<Nothing>> allPublished;

  if (error.isSome()) {
    allPublished = Failure(error.get());
  } else {
    list<Future<Nothing>> futures;

    foreach (const string& volumeId, volumeIds) {
      // We check the state of the volume along with the CSI calls
      // atomically with respect to other publish or deletion requests
      // for the same volume through dispatching the whole lambda on the
      // volume's sequence.
      std::function<Future<Nothing>()> controllerAndNodePublish =
        defer(self(), [=] {
          CHECK(volumes.contains(volumeId));

          Future<Nothing> published = Nothing();

          // NOTE: We don't break for `CREATED` and `NODE_READY` as
          // publishing the volume in these states needs all operations
          // beneath it.
          switch (volumes.at(volumeId).state.state()) {
            case csi::state::VolumeState::CREATED: {
              published = published
                .then(defer(self(), &Self::controllerPublish, volumeId));
            }
            case csi::state::VolumeState::NODE_READY: {
              published = published
                .then(defer(self(), &Self::nodePublish, volumeId));
            }
            case csi::state::VolumeState::PUBLISHED: {
              break;
            }
            case csi::state::VolumeState::UNKNOWN:
            case csi::state::VolumeState::CONTROLLER_PUBLISH:
            case csi::state::VolumeState::CONTROLLER_UNPUBLISH:
            case csi::state::VolumeState::NODE_PUBLISH:
            case csi::state::VolumeState::NODE_UNPUBLISH: {
              UNREACHABLE();
            }

            // NOTE: We avoid using a default clause for the following
            // values in proto3's open enum to enable the compiler to detcet
            // missing enum cases for us. See:
            // https://github.com/google/protobuf/issues/3917
            case google::protobuf::kint32min:
            case google::protobuf::kint32max: {
              UNREACHABLE();
            }
          }

          return published;
        });

      futures.push_back(
          volumes.at(volumeId).sequence->add(controllerAndNodePublish));
    }

    allPublished = collect(futures);
  }

  allPublished
    .onAny(defer(self(), [=](const Future<list<Nothing>>& future) {
      // TODO(chhsiao): Currently there is no way to reply to the
      // resource provider manager with a failure message, so we log the
      // failure here.
      if (!future.isReady()) {
        LOG(ERROR)
          << "Failed to publish resources '" << publish.resources() << "': "
          << (future.isFailed() ? future.failure() : "future discarded");
      }

      Call call;
      call.mutable_resource_provider_id()->CopyFrom(info.id());
      call.set_type(Call::UPDATE_PUBLISH_RESOURCES_STATUS);

      Call::UpdatePublishResourcesStatus* update =
        call.mutable_update_publish_resources_status();
      update->set_uuid(publish.uuid());
      update->set_status(future.isReady()
        ? Call::UpdatePublishResourcesStatus::OK
        : Call::UpdatePublishResourcesStatus::FAILED);

      auto err = [](const string& uuid, const string& message) {
        LOG(ERROR)
          << "Failed to send status update for publish "
          << UUID::fromBytes(uuid).get() << ": " << message;
      };

      driver->send(evolve(call))
        .onFailed(std::bind(err, publish.uuid(), lambda::_1))
        .onDiscarded(std::bind(err, publish.uuid(), "future discarded"));
    }));
}


void StorageLocalResourceProviderProcess::acknowledgeOfferOperation(
    const Event::AcknowledgeOfferOperation& acknowledge)
{
  CHECK_EQ(READY, state);
}


void StorageLocalResourceProviderProcess::reconcileOfferOperations(
    const Event::ReconcileOfferOperations& reconcile)
{
  CHECK_EQ(READY, state);
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
      return client.GetSupportedVersions(csi::GetSupportedVersionsRequest())
        .then(defer(self(), [=](
            const csi::GetSupportedVersionsResponse& response)
            -> Future<csi::Client> {
          auto it = find(
              response.supported_versions().begin(),
              response.supported_versions().end(),
              csiVersion);
          if (it == response.supported_versions().end()) {
            return Failure(
                "CSI version " + stringify(csiVersion) + " is not supported");
          }

          return client;
        }));
    }));
}


// Returns a future of the latest CSI client for the specified plugin
// container. If the container is not already running, this method will
// start a new a new container daemon.
Future<csi::Client> StorageLocalResourceProviderProcess::getService(
    const ContainerID& containerId)
{
  if (daemons.contains(containerId)) {
    CHECK(services.contains(containerId));
    return services.at(containerId)->future();
  }

  Option<CSIPluginContainerInfo> config =
    getCSIPluginContainerInfo(info, containerId);

  CHECK_SOME(config);

  CommandInfo commandInfo;

  if (config->has_command()) {
    commandInfo.CopyFrom(config->command());
  }

  // Set the `CSI_ENDPOINT` environment variable.
  Try<string> endpoint = csi::paths::getEndpointSocketPath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      containerId);

  if (endpoint.isError()) {
    return Failure(
        "Failed to resolve endpoint path for plugin container '" +
        stringify(containerId) + "': " + endpoint.error());
  }

  const string& endpointPath = endpoint.get();
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
  const string endpointDir = Path(endpointPath).dirname();
  Volume* endpointVolume = containerInfo.add_volumes();
  endpointVolume->set_mode(Volume::RW);
  endpointVolume->set_container_path(endpointDir);
  endpointVolume->set_host_path(endpointDir);

  // Prepare the directory where the mount points will be placed.
  const string mountDir = csi::paths::getMountRootDir(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name());

  Try<Nothing> mkdir = os::mkdir(mountDir);
  if (mkdir.isError()) {
    return Failure(
        "Failed to create directory '" + mountDir +
        "': " + mkdir.error());
  }

  // Prepare a volume where the mount points will be placed.
  Volume* mountVolume = containerInfo.add_volumes();
  mountVolume->set_mode(Volume::RW);
  mountVolume->set_container_path(mountDir);
  mountVolume->mutable_source()->set_type(Volume::Source::HOST_PATH);
  mountVolume->mutable_source()->mutable_host_path()->set_path(mountDir);
  mountVolume->mutable_source()->mutable_host_path()
    ->mutable_mount_propagation()->set_mode(MountPropagation::BIDIRECTIONAL);

  CHECK(!services.contains(containerId));
  services[containerId].reset(new Promise<csi::Client>());

  Try<Owned<ContainerDaemon>> daemon = ContainerDaemon::create(
      extractParentEndpoint(url),
      authToken,
      containerId,
      commandInfo,
      config->resources(),
      containerInfo,
      std::function<Future<Nothing>()>(defer(self(), [=]() {
        CHECK(services.at(containerId)->future().isPending());

        return connect(endpointPath)
          .then(defer(self(), [=](const csi::Client& client) {
            services.at(containerId)->set(client);
            return Nothing();
          }))
          .onFailed(defer(self(), [=](const string& failure) {
            services.at(containerId)->fail(failure);
          }))
          .onDiscarded(defer(self(), [=] {
            services.at(containerId)->discard();
          }));
      })),
      std::function<Future<Nothing>()>(defer(self(), [=]() -> Future<Nothing> {
        services.at(containerId)->discard();
        services.at(containerId).reset(new Promise<csi::Client>());

        if (os::exists(endpointPath)) {
          Try<Nothing> rm = os::rm(endpointPath);
          if (rm.isError()) {
            return Failure(
                "Failed to remove endpoint '" + endpointPath +
                "': " + rm.error());
          }
        }

        return Nothing();
      })));

  if (daemon.isError()) {
    return Failure(
        "Failed to create container daemon for plugin container '" +
        stringify(containerId) + "': " + daemon.error());
  }

  // Checkpoint the plugin container config.
  const string configPath = csi::paths::getContainerInfoPath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      containerId);

  Try<Nothing> checkpoint = slave::state::checkpoint(configPath, config.get());
  if (checkpoint.isError()) {
    return Failure(
        "Failed to checkpoint plugin container config to '" + configPath +
        "': " + checkpoint.error());
  }

  const string message =
    "Container daemon for '" + stringify(containerId) + "' failed";

  daemons[containerId] = daemon.get();
  daemon.get()->wait()
    .onFailed(defer(self(), &Self::fatal, message, lambda::_1))
    .onDiscarded(defer(self(), &Self::fatal, message, "future discarded"));

  return services.at(containerId)->future();
}


// Kills the specified plugin container and returns a future that waits
// for it to terminate.
Future<Nothing> StorageLocalResourceProviderProcess::killService(
    const ContainerID& containerId)
{
  CHECK(!daemons.contains(containerId));
  CHECK(!services.contains(containerId));

  agent::Call call;
  call.set_type(agent::Call::KILL_CONTAINER);
  call.mutable_kill_container()->mutable_container_id()->CopyFrom(containerId);

  return http::post(
      extractParentEndpoint(url),
      getAuthHeader(authToken),
      serialize(contentType, evolve(call)),
      stringify(contentType))
    .then(defer(self(), [=](const http::Response& response) -> Future<Nothing> {
      if (response.status == http::NotFound().status) {
        return Nothing();
      }

      if (response.status != http::OK().status) {
        return Failure(
            "Failed to kill container '" + stringify(containerId) +
            "': Unexpected response '" + response.status + "' (" + response.body
            + ")");
      }

      agent::Call call;
      call.set_type(agent::Call::WAIT_CONTAINER);
      call.mutable_wait_container()
        ->mutable_container_id()->CopyFrom(containerId);

      return http::post(
          extractParentEndpoint(url),
          getAuthHeader(authToken),
          serialize(contentType, evolve(call)),
          stringify(contentType))
        .then(defer(self(), [=](
            const http::Response& response) -> Future<Nothing> {
          if (response.status != http::OK().status &&
              response.status != http::NotFound().status) {
            return Failure(
                "Failed to wait for container '" + stringify(containerId) +
                "': Unexpected response '" + response.status + "' (" +
                response.body + ")");
          }

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::prepareControllerService()
{
  return getService(controllerContainerId)
    .then(defer(self(), [=](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then(defer(self(), [=](const csi::GetPluginInfoResponse& response) {
          controllerInfo = response;

          LOG(INFO)
            << "Controller plugin loaded: " << stringify(controllerInfo.get());

          if (nodeInfo.isSome() &&
              (controllerInfo->name() != nodeInfo->name() ||
               controllerInfo->vendor_version() !=
                 nodeInfo->vendor_version())) {
            LOG(WARNING)
              << "Inconsistent controller and node plugin components. Please "
                 "check with the plugin vendor to ensure compatibility.";
          }

          // NOTE: We always get the latest service future before
          // proceeding to the next step.
          return getService(controllerContainerId);
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Probe the plugin to validate the runtime environment.
      csi::ControllerProbeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ControllerProbe(request)
        .then(defer(self(), [=](const csi::ControllerProbeResponse& response) {
          return getService(controllerContainerId);
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Get the controller capabilities.
      csi::ControllerGetCapabilitiesRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.ControllerGetCapabilities(request)
        .then(defer(self(), [=](
            const csi::ControllerGetCapabilitiesResponse& response) {
          controllerCapabilities = response.capabilities();

          return Nothing();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::prepareNodeService()
{
  return getService(nodeContainerId)
    .then(defer(self(), [=](csi::Client client) {
      // Get the plugin info and check for consistency.
      csi::GetPluginInfoRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetPluginInfo(request)
        .then(defer(self(), [=](const csi::GetPluginInfoResponse& response) {
          nodeInfo = response;

          LOG(INFO) << "Node plugin loaded: " << stringify(nodeInfo.get());

          if (controllerInfo.isSome() &&
              (controllerInfo->name() != nodeInfo->name() ||
               controllerInfo->vendor_version() !=
                 nodeInfo->vendor_version())) {
            LOG(WARNING)
              << "Inconsistent controller and node plugin components. Please "
                 "check with the plugin vendor to ensure compatibility.";
          }

          // NOTE: We always get the latest service future before
          // proceeding to the next step.
          return getService(nodeContainerId);
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Probe the plugin to validate the runtime environment.
      csi::NodeProbeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.NodeProbe(request)
        .then(defer(self(), [=](const csi::NodeProbeResponse& response) {
          return getService(nodeContainerId);
        }));
    }))
    .then(defer(self(), [=](csi::Client client) {
      // Get the node ID.
      csi::GetNodeIDRequest request;
      request.mutable_version()->CopyFrom(csiVersion);

      return client.GetNodeID(request)
        .then(defer(self(), [=](const csi::GetNodeIDResponse& response) {
          nodeId = response.node_id();

          return Nothing();
        }));
    }));
}


// Returns resources reported by the CSI plugin, which are unreserved
// raw disk resources without any persistent volume.
Future<Resources> StorageLocalResourceProviderProcess::importResources()
{
  // NOTE: This can only be called after `prepareControllerService` and
  // the resource provider ID has been obtained.
  CHECK_SOME(controllerCapabilities);
  CHECK(info.has_id());

  Future<Resources> preprovisioned;

  if (controllerCapabilities->listVolumes) {
    preprovisioned = getService(controllerContainerId)
      .then(defer(self(), [=](csi::Client client) {
        // TODO(chhsiao): Set the max entries and use a loop to do
        // mutliple `ListVolumes` calls.
        csi::ListVolumesRequest request;
        request.mutable_version()->CopyFrom(csiVersion);

        return client.ListVolumes(request)
          .then(defer(self(), [=](const csi::ListVolumesResponse& response) {
            Resources resources;

            // Recover volume profiles from the checkpointed state.
            hashmap<string, string> volumesToProfiles;
            foreach (const Resource& resource, totalResources) {
              if (resource.disk().source().has_id() &&
                  resource.disk().source().has_profile()) {
                volumesToProfiles[resource.disk().source().id()] =
                  resource.disk().source().profile();
              }
            }

            foreach (const auto& entry, response.entries()) {
              resources += createRawDiskResource(
                  info.id(),
                  entry.volume_info().capacity_bytes(),
                  volumesToProfiles.contains(entry.volume_info().id())
                    ? volumesToProfiles.at(entry.volume_info().id())
                    : Option<string>::none(),
                  entry.volume_info().id(),
                  entry.volume_info().attributes().empty()
                    ? Option<Labels>::none()
                    : convertStringMapToLabels(
                          entry.volume_info().attributes()));
            }

            return resources;
          }));
      }));
  } else {
    preprovisioned = Resources();
  }

  return preprovisioned
    .then(defer(self(), [=](const Resources& preprovisioned) {
      list<Future<Resources>> futures;

      foreach (const Resource& resource, preprovisioned) {
        futures.push_back(getService(controllerContainerId)
          .then(defer(self(), [=](csi::Client client) {
            csi::ValidateVolumeCapabilitiesRequest request;
            request.mutable_version()->CopyFrom(csiVersion);
            request.set_volume_id(resource.disk().source().id());

            // The default profile is used if `profile` is unset.
            request.add_volume_capabilities()->CopyFrom(
                profiles.at(resource.disk().source().profile()).capability);

            if (resource.disk().source().has_metadata()) {
              request.mutable_volume_attributes()->swap(
                  convertLabelsToStringMap(
                      resource.disk().source().metadata()).get());
            }

            return client.ValidateVolumeCapabilities(request)
              .then(defer(self(), [=](
                  const csi::ValidateVolumeCapabilitiesResponse& response)
                  -> Future<Resources> {
                if (!response.supported()) {
                  return Failure(
                      "Unsupported volume capability for resource " +
                      stringify(resource) + ": " + response.message());
                }

                return resource;
              }));
          })));
      }

      if (controllerCapabilities->getCapacity) {
        foreachkey (const string& profile, profiles) {
          futures.push_back(getService(controllerContainerId)
            .then(defer(self(), [=](csi::Client client) {
              csi::GetCapacityRequest request;
              request.mutable_version()->CopyFrom(csiVersion);
              request.add_volume_capabilities()
                ->CopyFrom(profiles.at(profile).capability);
              *request.mutable_parameters() = profiles.at(profile).parameters;

              return client.GetCapacity(request)
                .then(defer(self(), [=](
                    const csi::GetCapacityResponse& response)
                    -> Future<Resources> {
                  if (response.available_capacity() == 0) {
                    return Resources();
                  }

                  return createRawDiskResource(
                      info.id(),
                      response.available_capacity(),
                      profile.empty() ? Option<string>::none() : profile);
              }));
            })));
        }
      }

      return collect(futures)
        .then(defer(self(), [=](const list<Resources>& resources) {
          return accumulate(resources.begin(), resources.end(), Resources());
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::controllerPublish(
    const string& volumeId)
{
  // NOTE: This can only be called after `prepareControllerService` and
  // `prepareNodeService`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeId);

  CHECK(volumes.contains(volumeId));
  if (volumes.at(volumeId).state.state() ==
        csi::state::VolumeState::CONTROLLER_PUBLISH) {
    // The resource provider failed over during the last
    // `ControllerPublishVolume` call.
    CHECK_EQ(RECOVERING, state);
  } else {
    CHECK_EQ(csi::state::VolumeState::CREATED,
             volumes.at(volumeId).state.state());

    volumes.at(volumeId).state.set_state(
        csi::state::VolumeState::CONTROLLER_PUBLISH);
    checkpointVolumeState(volumeId);
  }

  Future<Nothing> controllerPublished;

  if (controllerCapabilities->publishUnpublishVolume) {
    controllerPublished = getService(controllerContainerId)
      .then(defer(self(), [=](csi::Client client) {
        csi::ControllerPublishVolumeRequest request;
        request.mutable_version()->CopyFrom(csiVersion);
        request.set_volume_id(volumeId);
        request.set_node_id(nodeId.get());
        request.mutable_volume_capability()
          ->CopyFrom(volumes.at(volumeId).state.volume_capability());
        request.set_readonly(false);
        *request.mutable_volume_attributes() =
          volumes.at(volumeId).state.volume_attributes();

        return client.ControllerPublishVolume(request)
          .then(defer(self(), [=](
              const csi::ControllerPublishVolumeResponse& response) {
            *volumes.at(volumeId).state.mutable_publish_volume_info() =
              response.publish_volume_info();

            return Nothing();
          }));
      }));
  } else {
    controllerPublished = Nothing();
  }

  return controllerPublished
    .then(defer(self(), [=] {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::NODE_READY);
      checkpointVolumeState(volumeId);

      return Nothing();
    }))
    .repair(defer(self(), [=](const Future<Nothing>& future) {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::CREATED);
      checkpointVolumeState(volumeId);

      return future;
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::controllerUnpublish(
    const string& volumeId)
{
  // NOTE: This can only be called after `prepareControllerService` and
  // `prepareNodeService`.
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeId);

  CHECK(volumes.contains(volumeId));
  if (volumes.at(volumeId).state.state() ==
        csi::state::VolumeState::CONTROLLER_UNPUBLISH) {
    // The resource provider failed over during the last
    // `ControllerUnpublishVolume` call.
    CHECK_EQ(RECOVERING, state);
  } else {
    CHECK_EQ(csi::state::VolumeState::NODE_READY,
             volumes.at(volumeId).state.state());

    volumes.at(volumeId).state.set_state(
        csi::state::VolumeState::CONTROLLER_UNPUBLISH);
    checkpointVolumeState(volumeId);
  }

  Future<Nothing> controllerUnpublished;

  if (controllerCapabilities->publishUnpublishVolume) {
    controllerUnpublished = getService(controllerContainerId)
      .then(defer(self(), [=](csi::Client client) {
        csi::ControllerUnpublishVolumeRequest request;
        request.mutable_version()->CopyFrom(csiVersion);
        request.set_volume_id(volumeId);
        request.set_node_id(nodeId.get());

        return client.ControllerUnpublishVolume(request)
          .then([] { return Nothing(); });
      }));
  } else {
    controllerUnpublished = Nothing();
  }

  return controllerUnpublished
    .then(defer(self(), [=] {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::CREATED);
      volumes.at(volumeId).state.mutable_publish_volume_info()->clear();
      checkpointVolumeState(volumeId);

      return Nothing();
    }))
    .repair(defer(self(), [=](const Future<Nothing>& future) {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::NODE_READY);
      checkpointVolumeState(volumeId);

      return future;
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::nodePublish(
    const string& volumeId)
{
  CHECK(volumes.contains(volumeId));
  if (volumes.at(volumeId).state.state() ==
        csi::state::VolumeState::NODE_PUBLISH) {
    // The resource provider failed over during the last
    // `NodePublishVolume` call.
    CHECK_EQ(RECOVERING, state);
  } else {
    CHECK_EQ(csi::state::VolumeState::NODE_READY,
             volumes.at(volumeId).state.state());

    volumes.at(volumeId).state.set_state(csi::state::VolumeState::NODE_PUBLISH);
    checkpointVolumeState(volumeId);
  }

  const string mountPath = csi::paths::getMountPath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      volumeId);

  Try<Nothing> mkdir = os::mkdir(mountPath);
  if (mkdir.isError()) {
    return Failure(
        "Failed to create mount point '" + mountPath + "': " + mkdir.error());
  }

  return getService(nodeContainerId)
    .then(defer(self(), [=](csi::Client client) {
      csi::NodePublishVolumeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);
      request.set_volume_id(volumeId);
      *request.mutable_publish_volume_info() =
        volumes.at(volumeId).state.publish_volume_info();
      request.set_target_path(mountPath);
      request.mutable_volume_capability()
        ->CopyFrom(volumes.at(volumeId).state.volume_capability());
      request.set_readonly(false);
      *request.mutable_volume_attributes() =
        volumes.at(volumeId).state.volume_attributes();

      return client.NodePublishVolume(request);
    }))
    .then(defer(self(), [=] {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::PUBLISHED);
      volumes.at(volumeId).state.set_boot_id(bootId);
      checkpointVolumeState(volumeId);

      return Nothing();
    }))
    .repair(defer(self(), [=](const Future<Nothing>& future) {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::NODE_READY);
      checkpointVolumeState(volumeId);

      return future;
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::nodeUnpublish(
    const string& volumeId)
{
  CHECK(volumes.contains(volumeId));
  if (volumes.at(volumeId).state.state() ==
        csi::state::VolumeState::NODE_UNPUBLISH) {
    // The resource provider failed over during the last
    // `NodeUnpublishVolume` call.
    CHECK_EQ(RECOVERING, state);
  } else {
    CHECK_EQ(csi::state::VolumeState::PUBLISHED,
             volumes.at(volumeId).state.state());

    volumes.at(volumeId).state.set_state(
        csi::state::VolumeState::NODE_UNPUBLISH);
    checkpointVolumeState(volumeId);
  }

  const string mountPath = csi::paths::getMountPath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      volumeId);

  Future<Nothing> nodeUnpublished;

  if (os::exists(mountPath)) {
    nodeUnpublished = getService(nodeContainerId)
      .then(defer(self(), [=](csi::Client client) {
        csi::NodeUnpublishVolumeRequest request;
        request.mutable_version()->CopyFrom(csiVersion);
        request.set_volume_id(volumeId);
        request.set_target_path(mountPath);

        return client.NodeUnpublishVolume(request)
          .then([] { return Nothing(); });
      }));
  } else {
    // The volume has been actually unpublished before failover.
    CHECK_EQ(RECOVERING, state);

    nodeUnpublished = Nothing();
  }

  return nodeUnpublished
    .then(defer(self(), [=]() -> Future<Nothing> {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::NODE_READY);
      volumes.at(volumeId).state.clear_boot_id();

      Try<Nothing> rmdir = os::rmdir(mountPath);
      if (rmdir.isError()) {
        return Failure(
            "Failed to remove mount point '" + mountPath + "': " +
            rmdir.error());
      }

      checkpointVolumeState(volumeId);

      return Nothing();
    }))
    .repair(defer(self(), [=](const Future<Nothing>& future) {
      volumes.at(volumeId).state.set_state(csi::state::VolumeState::PUBLISHED);
      checkpointVolumeState(volumeId);

      return future;
    }));
}


Future<string> StorageLocalResourceProviderProcess::createVolume(
    const string& name,
    const Bytes& capacity,
    const ProfileData& profile)
{
  // NOTE: This can only be called after `prepareControllerService`.
  CHECK_SOME(controllerCapabilities);

  if (!controllerCapabilities->createDeleteVolume) {
    return Failure("Capability 'CREATE_DELETE_VOLUME' is not supported");
  }

  return getService(controllerContainerId)
    .then(defer(self(), [=](csi::Client client) {
      csi::CreateVolumeRequest request;
      request.mutable_version()->CopyFrom(csiVersion);
      request.set_name(name);
      request.mutable_capacity_range()
        ->set_required_bytes(capacity.bytes());
      request.mutable_capacity_range()
        ->set_limit_bytes(capacity.bytes());
      request.add_volume_capabilities()->CopyFrom(profile.capability);
      *request.mutable_parameters() = profile.parameters;

      return client.CreateVolume(request)
        .then(defer(self(), [=](const csi::CreateVolumeResponse& response) {
          const csi::VolumeInfo& volumeInfo = response.volume_info();

          if (volumes.contains(volumeInfo.id())) {
            // The resource provider failed over after the last
            // `CreateVolume` call, but before the operation status
            // was checkpointed.
            CHECK_EQ(csi::state::VolumeState::CREATED,
                     volumes.at(volumeInfo.id()).state.state());
          } else {
            csi::state::VolumeState volumeState;
            volumeState.set_state(csi::state::VolumeState::CREATED);
            volumeState.mutable_volume_capability()
              ->CopyFrom(profile.capability);
            *volumeState.mutable_volume_attributes() =
              volumeInfo.attributes();

            volumes.put(volumeInfo.id(), std::move(volumeState));
            checkpointVolumeState(volumeInfo.id());
          }

          return volumeInfo.id();
        }));
    }));
}


Future<Nothing> StorageLocalResourceProviderProcess::deleteVolume(
    const string& volumeId)
{
  // NOTE: This can only be called after `prepareControllerService` and
  // `prepareNodeService` (since it may require `NodeUnpublishVolume`).
  CHECK_SOME(controllerCapabilities);
  CHECK_SOME(nodeId);

  if (!controllerCapabilities->createDeleteVolume) {
    return Failure("Capability 'CREATE_DELETE_VOLUME' is not supported");
  }

  const string volumePath = csi::paths::getVolumePath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      volumeId);

  Future<Nothing> deleted = Nothing();

  if (volumes.contains(volumeId)) {
    // NOTE: We don't break for `PUBLISHED` and `NODE_READY` as deleting
    // the volume in these states needs all operations beneath it.
    switch (volumes.at(volumeId).state.state()) {
      case csi::state::VolumeState::PUBLISHED: {
        deleted = deleted
          .then(defer(self(), &Self::nodeUnpublish, volumeId));
      }
      case csi::state::VolumeState::NODE_READY: {
        deleted = deleted
          .then(defer(self(), &Self::controllerUnpublish, volumeId));
      }
      case csi::state::VolumeState::CREATED: {
        deleted = deleted
          .then(defer(self(), &Self::getService, controllerContainerId))
          .then(defer(self(), [=](csi::Client client) {
            csi::DeleteVolumeRequest request;
            request.mutable_version()->CopyFrom(csiVersion);
            request.set_volume_id(volumeId);

            return client.DeleteVolume(request)
              .then(defer(self(), [=] {
                // NOTE: This will destruct the volume's sequence!
                volumes.erase(volumeId);
                CHECK_SOME(os::rmdir(volumePath));

                return Nothing();
              }));
          }));
        break;
      }
      case csi::state::VolumeState::UNKNOWN:
      case csi::state::VolumeState::CONTROLLER_PUBLISH:
      case csi::state::VolumeState::CONTROLLER_UNPUBLISH:
      case csi::state::VolumeState::NODE_PUBLISH:
      case csi::state::VolumeState::NODE_UNPUBLISH:
      case google::protobuf::kint32min:
      case google::protobuf::kint32max: {
        UNREACHABLE();
      }
    }
  } else {
    // The resource provider failed over after the last `DeleteVolume`
    // call, but before the operation status was checkpointed.
    CHECK(!os::exists(volumePath));
  }

  // NOTE: We make the returned future undiscardable because the
  // deletion may cause the volume's sequence to be destructed, which
  // will in turn discard all futures in the sequence.
  return undiscardable(deleted);
}


Future<Nothing> StorageLocalResourceProviderProcess::applyOfferOperation(
    const UUID& operationUuid)
{
  Future<vector<ResourceConversion>> conversions;
  Option<Error> error;

  CHECK(pendingOperations.contains(operationUuid));
  const Event::ApplyOfferOperation& operation =
    pendingOperations.at(operationUuid);

  Try<UUID> operationVersion =
    UUID::fromBytes(operation.resource_version_uuid());
  CHECK_SOME(operationVersion);

  if (resourceVersion.get() != operationVersion.get()) {
    error = Error(
        "Mismatched resource version " + stringify(operationVersion.get()) +
        " (expected: " + stringify(resourceVersion.get()) + ")");
  }

  switch (operation.info().type()) {
    case Offer::Operation::RESERVE:
    case Offer::Operation::UNRESERVE:
    case Offer::Operation::CREATE:
    case Offer::Operation::DESTROY: {
      // Synchronously apply the conventional operations.
      return applyResourceConversions(
          operationUuid,
          error.isNone()
            ? getResourceConversions(operation.info())
            : Try<vector<ResourceConversion>>::error(error.get()));
    }
    case Offer::Operation::CREATE_VOLUME: {
      CHECK(operation.info().has_create_volume());

      if (error.isNone()) {
        conversions = applyCreateVolumeOrBlock(
            operation.info().create_volume().source(),
            operationUuid,
            operation.info().create_volume().target_type());
      } else {
        conversions = Failure(error.get());
      }
      break;
    }
    case Offer::Operation::DESTROY_VOLUME: {
      CHECK(operation.info().has_destroy_volume());

      if (error.isNone()) {
        conversions = applyDestroyVolumeOrBlock(
            operation.info().destroy_volume().volume());
      } else {
        conversions = Failure(error.get());
      }
      break;
    }
    case Offer::Operation::CREATE_BLOCK: {
      CHECK(operation.info().has_create_block());

      if (error.isNone()) {
        conversions = applyCreateVolumeOrBlock(
            operation.info().create_block().source(),
            operationUuid,
            Resource::DiskInfo::Source::BLOCK);
      } else {
        conversions = Failure(error.get());
      }
      break;
    }
    case Offer::Operation::DESTROY_BLOCK: {
      CHECK(operation.info().has_destroy_block());

      if (error.isNone()) {
        conversions = applyDestroyVolumeOrBlock(
            operation.info().destroy_block().block());
      } else {
        conversions = Failure(error.get());
      }
      break;
    }
    case Offer::Operation::UNKNOWN:
    case Offer::Operation::LAUNCH:
    case Offer::Operation::LAUNCH_GROUP: {
      UNREACHABLE();
    }
  }

  // NOTE: The code below is executed only when applying a storage operation.
  shared_ptr<Promise<Nothing>> promise(new Promise<Nothing>());

  conversions
    .onAny(defer(self(), [=](const Future<vector<ResourceConversion>>& future) {
      Option<Error> error;
      if (!future.isReady()) {
        error =
          Error(future.isFailed() ? future.failure() : "future discarded");
      }

      if (future.isReady()) {
        LOG(INFO)
          << "Applying conversion from '" << future->at(0).consumed << "' to '"
          << future->at(0).converted << "'";
      } else {
        LOG(ERROR)
          << "Failed to apply " << operation.info().type() << " operation: "
          << error->message;
      }

      promise->associate(applyResourceConversions(
          operationUuid,
          error.isNone()
            ? future.get()
            : Try<vector<ResourceConversion>>::error(error.get())));
    }));

  return promise->future();
}


Future<vector<ResourceConversion>>
StorageLocalResourceProviderProcess::applyCreateVolumeOrBlock(
    const Resource& resource,
    const UUID& operationUuid,
    const Resource::DiskInfo::Source::Type& type)
{
  if (resource.disk().source().type() != Resource::DiskInfo::Source::RAW) {
    return Failure(
        "Cannot create volume from source of " +
        stringify(resource.disk().source().type()) + " type");
  }

  switch (type) {
    case Resource::DiskInfo::Source::PATH:
    case Resource::DiskInfo::Source::MOUNT: {
      if (!profiles.at(resource.disk().source().profile())
             .capability.has_mount()) {
        return Failure(
            "Profile '" + resource.disk().source().profile() +
            "' cannot be used for CREATE_VOLUME operation");
      }
      break;
    }
    case Resource::DiskInfo::Source::BLOCK: {
      if (!profiles.at(resource.disk().source().profile())
             .capability.has_block()) {
        return Failure(
            "Profile '" + resource.disk().source().profile() +
            "' cannot be used for CREATE_BLOCK operation");
      }
      break;
    }
    case Resource::DiskInfo::Source::UNKNOWN:
    case Resource::DiskInfo::Source::RAW: {
      UNREACHABLE();
    }
  }

  Future<string> created;

  if (resource.disk().source().has_id()) {
    // Preprovisioned volumes with RAW type.
    // TODO(chhsiao): Call `ValidateVolumeCapabilities` sequentially
    // once we use the profile module and make profile optional.
    CHECK(volumes.contains(resource.disk().source().id()));
    created = resource.disk().source().id();
  } else {
    // We use the operation UUID as the name of the volume, so the same
    // operation will create the same volume after recovery.
    // TODO(chhsiao): Call `CreateVolume` sequentially with other create
    // or delete operations.
    // TODO(chhsiao): Send `UPDATE_STATE` for RAW resources.
    created = createVolume(
        operationUuid.toString(),
        resource.scalar().value(),
        profiles.at(resource.disk().source().profile()));
  }

  return created
    .then(defer(self(), [=](const string& volumeId) {
      CHECK(volumes.contains(volumeId));
      const csi::state::VolumeState& volumeState = volumes.at(volumeId).state;

      Resource converted = resource;
      converted.mutable_disk()->mutable_source()->set_id(volumeId);
      converted.mutable_disk()->mutable_source()->set_type(type);

      if (!volumeState.volume_attributes().empty()) {
        converted.mutable_disk()->mutable_source()->mutable_metadata()
          ->CopyFrom(convertStringMapToLabels(volumeState.volume_attributes()));
      }

      const string mountPath = csi::paths::getMountPath(
          slave::paths::getCsiRootDir("."),
          info.storage().plugin().type(),
          info.storage().plugin().name(),
          volumeId);

      switch (type) {
        case Resource::DiskInfo::Source::PATH: {
          // Set the root path relative to agent work dir.
          converted.mutable_disk()->mutable_source()->mutable_path()
            ->set_root(mountPath);
          break;
        }
        case Resource::DiskInfo::Source::MOUNT: {
          // Set the root path relative to agent work dir.
          converted.mutable_disk()->mutable_source()->mutable_mount()
            ->set_root(mountPath);
          break;
        }
        case Resource::DiskInfo::Source::BLOCK: {
          break;
        }
        case Resource::DiskInfo::Source::UNKNOWN:
        case Resource::DiskInfo::Source::RAW: {
          UNREACHABLE();
        }
      }

      vector<ResourceConversion> conversions;
      conversions.emplace_back(resource, std::move(converted));

      return conversions;
    }));
}


Future<vector<ResourceConversion>>
StorageLocalResourceProviderProcess::applyDestroyVolumeOrBlock(
    const Resource& resource)
{
  switch (resource.disk().source().type()) {
    case Resource::DiskInfo::Source::PATH:
    case Resource::DiskInfo::Source::MOUNT:
    case Resource::DiskInfo::Source::BLOCK: {
      break;
    }
    case Resource::DiskInfo::Source::UNKNOWN:
    case Resource::DiskInfo::Source::RAW: {
      return Failure(
          "Cannot delete volume of " +
          stringify(resource.disk().source().type()) + " type");
      break;
    }
  }

  CHECK(resource.disk().source().has_id());
  CHECK(volumes.contains(resource.disk().source().id()));

  // Sequentialize the deletion with other operation on the same volume.
  return volumes.at(resource.disk().source().id()).sequence->add(
      std::function<Future<Nothing>()>(
          defer(self(), &Self::deleteVolume, resource.disk().source().id())))
    .then(defer(self(), [=]() {
      Resource converted = resource;
      converted.mutable_disk()->mutable_source()->clear_id();
      converted.mutable_disk()->mutable_source()->clear_metadata();
      converted.mutable_disk()->mutable_source()->set_type(
          Resource::DiskInfo::Source::RAW);
      converted.mutable_disk()->mutable_source()->clear_path();
      converted.mutable_disk()->mutable_source()->clear_mount();

      vector<ResourceConversion> conversions;
      conversions.emplace_back(resource, std::move(converted));

      return conversions;
    }));
}


Try<Nothing> StorageLocalResourceProviderProcess::applyResourceConversions(
    const UUID& operationUuid,
    const Try<vector<ResourceConversion>>& conversions)
{
  Option<Error> error;

  CHECK(pendingOperations.contains(operationUuid));
  const Event::ApplyOfferOperation& operation =
    pendingOperations.at(operationUuid);

  if (conversions.isSome()) {
    // Strip away the allocation info when applying the convertion to
    // the total resources.
    vector<ResourceConversion> _conversions;
    foreach (ResourceConversion conversion, conversions.get()) {
      conversion.consumed.unallocate();
      conversion.converted.unallocate();
      _conversions.push_back(std::move(conversion));
    }

    Try<Resources> result = totalResources.apply(_conversions);
    if (result.isSome()) {
      totalResources = result.get();
    } else {
      error = result.error();
    }
  } else {
    error = conversions.error();
  }

  // We first ask the status update manager to checkpoint the operation
  // status, then checkpoint the resource provider state.
  // TODO(chhsiao): Use the status update manager.
  Call call;
  call.set_type(Call::UPDATE_OFFER_OPERATION_STATUS);
  call.mutable_resource_provider_id()->CopyFrom(info.id());

  Call::UpdateOfferOperationStatus* update =
    call.mutable_update_offer_operation_status();
  update->mutable_framework_id()->CopyFrom(operation.framework_id());
  update->set_operation_uuid(operation.operation_uuid());

  OfferOperationStatus* status = update->mutable_status();
  status->set_status_uuid(UUID::random().toBytes());

  if (operation.info().has_id()) {
    status->mutable_operation_id()->CopyFrom(operation.info().id());
  }

  if (error.isSome()) {
    // We only update the resource version for failed conventional
    // operations, which are speculatively executed on the master.
    if (operation.info().type() == Offer::Operation::RESERVE ||
        operation.info().type() == Offer::Operation::UNRESERVE ||
        operation.info().type() == Offer::Operation::CREATE ||
        operation.info().type() == Offer::Operation::DESTROY) {
      resourceVersion = UUID::random();

      // Send an `UPDATE_STATE` after we finish the current operation.
      dispatch(self(), &Self::sendResourceProviderStateUpdate);
    }

    status->set_state(OFFER_OPERATION_FAILED);
    status->set_message(error->message);
  } else {
    status->set_state(OFFER_OPERATION_FINISHED);

    foreach (const ResourceConversion& conversion, conversions.get()) {
      foreach (const Resource& resource, conversion.converted) {
        status->add_converted_resources()->CopyFrom(resource);
      }
    }
  }

  update->mutable_latest_status()->CopyFrom(*status);

  auto err = [](const UUID& operationUuid, const string& message) {
    LOG(ERROR)
      << "Failed to send status update for offer operation " << operationUuid
      << ": " << message;
  };

  driver->send(evolve(call))
    .onFailed(std::bind(err, operationUuid, lambda::_1))
    .onDiscarded(std::bind(err, operationUuid, "future discarded"));

  pendingOperations.erase(operationUuid);
  checkpointResourceProviderState();

  if (error.isSome()) {
    return error.get();
  }

  return Nothing();
}


void StorageLocalResourceProviderProcess::checkpointResourceProviderState()
{
  ResourceProviderState state;

  foreachvalue (
      const Event::ApplyOfferOperation& operation,
      pendingOperations) {
    state.add_operations()->CopyFrom(operation);
  }

  state.mutable_resources()->CopyFrom(totalResources);

  CHECK_SOME(resourceVersion);
  state.set_resource_version_uuid(resourceVersion->toBytes());

  const string statePath = slave::paths::getResourceProviderStatePath(
      metaDir, slaveId, info.type(), info.name(), info.id());

  CHECK_SOME(slave::state::checkpoint(statePath, state))
    << "Failed to checkpoint resource provider state to '" << statePath << "'";
}


void StorageLocalResourceProviderProcess::sendResourceProviderStateUpdate()
{
  Call call;
  call.set_type(Call::UPDATE_STATE);
  call.mutable_resource_provider_id()->CopyFrom(info.id());

  Call::UpdateState* update = call.mutable_update_state();

  foreachpair (const UUID& uuid,
               const Event::ApplyOfferOperation& operation,
               pendingOperations) {
    // TODO(chhsiao): Maintain a list of terminated but unacknowledged
    // offer operations in memory and reconstruc that during recovery
    // by querying status update manager.
    update->add_operations()->CopyFrom(
        protobuf::createOfferOperation(
            operation.info(),
            protobuf::createOfferOperationStatus(
                OFFER_OPERATION_PENDING,
                operation.info().has_id()
                  ? Option<OfferOperationID>(operation.info().id())
                  : None()),
            operation.framework_id(),
            slaveId,
            uuid));
  }

  update->mutable_resources()->CopyFrom(totalResources);

  CHECK_SOME(resourceVersion);
  update->set_resource_version_uuid(resourceVersion->toBytes());

  auto err = [](const ResourceProviderID& id, const string& message) {
    LOG(ERROR)
      << "Failed to update state for resource provider " << id << ": "
      << message;
  };

  driver->send(evolve(call))
    .onFailed(std::bind(err, info.id(), lambda::_1))
    .onDiscarded(std::bind(err, info.id(), "future discarded"));
}


void StorageLocalResourceProviderProcess::checkpointVolumeState(
    const string& volumeId)
{
  const string statePath = csi::paths::getVolumeStatePath(
      slave::paths::getCsiRootDir(workDir),
      info.storage().plugin().type(),
      info.storage().plugin().name(),
      volumeId);

  CHECK_SOME(slave::state::checkpoint(statePath, volumes.at(volumeId).state))
    << "Failed to checkpoint volume state to '" << statePath << "'";
}


Try<Owned<LocalResourceProvider>> StorageLocalResourceProvider::create(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const SlaveID& slaveId,
    const Option<string>& authToken)
{
  // Verify that the name follows Java package naming convention.
  // TODO(chhsiao): We should move this check to a validation function
  // for `ResourceProviderInfo`.
  if (!isValidName(info.name())) {
    return Error(
        "Resource provider name '" + info.name() +
        "' does not follow Java package naming convention");
  }

  if (!info.has_storage()) {
    return Error("'ResourceProviderInfo.storage' must be set");
  }

  // Verify that the type and name of the CSI plugin follow Java package
  // naming convention.
  // TODO(chhsiao): We should move this check to a validation function
  // for `CSIPluginInfo`.
  if (!isValidType(info.storage().plugin().type()) ||
      !isValidName(info.storage().plugin().name())) {
    return Error(
        "CSI plugin type '" + info.storage().plugin().type() +
        "' and name '" + info.storage().plugin().name() +
        "' does not follow Java package naming convention");
  }

  bool hasControllerService = false;
  bool hasNodeService = false;

  foreach (const CSIPluginContainerInfo& container,
           info.storage().plugin().containers()) {
    for (int i = 0; i < container.services_size(); i++) {
      const CSIPluginContainerInfo::Service service = container.services(i);
      if (service == CSIPluginContainerInfo::CONTROLLER_SERVICE) {
        hasControllerService = true;
      } else if (service == CSIPluginContainerInfo::NODE_SERVICE) {
        hasNodeService = true;
      }
    }
  }

  if (!hasControllerService) {
    return Error(
        stringify(CSIPluginContainerInfo::CONTROLLER_SERVICE) + " not found");
  }

  if (!hasNodeService) {
    return Error(
        stringify(CSIPluginContainerInfo::NODE_SERVICE) + " not found");
  }

  return Owned<LocalResourceProvider>(
      new StorageLocalResourceProvider(url, workDir, info, slaveId, authToken));
}


Try<Principal> StorageLocalResourceProvider::principal(
    const ResourceProviderInfo& info)
{
  return Principal(
      Option<string>::none(),
      {{"cid_prefix", getContainerIdPrefix(info)}});
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
