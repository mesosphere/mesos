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

#include <glog/logging.h>

#include <process/after.hpp>
#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/process.hpp>
#include <process/timeout.hpp>

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

using std::queue;
using std::string;

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
using process::defer;
using process::loop;
using process::spawn;

using process::http::authentication::Principal;

using mesos::ResourceProviderInfo;

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
}


void StorageLocalResourceProviderProcess::operation(
    const Event::Operation& operation)
{
}


void StorageLocalResourceProviderProcess::publish(const Event::Publish& publish)
{
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
