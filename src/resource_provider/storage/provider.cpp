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

using std::queue;
using std::shared_ptr;
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
using process::terminate;
using process::wait;

using process::http::authentication::Principal;

using mesos::ResourceProviderInfo;

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
