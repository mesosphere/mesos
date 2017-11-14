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

#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

#include <mesos/resource_provider/resource_provider.hpp>

#include <mesos/v1/resource_provider.hpp>

#include <stout/os.hpp>

#include "common/http.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "resource_provider/detector.hpp"

#include "resource_provider/storage/paths.hpp"

#include "slave/paths.hpp"

namespace http = process::http;

using std::queue;
using std::string;

using process::Owned;
using process::Process;

using process::defer;
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


// Returns a prefix for naming components of the resource provider. This
// can be used in various places, such as container IDs for CSI plugins.
static inline string getPrefix(const ResourceProviderInfo& info)
{
  return SLRP_NAME_PREFIX + http::encode(info.name(), SLRP_NAME_RESERVED) + "-";
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

  const http::URL url;
  const string workDir;
  const string metaDir;
  const ContentType contentType;
  ResourceProviderInfo info;
  const SlaveID slaveId;
  const Option<string> authToken;

  Owned<v1::resource_provider::Driver> driver;
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
