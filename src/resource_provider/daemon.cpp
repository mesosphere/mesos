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

#include "resource_provider/daemon.hpp"

#include <utility>
#include <vector>

#include <glog/logging.h>

#include <mesos/type_utils.hpp>

#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

#include <stout/foreach.hpp>
#include <stout/json.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/try.hpp>

#include "common/validation.hpp"

#include "resource_provider/local.hpp"

namespace http = process::http;

using std::list;
using std::string;
using std::vector;

using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::ProcessBase;

using process::defer;
using process::dispatch;
using process::spawn;
using process::terminate;
using process::wait;

using process::http::authentication::Principal;

namespace mesos {
namespace internal {

class LocalResourceProviderDaemonProcess
  : public Process<LocalResourceProviderDaemonProcess>
{
public:
  LocalResourceProviderDaemonProcess(
      const process::http::URL& _url,
      const string& _workDir,
      const string& _configDir,
      const SlaveID& _slaveId,
      SecretGenerator* _secretGenerator)
    : ProcessBase(process::ID::generate("local-resource-provider-daemon")),
      url(_url),
      workDir(_workDir),
      configDir(_configDir),
      slaveId(_slaveId),
      secretGenerator(_secretGenerator) {}

protected:
  void initialize() override;

private:
  struct Provider
  {
    Provider(const ResourceProviderInfo& _info,
             Owned<LocalResourceProvider> _provider)
      : info(_info),
        provider(std::move(_provider)) {}

    const ResourceProviderInfo info;
    const Owned<LocalResourceProvider> provider;
  };

  Future<Nothing> load(const string& path);

  Future<Option<string>> generateAuthToken(const ResourceProviderInfo& info);

  const http::URL url;
  const string workDir;
  const string configDir;
  const SlaveID slaveId;
  SecretGenerator* const secretGenerator;

  vector<Provider> providers;
};


void LocalResourceProviderDaemonProcess::initialize()
{
  Try<list<string>> entries = os::ls(configDir);
  if (entries.isError()) {
    LOG(ERROR) << "Unable to list the resource provider config directory '"
               << configDir << "': " << entries.error();
    return;
  }

  foreach (const string& entry, entries.get()) {
    const string path = path::join(configDir, entry);

    if (os::stat::isdir(path)) {
      continue;
    }

    dispatch(self(), &Self::load, path)
      .onFailed([=](const string& failure) {
        LOG(ERROR) << "Failed to load resource provider config '"
                   << path << "': " << failure;
      });
  }
}


Future<Nothing> LocalResourceProviderDaemonProcess::load(const string& path)
{
  Try<string> read = os::read(path);
  if (read.isError()) {
    return Failure("Failed to read the config file: " + read.error());
  }

  Try<JSON::Object> json = JSON::parse<JSON::Object>(read.get());
  if (json.isError()) {
    return Failure("Failed to parse the JSON config: " + json.error());
  }

  Try<ResourceProviderInfo> info =
    ::protobuf::parse<ResourceProviderInfo>(json.get());

  if (info.isError()) {
    return Failure("Not a valid resource provider config: " + info.error());
  }

  // Ensure that ('type', 'name') pair is unique.
  foreach (const Provider& provider, providers) {
    if (info->type() == provider.info.type() &&
        info->name() == provider.info.name()) {
      return Failure(
          "Multiple resource providers with type '" + info->type() +
          "' and name '" + info->name() + "'");
    }
  }

  return generateAuthToken(info.get())
    .then(defer(self(), [=](const Option<string>& token) -> Future<Nothing> {
      Try<Owned<LocalResourceProvider>> provider =
        LocalResourceProvider::create(url, workDir, info.get(), slaveId, token);

      if (provider.isError()) {
        return Failure(
            "Failed to create resource provider with type '" + info->type() +
            "' and name '" + info->name() + "': " + provider.error());
      }

      providers.emplace_back(info.get(), provider.get());

      return Nothing();
    }));
}


// Generates a secret for local resource provider authentication if needed.
Future<Option<string>> LocalResourceProviderDaemonProcess::generateAuthToken(
    const ResourceProviderInfo& info)
{
  if (secretGenerator) {
    Try<Principal> principal = LocalResourceProvider::principal(info);

    if (principal.isError()) {
      return Failure(
          "Failed to generate resource provider principal with type '" +
          info.type() + "' and name '" + info.name() + "': " +
          principal.error());
    }

    return secretGenerator->generate(principal.get())
      .then(defer(self(), [](const Secret& secret) -> Future<Option<string>> {
        Option<Error> error = common::validation::validateSecret(secret);

        if (error.isSome()) {
          return Failure(
              "Failed to validate generated secret: " + error->message);
        } else if (secret.type() != Secret::VALUE) {
          return Failure(
              "Expecting generated secret to be of VALUE type insteaf of " +
              stringify(secret.type()) + " type; " +
              "only VALUE type secrets are supported at this time");
        }

        CHECK(secret.has_value());

        return secret.value().data();
      }));
  }

  return None();
}


Try<Owned<LocalResourceProviderDaemon>> LocalResourceProviderDaemon::create(
    const process::http::URL& url,
    const slave::Flags& flags)
{
  // We require that the config directory exists to create a daemon.
  Option<string> configDir = flags.resource_provider_config_dir;
  if (configDir.isSome() && !os::exists(configDir.get())) {
    return Error("Config directory '" + configDir.get() + "' does not exist");
  }

  return new LocalResourceProviderDaemon(
      url,
      flags.work_dir,
      configDir);
}


LocalResourceProviderDaemon::LocalResourceProviderDaemon(
    const process::http::URL& _url,
    const string& _workDir,
    const Option<string>& _configDir)
  : url(_url),
    workDir(_workDir),
    configDir(_configDir)
{
}


LocalResourceProviderDaemon::~LocalResourceProviderDaemon()
{
  if (process.get() != nullptr) {
    terminate(process.get());
    wait(process.get());
  }
}


Try<Nothing> LocalResourceProviderDaemon::start(
    const SlaveID& slaveId,
    SecretGenerator* secretGenerator)
{
  if (configDir.isNone()) {
    return Nothing();
  }

  if (process.get() != nullptr) {
    return Error("Daemon is already started");
  }

  process.reset(new LocalResourceProviderDaemonProcess(
      url,
      workDir,
      configDir.get(),
      slaveId,
      secretGenerator));

  spawn(CHECK_NOTNULL(process.get()));

  return Nothing();
}

} // namespace internal {
} // namespace mesos {
