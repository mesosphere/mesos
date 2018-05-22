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

#include <stdint.h>

#include <vector>
#include <utility>

#include <mesos/authorizer/authorizer.hpp>

#include <mesos/master/detector.hpp>

#include <mesos/mesos.hpp>

#include <mesos/module/anonymous.hpp>

#include <mesos/slave/resource_estimator.hpp>

#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/check.hpp>
#include <stout/flags.hpp>
#include <stout/hashset.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>
#include <stout/version.hpp>

#include "common/build.hpp"
#include "common/http.hpp"

#include "hook/manager.hpp"

#ifdef __linux__
#include "linux/systemd.hpp"
#endif // __linux__

#include "logging/logging.hpp"

#include "messages/flags.hpp"
#include "messages/messages.hpp"

#include "module/manager.hpp"

#include "slave/gc.hpp"
#include "slave/slave.hpp"
#include "slave/status_update_manager.hpp"

#include "version/version.hpp"

using namespace mesos::internal;
using namespace mesos::internal::slave;

using mesos::master::detector::MasterDetector;

using mesos::modules::Anonymous;
using mesos::modules::ModuleManager;

using mesos::master::detector::MasterDetector;

using mesos::slave::QoSController;
using mesos::slave::ResourceEstimator;

using mesos::Authorizer;
using mesos::SlaveInfo;

using process::Owned;

using process::firewall::DisabledEndpointsFirewallRule;
using process::firewall::FirewallRule;

using std::cerr;
using std::cout;
using std::endl;
using std::move;
using std::string;
using std::vector;


int main(int argc, char** argv)
{
  // The order of initialization is as follows:
  // * Windows socket stack.
  // * Validate flags.
  // * Log build information.
  // * Libprocess
  // * Logging
  // * Version process
  // * Firewall rules: should be initialized before initializing HTTP endpoints.
  // * Modules: Load module libraries and manifests before they
  //   can be instantiated.
  // * Anonymous modules: Later components such as Allocators, and master
  //   contender/detector might depend upon anonymous modules.
  // * Hooks.
  // * Systemd support (if it exists).
  // * Fetcher and Containerizer.
  // * Master detector.
  // * Authorizer.
  // * Garbage collector.
  // * Status update manager.
  // * Resource estimator.
  // * QoS controller.
  // * `Agent` process.
  //
  // TODO(avinash): Add more comments discussing the rationale behind for this
  // particular component ordering.

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  slave::Flags flags;

  Try<flags::Warnings> load = flags.load("MESOS_", argc, argv);

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.version) {
    cout << "mesos" << " " << MESOS_VERSION << endl;
    return EXIT_SUCCESS;
  }

  // TODO(marco): this pattern too should be abstracted away
  // in FlagsBase; I have seen it at least 15 times.
  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  // Check that agent's version has the expected format (SemVer).
  {
    Try<Version> version = Version::parse(MESOS_VERSION);
    if (version.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse Mesos version '" << MESOS_VERSION << "': "
        << version.error();
    }
  }

  if (flags.master.isNone() && flags.master_detector.isNone()) {
    cerr << flags.usage("Missing required option `--master` or "
                        "`--master_detector`.") << endl;
    return EXIT_FAILURE;
  }

  if (flags.master.isSome() && flags.master_detector.isSome()) {
    cerr << flags.usage("Only one of --master or --master_detector options "
                        "should be specified.");
    return EXIT_FAILURE;
  }

  // Initialize libprocess.
  if (flags.ip_discovery_command.isSome() && flags.ip.isSome()) {
    EXIT(EXIT_FAILURE) << flags.usage(
        "Only one of `--ip` or `--ip_discovery_command` should be specified");
  }

  if (flags.ip_discovery_command.isSome()) {
    Try<string> ipAddress = os::shell(flags.ip_discovery_command.get());

    if (ipAddress.isError()) {
      EXIT(EXIT_FAILURE) << ipAddress.error();
    }

    os::setenv("LIBPROCESS_IP", strings::trim(ipAddress.get()));
  } else if (flags.ip.isSome()) {
    os::setenv("LIBPROCESS_IP", flags.ip.get());
  }

  os::setenv("LIBPROCESS_PORT", stringify(flags.port));

  if (flags.advertise_ip.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_IP", flags.advertise_ip.get());
  }

  if (flags.advertise_port.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_PORT", flags.advertise_port.get());
  }

  // Log build information.
  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Version: " << MESOS_VERSION;

  if (build::GIT_TAG.isSome()) {
    LOG(INFO) << "Git tag: " << build::GIT_TAG.get();
  }

  if (build::GIT_SHA.isSome()) {
    LOG(INFO) << "Git SHA: " << build::GIT_SHA.get();
  }

  const string id = process::ID::generate("slave"); // Process ID.

  // If `process::initialize()` returns `false`, then it was called before this
  // invocation, meaning the authentication realm for libprocess-level HTTP
  // endpoints was set incorrectly. This should be the first invocation.
  if (!process::initialize(
          id,
          READWRITE_HTTP_AUTHENTICATION_REALM,
          READONLY_HTTP_AUTHENTICATION_REALM)) {
    EXIT(EXIT_FAILURE) << "The call to `process::initialize()` in the agent's "
                       << "`main()` was not the function's first invocation";
  }

  logging::initialize(argv[0], flags, true); // Catch signals.

  // Log any flag warnings (after logging is initialized).
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  spawn(new VersionProcess(), true);

  if (flags.firewall_rules.isSome()) {
    vector<Owned<FirewallRule>> rules;

    const Firewall firewall = flags.firewall_rules.get();

    if (firewall.has_disabled_endpoints()) {
      hashset<string> paths;

      foreach (const string& path, firewall.disabled_endpoints().paths()) {
        paths.insert(path);
      }

      rules.emplace_back(new DisabledEndpointsFirewallRule(paths));
    }

    process::firewall::install(move(rules));
  }

  // Initialize modules.
  if (flags.modules.isSome() && flags.modulesDir.isSome()) {
    EXIT(EXIT_FAILURE) <<
      flags.usage("Only one of --modules or --modules_dir should be specified");
  }

  if (flags.modulesDir.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modulesDir.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
  }

  if (flags.modules.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modules.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
  }

  // Create anonymous modules.
  foreach (const string& name, ModuleManager::find<Anonymous>()) {
    Try<Anonymous*> create = ModuleManager::create<Anonymous>(name);
    if (create.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create anonymous module named '" << name << "'";
    }

    // We don't bother keeping around the pointer to this anonymous
    // module, when we exit that will effectively free its memory.
    //
    // TODO(benh): We might want to add explicit finalization (and
    // maybe explicit initialization too) in order to let the module
    // do any housekeeping necessary when the slave is cleanly
    // terminating.
  }

  // Initialize hooks.
  if (flags.hooks.isSome()) {
    Try<Nothing> result = HookManager::initialize(flags.hooks.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error installing hooks: " << result.error();
    }
  }

#ifdef __linux__
  // Initialize systemd if it exists.
  if (flags.systemd_enable_support && systemd::exists()) {
    LOG(INFO) << "Inializing systemd state";

    systemd::Flags systemdFlags;
    systemdFlags.enabled = flags.systemd_enable_support;
    systemdFlags.runtime_directory = flags.systemd_runtime_directory;
    systemdFlags.cgroups_hierarchy = flags.cgroups_hierarchy;

    Try<Nothing> initialize = systemd::initialize(systemdFlags);
    if (initialize.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to initialize systemd: " + initialize.error();
    }
  }
#endif // __linux__

  Fetcher* fetcher = new Fetcher();

  Try<Containerizer*> containerizer =
    Containerizer::create(flags, false, fetcher);

  if (containerizer.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a containerizer: " << containerizer.error();
  }

  Try<MasterDetector*> detector_ = MasterDetector::create(
      flags.master, flags.master_detector);

  if (detector_.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a master detector: " << detector_.error();
  }

  MasterDetector* detector = detector_.get();

  Option<Authorizer*> authorizer_ = None();

  string authorizerName = flags.authorizer;

  Result<Authorizer*> authorizer((None()));
  if (authorizerName != slave::DEFAULT_AUTHORIZER) {
    LOG(INFO) << "Creating '" << authorizerName << "' authorizer";

    // NOTE: The contents of --acls will be ignored.
    authorizer = Authorizer::create(authorizerName);
  } else {
    // `authorizerName` is `DEFAULT_AUTHORIZER` at this point.
    if (flags.acls.isSome()) {
      LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

      authorizer = Authorizer::create(flags.acls.get());
    }
  }

  if (authorizer.isError()) {
    EXIT(EXIT_FAILURE) << "Could not create '" << authorizerName
                       << "' authorizer: " << authorizer.error();
  } else if (authorizer.isSome()) {
    authorizer_ = authorizer.get();

    // Set the authorization callbacks for libprocess HTTP endpoints.
    // Note that these callbacks capture `authorizer_.get()`, but the agent
    // creates a copy of the authorizer during construction. Thus, if in the
    // future it becomes possible to dynamically set the authorizer, this would
    // break.
    process::http::authorization::setCallbacks(
        createAuthorizationCallbacks(authorizer_.get()));
  }

  Files* files = new Files(READONLY_HTTP_AUTHENTICATION_REALM, authorizer_);
  GarbageCollector* gc = new GarbageCollector(flags.work_dir);
  StatusUpdateManager* statusUpdateManager = new StatusUpdateManager(flags);

  Try<ResourceEstimator*> resourceEstimator =
    ResourceEstimator::create(flags.resource_estimator);

  if (resourceEstimator.isError()) {
    cerr << "Failed to create resource estimator: "
         << resourceEstimator.error() << endl;
    return EXIT_FAILURE;
  }

  Try<QoSController*> qosController =
    QoSController::create(flags.qos_controller);

  if (qosController.isError()) {
    cerr << "Failed to create QoS Controller: "
         << qosController.error() << endl;
    return EXIT_FAILURE;
  }

  Slave* slave = new Slave(
      id,
      flags,
      detector,
      containerizer.get(),
      files,
      gc,
      statusUpdateManager,
      resourceEstimator.get(),
      qosController.get(),
      authorizer_);

  process::spawn(slave);
  process::wait(slave->self());

  delete slave;

  delete qosController.get();

  delete resourceEstimator.get();

  delete statusUpdateManager;

  delete gc;

  delete files;

  if (authorizer_.isSome()) {
    delete authorizer_.get();
  }

  delete detector;

  delete containerizer.get();

  delete fetcher;

  // NOTE: We need to finalize libprocess, on Windows especially,
  // as any binary that uses the networking stack on Windows must
  // also clean up the networking stack before exiting.
  process::finalize(true);
  return EXIT_SUCCESS;
}
