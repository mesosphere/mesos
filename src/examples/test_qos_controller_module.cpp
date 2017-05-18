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

#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/qos_controller.hpp>

#include <mesos/slave/qos_controller.hpp>

#include <process/owned.hpp>

#include <stout/try.hpp>

#include "slave/qos_controllers/noop.hpp"

using namespace mesos;

using mesos::internal::slave::NoopQoSController;

using mesos::modules::ModuleInfo;

using mesos::slave::QoSController;

using process::Owned;

static Try<Owned<QoSController>> createQoSController(
    const ModuleInfo& moduleInfo)
{
  Try<QoSController*> result = NoopQoSController::create(None());
  if (result.isError()) {
    return Error(
        "Error creating noop QoS controller module: " + result.error());
  }
  return Owned<QoSController>(result.get());
}


// Declares a TestNoopQoSController module named
// 'org_apache_mesos_TestNoopQoSController'.
mesos::modules::Module<QoSController>
  org_apache_mesos_TestNoopQoSController(
      MESOS_MODULE_API_VERSION,
      MESOS_VERSION,
      "Apache Mesos",
      "modules@mesos.apache.org",
      "Test Noop QoS Controller module.",
      nullptr,
      createQoSController);
