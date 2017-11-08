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

#ifndef __SLAVE_CONTAINER_DAEMON_HPP__
#define __SLAVE_CONTAINER_DAEMON_HPP__

#include <functional>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace mesos {
namespace internal {

// Forward declarations.
class ContainerDaemonProcess;


/**
 * A daemon that launches and monitors a service running in a standalone
 * container.
 *
 * The `ContainerDaemon` monitors a long-running service in a standalone
 * container, and automatically relaunches the container when it exits.
 *
 * NOTE: The `ContainerDaemon` itself is not responsible to manage the
 * lifecycle of the service container it monitors.
 */
class ContainerDaemon
{
public:
  /**
   * Creates and initializes a container daemon instance.
   *
   * Once initialized, the daemon idempotently launches the container,
   * then run the `postStartHook` function. Upon container exits, it
   * executes the `postStopHook` function and then relaunches the
   * container. Any failed or discarded future returned by the hook
   * functions will be reflected by the `wait()` method.
   *
   * @param agentUrl URL to the agent endpoint to launch the container.
   * @param containerId ID of the container running the service.
   * @param commandInfo command to start the service.
   * @param resources resources allocated to run the service.
   * @param containerInfo information to setup the container.
   * @param postStartHook an asynchronous function that is called after
   *     the daemon launches the container. This function must not hang.
   * @param postStopHook an asynchronous function that is called after
   *     the container exits. This function must not hang.
   */
  static Try<process::Owned<ContainerDaemon>> create(
      const process::http::URL& agentUrl,
      const Option<std::string>& authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const std::function<process::Future<Nothing>()>& postStartHook,
      const std::function<process::Future<Nothing>()>& postStopHook);

  ~ContainerDaemon();

  /**
   * Kills the service container and stops relaunching.
   */
  void terminate();

  /**
   * Asynchronously waits for the service contanier to exit after
   * `terminate()` is called.
   */
  process::Future<Nothing> wait();

private:
  explicit ContainerDaemon(
      const process::http::URL& agentUrl,
      const Option<std::string>& authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const std::function<process::Future<Nothing>()>& postStartHook,
      const std::function<process::Future<Nothing>()>& postStopHook);

  process::Owned<ContainerDaemonProcess> process;
};

} // namespace internal {
} // namespace mesos {

#endif // __SLAVE_CONTAINER_DAEMON_HPP__
