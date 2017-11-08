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

#include "slave/container_daemon.hpp"

#include <mesos/agent/agent.hpp>

#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/mutex.hpp>
#include <process/process.hpp>

#include <stout/lambda.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>

#include "common/http.hpp"

#include "internal/evolve.hpp"

namespace http = process::http;

using std::bind;
using std::string;

using mesos::agent::Call;

using process::Failure;
using process::Future;
using process::Mutex;
using process::Owned;
using process::Process;
using process::Promise;

using process::defer;

namespace mesos {
namespace internal {

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


class ContainerDaemonProcess : public Process<ContainerDaemonProcess>
{
public:
  explicit ContainerDaemonProcess(
      const http::URL& _agentUrl,
      const Option<string>& _authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const std::function<Future<Nothing>()>& _postStartHook,
      const std::function<Future<Nothing>()>& _postStopHook);

  ContainerDaemonProcess(const ContainerDaemonProcess& other) = delete;

  ContainerDaemonProcess& operator=(
      const ContainerDaemonProcess& other) = delete;

  void terminate();
  Future<Nothing> wait();

private:
  void initialize() override;

  void started();
  void stopped();

  void _terminate();

  Future<Nothing> launchContainer();
  Future<Nothing> waitContainer();
  Future<Nothing> killContainer();

  const http::URL agentUrl;
  const Option<string> authToken;
  const ContentType contentType;
  const std::function<Future<Nothing>()> postStartHook;
  const std::function<Future<Nothing>()> postStopHook;

  Call launchCall;
  Call killCall;
  Call waitCall;

  Mutex mutex;
  bool terminating;
  Promise<Nothing> terminated;
};


ContainerDaemonProcess::ContainerDaemonProcess(
    const http::URL& _agentUrl,
    const Option<string>& _authToken,
    const ContainerID& containerId,
    const Option<CommandInfo>& commandInfo,
    const Option<Resources>& resources,
    const Option<ContainerInfo>& containerInfo,
    const std::function<Future<Nothing>()>& _postStartHook,
    const std::function<Future<Nothing>()>& _postStopHook)
  : ProcessBase(process::ID::generate("container-daemon")),
    agentUrl(_agentUrl),
    authToken(_authToken),
    contentType(ContentType::PROTOBUF),
    postStartHook(_postStartHook),
    postStopHook(_postStopHook),
    terminating(false)
{
  launchCall.set_type(Call::LAUNCH_CONTAINER);
  launchCall.mutable_launch_container()->mutable_container_id()->CopyFrom(
      containerId);
  if (commandInfo.isSome()) {
    launchCall.mutable_launch_container()->mutable_command()->CopyFrom(
        commandInfo.get());
  }
  if (resources.isSome()) {
    launchCall.mutable_launch_container()->mutable_resources()->CopyFrom(
        resources.get());
  }
  if (containerInfo.isSome()) {
    launchCall.mutable_launch_container()->mutable_container()->CopyFrom(
        containerInfo.get());
  }

  killCall.set_type(Call::KILL_CONTAINER);
  killCall.mutable_kill_container()->mutable_container_id()->CopyFrom(
      containerId);

  waitCall.set_type(Call::WAIT_CONTAINER);
  waitCall.mutable_wait_container()->mutable_container_id()->CopyFrom(
      containerId);
}


void ContainerDaemonProcess::terminate()
{
  dispatch(self(), &Self::_terminate);
}


Future<Nothing> ContainerDaemonProcess::wait()
{
  return terminated.future();
}


void ContainerDaemonProcess::initialize()
{
  // NOTE: We directly invoke `launchContainer()` here so no
  // `killContainer()` can be called before the container is launched.
  launchContainer()
    .onFailed(defer(self(), [=](const string& failure) {
      terminated.fail(failure);
    }))
    .onDiscarded(defer(self(), [=] {
      terminated.discard();
    }));
}


void ContainerDaemonProcess::started()
{
  postStartHook()
    .then(defer(self(), &Self::waitContainer))
    .onFailed(defer(self(), [=](const string& failure) {
      terminated.fail(failure);
    }))
    .onDiscarded(defer(self(), [=] {
      terminated.discard();
    }));
}


void ContainerDaemonProcess::stopped()
{
  postStopHook()
    .then(defer(self(), [=]() -> Future<Nothing> {
      if (terminating) {
        terminated.set(Nothing());
        return Nothing();
      }

      return launchContainer();
    }))
    .onFailed(defer(self(), [=](const string& failure) {
      terminated.fail(failure);
    }))
    .onDiscarded(defer(self(), [=] {
      terminated.discard();
    }));
}


void ContainerDaemonProcess::_terminate()
{
  if (terminating) {
    return;
  }

  terminating = true;
  killContainer()
    .onFailed(defer(self(), [=](const string& failure) {
      terminated.fail(failure);
    }))
    .onDiscarded(defer(self(), [=] {
      terminated.discard();
    }));
}


Future<Nothing> ContainerDaemonProcess::launchContainer()
{
  // NOTE: The mutex ensures mutual exclusion between the call of the
  // launch operation and its response, and the call of the kill
  // operation and its response.
  return mutex.lock()
    .then(defer(self(), [=] {
      return http::post(
          agentUrl,
          getAuthHeader(authToken),
          serialize(contentType, evolve(launchCall)),
          stringify(contentType))
        .then(defer(self(), [=](
            const http::Response& response) -> Future<Nothing> {
          if (response.status != http::OK().status &&
              response.status != http::Accepted().status) {
            return Failure(
                "Failed to launch container '" +
                stringify(launchCall.launch_container().container_id()) +
                "': Unexpected response '" + response.status + "' (" +
                response.body + ")");
          }

          return Nothing();
        }));
    }))
    .onAny(bind(&Mutex::unlock, mutex))
    .onReady(defer(self(), &Self::started));
}


Future<Nothing> ContainerDaemonProcess::waitContainer()
{
  return http::post(
      agentUrl,
      getAuthHeader(authToken),
      serialize(contentType, evolve(waitCall)),
      stringify(contentType))
    .then(defer(self(), [=](const http::Response& response) -> Future<Nothing> {
      if (response.status != http::OK().status &&
          response.status != http::NotFound().status) {
        return Failure(
            "Failed to wait for container '" +
            stringify(waitCall.wait_container().container_id()) +
            "': Unexpected response '" + response.status + "' (" +
            response.body + ")");
      }

      return Nothing();
    }))
    .onReady(defer(self(), &Self::stopped));
}


Future<Nothing> ContainerDaemonProcess::killContainer()
{
  // NOTE: The mutex ensures mutual exclusion between the call of the
  // launch operation and its response, and the call of the kill
  // operation and its response.
  return mutex.lock()
    .then(defer(self(), [=] {
      return http::post(
          agentUrl,
          getAuthHeader(authToken),
          serialize(contentType, evolve(killCall)),
          stringify(contentType))
        .then(defer(self(), [=](
            const http::Response& response) -> Future<Nothing> {
          if (response.status != http::OK().status) {
            return Failure(
                "Failed to kill container '" +
                stringify(killCall.kill_container().container_id()) +
                "': Unexpected response '" + response.status + "' (" +
                response.body + ")");
          }

          return Nothing();
        }));
    }))
    .onAny(bind(&Mutex::unlock, mutex));
}


Try<Owned<ContainerDaemon>> ContainerDaemon::create(
    const http::URL& agentUrl,
    const Option<string>& authToken,
    const ContainerID& containerId,
    const Option<CommandInfo>& commandInfo,
    const Option<Resources>& resources,
    const Option<ContainerInfo>& containerInfo,
    const std::function<Future<Nothing>()>& postStartHook,
    const std::function<Future<Nothing>()>& postStopHook)
{
  return Owned<ContainerDaemon>(new ContainerDaemon(
      agentUrl,
      authToken,
      containerId,
      commandInfo,
      resources,
      containerInfo,
      postStartHook,
      postStopHook));
}


ContainerDaemon::ContainerDaemon(
    const http::URL& agentUrl,
    const Option<string>& authToken,
    const ContainerID& containerId,
    const Option<CommandInfo>& commandInfo,
    const Option<Resources>& resources,
    const Option<ContainerInfo>& containerInfo,
    const std::function<Future<Nothing>()>& postStartHook,
    const std::function<Future<Nothing>()>& postStopHook)
  : process(new ContainerDaemonProcess(
        agentUrl,
        authToken,
        containerId,
        commandInfo,
        resources,
        containerInfo,
        postStartHook,
        postStopHook))
{
  spawn(CHECK_NOTNULL(process.get()));
}


ContainerDaemon::~ContainerDaemon()
{
  process::terminate(process.get());
  process::wait(process.get());
}


void ContainerDaemon::terminate()
{
  process->terminate();
}


Future<Nothing> ContainerDaemon::wait()
{
  return process->wait();
}

} // namespace internal {
} // namespace mesos {
