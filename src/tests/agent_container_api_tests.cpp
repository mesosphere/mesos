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

#include <string>
#include <tuple>
#include <vector>

#include <mesos/http.hpp>

#include <mesos/v1/resources.hpp>

#include <mesos/v1/master/master.hpp>

#include <mesos/v1/scheduler/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>

#include <stout/gtest.hpp>
#include <stout/jsonify.hpp>
#include <stout/nothing.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/http.hpp"

#include "master/detector/standalone.hpp"

#include "slave/slave.hpp"

#include "slave/containerizer/fetcher.hpp"

#include "slave/containerizer/mesos/containerizer.hpp"

#include "tests/mesos.hpp"

namespace http = process::http;

using std::string;
using std::tuple;
using std::vector;

using mesos::master::detector::MasterDetector;
using mesos::master::detector::StandaloneMasterDetector;

using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;
using mesos::internal::slave::Slave;

using process::Clock;
using process::Failure;
using process::Future;
using process::Owned;

using testing::_;
using testing::Return;
using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {

enum ContainerLaunchType
{
  NORMAL,
  STANDALONE
};


// These tests are parameterized by:
// 1) The type of any parent container.
//    A NORMAL parent is launched with a framework and executor.
//    A STANDALONE parent is launched via `LAUNCH_CONTAINER`.
// 2) The type of any nested container.
//    A NORMAL nested container is launched via `LAUNCH_NESTED_CONTAINER`.
//    A STANDALONE nested contaienr is launched via `LAUNCH_CONTAINER`.
// 3) The content type of the HTTP request(s).
// 4) A tuple containing:
//    1) The isolators to enable during the test.
//    2) The launcher to use.
//    3) And, in case the isolator/launcher requires specific permissions
//       or capabilities, the last parameter is a free-form string used
//       to supply additional test filters.
class AgentContainerAPITest
  : public MesosTest,
    public WithParamInterface<
      tuple<
        ContainerLaunchType,
        ContainerLaunchType,
        ContentType,
        tuple<string, string, string>>>
{
public:
  // Helper function to post a request to the `/api/v1` agent endpoint
  // and return the response.
  Future<http::Response> post(
      const process::PID<slave::Slave>& pid,
      const v1::agent::Call& call)
  {
    ContentType contentType = std::get<2>(GetParam());

    http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    headers["Accept"] = stringify(contentType);

    return http::post(
        pid,
        "api/v1",
        headers,
        serialize(contentType, call),
        stringify(contentType));
  }

  // Helper function to deserialize the response from the `/api/v1` agent
  // endpoint as a V1 response protobuf.
  Future<v1::agent::Response> deserialize(
      const Future<http::Response>& response)
  {
    ContentType contentType = std::get<2>(GetParam());

    return response
      .then([contentType](const http::Response& response)
            -> Future<v1::agent::Response> {
        if (response.status != http::OK().status) {
          return Failure(
              "Unexpected response status: " + response.status +
              ": " + response.body);
        }

        return mesos::internal::deserialize<v1::agent::Response>(
            contentType, response.body);
      });
  }

  // Helper function to launch a top level container based on the first
  // test parameter:
  //   * NORMAL mode will launch a scheduler, executor, and task in order
  //     to create a top level container and uses the `GET_CONTAINERS`
  //     agent API call to retrieve the ContainerID (this method assumes
  //     there is only one container present on the agent).
  //   * STANDALONE mode uses the `LAUNCH_CONTAINER` agent API to create
  //     a top level container. The ContainerID is generated by the callee.
  //
  // Returns the ContainerID of the created container.
  Try<v1::ContainerID> launchParentContainer(
      const process::PID<master::Master>& master,
      const process::PID<slave::Slave>& slave)
  {
    switch (std::get<0>(GetParam())) {
      case ContainerLaunchType::NORMAL: {
        normal = NormalParentContainerDependencies(master);

        // Launch a normal executor and sleep task.
        EXPECT_CALL(
            *(normal->scheduler), registered(normal->driver.get(), _, _));

        Future<vector<Offer>> offers;
        EXPECT_CALL(
            *(normal->scheduler), resourceOffers(normal->driver.get(), _))
          .WillOnce(FutureArg<1>(&offers))
          .WillRepeatedly(Return()); // Ignore subsequent offers.

        normal->driver->start();

        offers.await(Seconds(15));
        if (!offers.isReady() || offers->empty()) {
          return Error("Failed to get offer(s)");
        }

        TaskInfo task = createTask(offers->front(), SLEEP_COMMAND(1000));

        Future<TaskStatus> statusRunning;
        EXPECT_CALL(*(normal->scheduler), statusUpdate(normal->driver.get(), _))
          .WillOnce(FutureArg<1>(&statusRunning))
          .WillRepeatedly(Return()); // Ignore subsequent status updates.

        normal->driver->launchTasks(offers->front().id(), {task});

        statusRunning.await(Seconds(15));
        if (!statusRunning.isReady() &&
            statusRunning->state() != TASK_RUNNING) {
          return Error("Failed to launch parent container");
        }

        // Use the GET_CONTAINERS call to retrieve the ContainerID.
        v1::agent::Call call;
        call.set_type(v1::agent::Call::GET_CONTAINERS);

        Future<v1::agent::Response> containers = deserialize(post(slave, call));

        containers.await(Seconds(15));
        if (!containers.isReady() ||
            containers->get_containers().containers_size() != 1u) {
          return Error("Failed to get parent ContainerID");
        }

        return containers->get_containers().containers(0).container_id();
      }

      case ContainerLaunchType::STANDALONE: {
        // TODO(josephw): The agent should not need to register with
        // the master to launch standalone containers.
        Future<SlaveRegisteredMessage> slaveRegisteredMessage =
          FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

        slaveRegisteredMessage.await(Seconds(15));
        if (!slaveRegisteredMessage.isReady()) {
          return Error("Failed to register agent");
        }

        // Launch a standalone parent container.
        v1::ContainerID containerId;
        containerId.set_value(UUID::random().toString());

        v1::agent::Call call;
        call.set_type(v1::agent::Call::LAUNCH_CONTAINER);

        call.mutable_launch_container()->mutable_container_id()
          ->CopyFrom(containerId);

        call.mutable_launch_container()->mutable_command()
          ->set_value(SLEEP_COMMAND(1000));

        v1::Resources resources = v1::Resources::parse("cpus:0.1;mem:32").get();
        call.mutable_launch_container()->mutable_resources()
          ->CopyFrom(resources);

        Future<http::Response> response = post(slave, call);

        AWAIT_EXPECT_RESPONSE_STATUS_EQ(http::OK().status, response);

        return containerId;
      }
    }

    UNREACHABLE();
  }

  // Helper function to launch a nested container under the given ContainerID
  // based on the second test parameter:
  //   * NORMAL mode uses the (deprecated) `LAUNCH_NESTED_CONTAINER` API.
  //   * STANDALONE mode uses the `LAUNCH_CONTAINER` API.
  Future<http::Response> launchNestedContainer(
      const process::PID<slave::Slave>& slave,
      const v1::ContainerID& containerId,
      const Option<mesos::v1::CommandInfo>& commandInfo = None(),
      const Option<mesos::v1::ContainerInfo>& containerInfo = None())
  {
    v1::agent::Call call;

    switch (std::get<1>(GetParam())) {
      case ContainerLaunchType::NORMAL: {
        call.set_type(v1::agent::Call::LAUNCH_NESTED_CONTAINER);

        call.mutable_launch_nested_container()->mutable_container_id()
          ->CopyFrom(containerId);

        call.mutable_launch_nested_container()->mutable_command()
          ->set_value(SLEEP_COMMAND(100));

        if (commandInfo.isSome()) {
          call.mutable_launch_nested_container()->mutable_command()
            ->CopyFrom(commandInfo.get());
        }

        if (containerInfo.isSome()) {
          call.mutable_launch_nested_container()->mutable_container()
            ->CopyFrom(containerInfo.get());
        }
        break;
      }

      case ContainerLaunchType::STANDALONE: {
        call.set_type(v1::agent::Call::LAUNCH_CONTAINER);

        call.mutable_launch_container()->mutable_container_id()
          ->CopyFrom(containerId);

        call.mutable_launch_container()->mutable_command()
          ->set_value(SLEEP_COMMAND(100));

        if (commandInfo.isSome()) {
          call.mutable_launch_container()->mutable_command()
            ->CopyFrom(commandInfo.get());
        }

        if (containerInfo.isSome()) {
          call.mutable_launch_container()->mutable_container()
            ->CopyFrom(containerInfo.get());
        }
        break;
      }
    }

    return post(slave, call);
  }

  // Helper function to wait for a nested container to terminate,
  // based on the second test parameter:
  //   * NORMAL mode uses the (deprecated) `WAIT_NESTED_CONTAINER` API.
  //   * STANDALONE mode uses the `WAIT_CONTAINER` API.
  Future<http::Response> waitNestedContainer(
      const process::PID<slave::Slave>& slave,
      const v1::ContainerID& containerId)
  {
    v1::agent::Call call;

    switch (std::get<1>(GetParam())) {
      case ContainerLaunchType::NORMAL: {
        call.set_type(v1::agent::Call::WAIT_NESTED_CONTAINER);

        call.mutable_wait_nested_container()->mutable_container_id()
          ->CopyFrom(containerId);
        break;
      }

      case ContainerLaunchType::STANDALONE: {
        call.set_type(v1::agent::Call::WAIT_CONTAINER);

        call.mutable_wait_container()->mutable_container_id()
          ->CopyFrom(containerId);
        break;
      }
    }

    return post(slave, call);
  }

  // Helper function to check the response to a `WAIT_[NESTED_]CONTAINER`
  // call according to the given signal or lack of signal.
  // The expected response type is based on the second test parameter.
  // See `waitNestedContainer`.
  bool checkWaitContainerResponse(
      const Future<v1::agent::Response>& response,
      const Option<int>& signal)
  {
    switch (std::get<1>(GetParam())) {
      case ContainerLaunchType::NORMAL: {
        if (response->type() != v1::agent::Response::WAIT_NESTED_CONTAINER) {
          return false;
        }

        if (signal.isSome()) {
          if (!response->wait_nested_container().has_exit_status()) {
            return false;
          }

          if (response->wait_nested_container().exit_status() != signal.get()) {
            return false;
          }
        }
        break;
      }

      case ContainerLaunchType::STANDALONE: {
        if (response->type() != v1::agent::Response::WAIT_CONTAINER) {
          return false;
        }

        if (signal.isSome()) {
          if (!response->wait_container().has_exit_status()) {
            return false;
          }

          if (response->wait_container().exit_status() != signal.get()) {
            return false;
          }
        }
        break;
      }
    }

    return true;
  }

  // Helper function to kill a nested container, based on the second
  // test parameter:
  //   * NORMAL mode uses the (deprecated) `KILL_NESTED_CONTAINER` API.
  //   * STANDALONE mode uses the `KILL_CONTAINER` API.
  Future<http::Response> killNestedContainer(
      const process::PID<slave::Slave>& slave,
      const v1::ContainerID& containerId)
  {
    v1::agent::Call call;

    switch (std::get<1>(GetParam())) {
      case ContainerLaunchType::NORMAL: {
        call.set_type(v1::agent::Call::KILL_NESTED_CONTAINER);

        call.mutable_kill_nested_container()->mutable_container_id()
          ->CopyFrom(containerId);
        break;
      }

      case ContainerLaunchType::STANDALONE: {
        call.set_type(v1::agent::Call::KILL_CONTAINER);

        call.mutable_kill_container()->mutable_container_id()
          ->CopyFrom(containerId);
        break;
      }
    }

    return post(slave, call);
  }

protected:
  virtual void TearDown()
  {
    if (normal.isSome()) {
      normal->driver->stop();
      normal->driver->join();

      normal = None();
    }

    MesosTest::TearDown();
  }

private:
  struct NormalParentContainerDependencies
  {
    NormalParentContainerDependencies(
        const process::PID<master::Master>& master)
      : scheduler(new MockScheduler())
    {
      // Enable checkpointing for the framework.
      FrameworkInfo framework = DEFAULT_FRAMEWORK_INFO;
      framework.set_checkpoint(true);

      driver.reset(new MesosSchedulerDriver(
          scheduler.get(),
          framework,
          master,
          DEFAULT_CREDENTIAL));
    }

    // NOTE: These need to be pointers due to how each mock's copy constructor
    // is explicitly deleted (for good reason) and because `Option` requires
    // said copy constructors for certain operations.
    Owned<MockScheduler> scheduler;
    Owned<MesosSchedulerDriver> driver;
  };

  Option<NormalParentContainerDependencies> normal;
};


INSTANTIATE_TEST_CASE_P(
    ParentChildContainerTypeAndContentType,
    AgentContainerAPITest,
    ::testing::Combine(
      ::testing::Values(
        ContainerLaunchType::NORMAL,
        ContainerLaunchType::STANDALONE),
      ::testing::Values(
        ContainerLaunchType::NORMAL,
        ContainerLaunchType::STANDALONE),
      ::testing::Values(
        ContentType::JSON,
        ContentType::PROTOBUF),
      ::testing::Values(
        make_tuple(
            string("posix/cpu,posix/mem"),
            string("posix"),
            string()),
        make_tuple(
            string("cgroups/cpu,cgroups/mem,filesystem/linux,namespaces/pid"),
            string("linux"),
            string("ROOT_CGROUPS_")))));


// This test runs through the basic workflow of launching a nested container,
// killing the nested container, and retrieving the exit status (SIGKILL).
TEST_P_TEMP_DISABLED_ON_WINDOWS(AgentContainerAPITest, NestedContainerLaunch)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  Owned<MasterDetector> detector = master.get()->createDetector();

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.launcher = std::get<1>(std::get<3>(GetParam()));
  slaveFlags.isolation = std::get<0>(std::get<3>(GetParam()));

  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), slaveFlags);
  ASSERT_SOME(slave);

  Try<v1::ContainerID> parentContainerId =
    launchParentContainer(master.get()->pid, slave.get()->pid);

  ASSERT_SOME(parentContainerId);

  // Launch a nested container and wait for it to finish.
  v1::ContainerID containerId;
  containerId.set_value(UUID::random().toString());
  containerId.mutable_parent()->CopyFrom(parentContainerId.get());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      http::OK().status,
      launchNestedContainer(slave.get()->pid, containerId));

  Future<v1::agent::Response> wait =
    deserialize(waitNestedContainer(slave.get()->pid, containerId));

  EXPECT_TRUE(wait.isPending());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      http::OK().status,
      killNestedContainer(slave.get()->pid, containerId));

  AWAIT_READY(wait);
  EXPECT_TRUE(checkWaitContainerResponse(wait, SIGKILL));
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
