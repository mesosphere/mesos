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

#include <google/protobuf/util/json_util.h>

#include <process/gtest.hpp>
#include <process/gmock.hpp>

#include "common/protobuf_utils.hpp"

#include "csi/spec.hpp"

#include "master/detector/standalone.hpp"

#include "resource_provider/manager_process.hpp"

#include "slave/slave.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/fetcher.hpp"

#include "tests/flags.hpp"
#include "tests/mesos.hpp"
#include "tests/mock_slave.hpp"

#include "tests/resource_provider/mock_manager.hpp"

using std::shared_ptr;
using std::string;
using std::vector;

using google::protobuf::util::JsonStringToMessage;

using mesos::internal::slave::Containerizer;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::Slave;

using mesos::master::detector::MasterDetector;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using mesos::slave::ContainerTermination;

using process::Future;
using process::Owned;
using process::PID;
using process::Promise;

using process::post;

using testing::WithArgs;

namespace mesos {
namespace internal {
namespace tests {

class StorageLocalResourceProviderTest : public MesosTest
{
public:
  virtual void SetUp()
  {
    MesosTest::SetUp();

    const string testPluginWorkDir = path::join(sandbox.get(), "test");
    ASSERT_SOME(os::mkdir(testPluginWorkDir));

    resourceProviderConfigDir =
      path::join(sandbox.get(), "resource_provider_configs");

    ASSERT_SOME(os::mkdir(resourceProviderConfigDir));

    string libraryPath = path::join(tests::flags.build_dir, "src", ".libs");
    string testPlugin = path::join(libraryPath, "test-csi-plugin");

    ASSERT_SOME(os::write(
        path::join(resourceProviderConfigDir, "test.json"),
        "{\n"
        "  \"type\": \"org.apache.mesos.rp.local.storage\",\n"
        "  \"name\": \"test\",\n"
        "  \"storage\": {\n"
        "    \"csi_plugins\": [\n"
        "      {\n"
        "        \"name\": \"unified\",\n"
        "        \"command\": {\n"
        "          \"environment\": {\n"
        "            \"variables\": [\n"
        "              {\n"
        "                \"name\": \"LD_LIBRARY_PATH\",\n"
        "                \"value\": \"" + libraryPath + "\"\n"
        "              }\n"
        "            ]\n"
        "          },\n"
        "          \"shell\": false,\n"
        "          \"value\": \"" + testPlugin + "\",\n"
        "          \"arguments\": [\n"
        "            \"" + testPlugin + "\",\n"
        "            \"--total_capacity=4GB\",\n"
        "            \"--work_dir=" + testPluginWorkDir + "\"\n"
        "          ]\n"
        "        }\n"
        "      }\n"
        "    ],\n"
        "    \"controller_plugin\": \"unified\",\n"
        "    \"node_plugin\": \"unified\"\n"
        "  }\n"
        "}\n"));
  }

protected:
  string resourceProviderConfigDir;
};


// This test verifies that a storage local resource provider can
// use the test CSI plugin to create, publish, and destroy a volume.
TEST_F(StorageLocalResourceProviderTest, ROOT_DestroyPublishedVolume)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  Owned<MasterDetector> detector = master.get()->createDetector();

  MockResourceProviderManagerProcess* process =
    new MockResourceProviderManagerProcess();

  MockResourceProviderManager resourceProviderManager(
      (Owned<ResourceProviderManagerProcess>(process)));

  slave::Flags flags = CreateSlaveFlags();
  flags.authenticate_http_readwrite = false;
  flags.authenticate_http_readonly = false;
  flags.isolation = "filesystem/linux";
  flags.resource_provider_config_dir = resourceProviderConfigDir;

  // Capture the SlaveID.
  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Future<Call::UpdateState> stateUpdated;
  EXPECT_CALL(*process, updateState(_, _))
    .WillOnce(FutureArg<1>(&stateUpdated));

  Try<Owned<cluster::Slave>> slave = this->StartSlave(
      detector.get(),
      &resourceProviderManager,
      flags);

  ASSERT_SOME(slave);

  AWAIT_READY(slaveRegisteredMessage);
  const SlaveID& slaveId = slaveRegisteredMessage->slave_id();

  AWAIT_READY(stateUpdated);
  ASSERT_EQ(1, stateUpdated->resources_size());

  const Resource& resource = stateUpdated->resources(0);
  ASSERT_TRUE(resource.has_provider_id());
  ASSERT_TRUE(resource.has_disk());
  ASSERT_TRUE(resource.disk().has_source());
  EXPECT_EQ(Resource::DiskInfo::Source::RAW, resource.disk().source().type());

  ResourceVersionUUID resourceVersionUuid;
  resourceVersionUuid.mutable_resource_provider_id()->CopyFrom(
      resource.provider_id());
  resourceVersionUuid.set_uuid(stateUpdated->resource_version_uuid());

  FrameworkID frameworkId;
  frameworkId.set_value("frameworkId");

  // Create a volume.
  Future<Call::UpdateOfferOperationStatus> volumeCreated;
  EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
    .WillOnce(FutureArg<1>(&volumeCreated));

  {
    ApplyOfferOperationMessage message;
    message.mutable_framework_id()->CopyFrom(frameworkId);
    message.set_operation_uuid(UUID::random().toBytes());
    message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
    message.mutable_operation_info()->set_type(Offer::Operation::CREATE_VOLUME);

    Offer::Operation::CreateVolume* createVolume =
      message.mutable_operation_info()->mutable_create_volume();
    createVolume->mutable_source()->CopyFrom(resource);
    createVolume->set_target_type(Resource::DiskInfo::Source::MOUNT);

    resourceProviderManager.applyOfferOperation(message);
  }

  AWAIT_READY(volumeCreated);
  ASSERT_EQ(1, volumeCreated->status().converted_resources_size());

  const Resource& created = volumeCreated->status().converted_resources(0);
  ASSERT_TRUE(created.has_disk());
  ASSERT_TRUE(created.disk().has_source());
  ASSERT_TRUE(created.disk().source().has_id());
  ASSERT_TRUE(created.disk().source().has_mount());
  ASSERT_TRUE(created.disk().source().mount().has_root());
  EXPECT_FALSE(path::absolute(created.disk().source().mount().root()));

  // Check if the volume is actually created by the test CSI plugin.
  // TODO(chhsiao): Use ID string once we update the CSI spec.
  csi::VolumeID volumeId;
  JsonStringToMessage(created.disk().source().id(), &volumeId);
  ASSERT_TRUE(volumeId.values().count("id"));
  const string& csiVolumePath = volumeId.values().at("id");
  EXPECT_TRUE(os::exists(csiVolumePath));

  // Publish the created volume.
  Future<Nothing> volumePublished;
  EXPECT_CALL(*process, published(_, _))
    .WillOnce(FutureSatisfy(&volumePublished));

  resourceProviderManager.publish(slaveId, created);

  AWAIT_READY(volumePublished);

  // Check if the mount point is created.
  const string& mountPoint = path::join(
      flags.work_dir,
      created.disk().source().mount().root());
  EXPECT_TRUE(os::exists(mountPoint));

  // Check that the mount is propagated.
  ASSERT_SOME(os::touch(path::join(csiVolumePath, "file")));
  EXPECT_TRUE(os::exists(path::join(mountPoint, "file")));

  // Destroy the published volume.
  Future<Call::UpdateOfferOperationStatus> volumeDestroyed;
  EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
    .WillOnce(FutureArg<1>(&volumeDestroyed));

  {
    ApplyOfferOperationMessage message;
    message.mutable_framework_id()->CopyFrom(frameworkId);
    message.set_operation_uuid(UUID::random().toBytes());
    message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
    message.mutable_operation_info()->set_type(
        Offer::Operation::DESTROY_VOLUME);
    message.mutable_operation_info()->mutable_destroy_volume()->mutable_volume()
      ->CopyFrom(created);

    resourceProviderManager.applyOfferOperation(message);
  }

  AWAIT_READY(volumeDestroyed);
  ASSERT_EQ(1, volumeDestroyed->status().converted_resources_size());

  const Resource& destroyed = volumeDestroyed->status().converted_resources(0);
  ASSERT_TRUE(destroyed.has_disk());
  ASSERT_TRUE(destroyed.disk().has_source());
  EXPECT_EQ(Resource::DiskInfo::Source::RAW, destroyed.disk().source().type());

  // Check if the mount point is removed.
  EXPECT_FALSE(os::exists(mountPoint));

  // Check if the volume is actually deleted by the test CSI plugin.
  EXPECT_FALSE(os::exists(csiVolumePath));
}


// This test verifies that a the agent asks the storage local resource
// provider to publish all allocated resources before launching tasks.
TEST_F(StorageLocalResourceProviderTest, ROOT_PublishAllVolumes)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.authenticate_http_readwrite = false;
  flags.authenticate_http_readonly = false;
  flags.isolation = "filesystem/linux";
  flags.resource_provider_config_dir = resourceProviderConfigDir;

  Owned<MasterDetector> detector = master.get()->createDetector();

  Fetcher fetcher(flags);
  Try<Containerizer*> _containerizer =
    Containerizer::create(flags, true, &fetcher);
  ASSERT_SOME(_containerizer);
  Owned<Containerizer> containerizer(_containerizer.get());

  MockResourceProviderManagerProcess* process =
    new MockResourceProviderManagerProcess();
  MockResourceProviderManager resourceProviderManager(
      (Owned<ResourceProviderManagerProcess>(process)));

  MockSlave slave(
      flags,
      detector.get(),
      containerizer.get(),
      None(),
      None(),
      None(),
      &resourceProviderManager);

  // Capture the SlaveID.
  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Future<Call::UpdateState> stateUpdated;
  EXPECT_CALL(*process, updateState(_, _))
    .WillOnce(FutureArg<1>(&stateUpdated));

  PID<Slave> slavePid = spawn(slave);

  AWAIT_READY(slaveRegisteredMessage);
  const SlaveID& slaveId = slaveRegisteredMessage->slave_id();

  AWAIT_READY(stateUpdated);
  ASSERT_EQ(1, stateUpdated->resources_size());

  const Resource& resource = stateUpdated->resources(0);
  ASSERT_TRUE(resource.has_scalar());

  ResourceVersionUUID resourceVersionUuid;
  resourceVersionUuid.mutable_resource_provider_id()->CopyFrom(
      resource.provider_id());

  FrameworkID frameworkId;
  frameworkId.set_value("storage");

  vector<Resource> volumes;

  // Create two persistent volumes and put files into the volumes.
  for (int i = 0; i < 2; i++) {
    // Create a CSI volume.
    Future<Call::UpdateOfferOperationStatus> volumeCreated;
    EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
      .WillOnce(FutureArg<1>(&volumeCreated));

    {
      ApplyOfferOperationMessage message;
      message.mutable_framework_id()->CopyFrom(frameworkId);
      message.set_operation_uuid(UUID::random().toBytes());
      message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
      message.mutable_operation_info()->set_type(
          Offer::Operation::CREATE_VOLUME);

      Offer::Operation::CreateVolume* createVolume =
        message.mutable_operation_info()->mutable_create_volume();
      createVolume->mutable_source()->CopyFrom(resource);
      createVolume->mutable_source()->mutable_scalar()->set_value(
          resource.scalar().value() / 2);
      createVolume->set_target_type(Resource::DiskInfo::Source::MOUNT);

      resourceProviderManager.applyOfferOperation(message);
    }

    AWAIT_READY(volumeCreated);
    ASSERT_EQ(1, volumeCreated->status().converted_resources_size());

    Resource volume = volumeCreated->status().converted_resources(0);
    ASSERT_TRUE(volume.has_disk());
    ASSERT_TRUE(volume.disk().has_source());
    ASSERT_TRUE(volume.disk().source().has_id());

    // Put a file into the volume.
    // TODO(chhsiao): Use ID string once we update the CSI spec.
    csi::VolumeID volumeId;
    JsonStringToMessage(volume.disk().source().id(), &volumeId);
    ASSERT_TRUE(volumeId.values().count("id"));
    const string& csiVolumePath = volumeId.values().at("id");
    ASSERT_SOME(os::touch(path::join(csiVolumePath, "file")));

    // Reserve the CSI volume.
    volume.add_reservations()->CopyFrom(createDynamicReservationInfo("test"));

    Future<Call::UpdateOfferOperationStatus> reserved;
    EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
      .WillOnce(FutureArg<1>(&reserved));

    {
      ApplyOfferOperationMessage message;
      message.mutable_framework_id()->CopyFrom(frameworkId);
      message.set_operation_uuid(UUID::random().toBytes());
      message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
      message.mutable_operation_info()->set_type(Offer::Operation::RESERVE);
      message.mutable_operation_info()->mutable_reserve()->add_resources()
        ->CopyFrom(volume);

      resourceProviderManager.applyOfferOperation(message);
    }

    AWAIT_READY(reserved);
    ASSERT_EQ(1, reserved->status().converted_resources_size());
    EXPECT_EQ(
        Resources(volume),
        Resources(reserved->status().converted_resources(0)));

    // Create a persistent volume using the CSI volume.
    Resource::DiskInfo* disk = volume.mutable_disk();
    disk->mutable_persistence()->set_id(UUID::random().toString());
    disk->mutable_volume()->set_container_path("volume");
    disk->mutable_volume()->set_mode(Volume::RW);

    Future<Call::UpdateOfferOperationStatus> created;
    EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
      .WillOnce(FutureArg<1>(&created));

    {
      ApplyOfferOperationMessage message;
      message.mutable_framework_id()->CopyFrom(frameworkId);
      message.set_operation_uuid(UUID::random().toBytes());
      message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
      message.mutable_operation_info()->set_type(Offer::Operation::CREATE);
      message.mutable_operation_info()->mutable_create()->add_volumes()
        ->CopyFrom(volume);

      resourceProviderManager.applyOfferOperation(message);
    }

    AWAIT_READY(created);
    ASSERT_EQ(1, created->status().converted_resources_size());
    EXPECT_EQ(
        Resources(volume),
        Resource(created->status().converted_resources(0)));

    volumes.emplace_back(volume);
  }

  DROP_PROTOBUFS(StatusUpdate(), _, master.get()->pid);

  shared_ptr<hashmap<TaskID, Promise<TaskState>>> promises(
      new hashmap<TaskID, Promise<TaskState>>());
  vector<Future<TaskState>> taskStates;

  EXPECT_CALL(slave, statusUpdate(_, _))
    .WillRepeatedly(DoAll(
        WithArgs<0>(Invoke([=](const StatusUpdate& update) {
          if (promises->count(update.status().task_id()) &&
              protobuf::isTerminalState(update.status().state())) {
            promises->at(update.status().task_id()).set(
                update.status().state());
          }
        })),
        Invoke(&slave, &MockSlave::unmocked_statusUpdate)));

  // We would like to run two tasks that use different volumes, and
  // check if the 'PUBLISH' request for launching the second task
  // contains both volumes while the first task is still running.
  // Therefore, we use a pipe to synchronize between the two tasks.
  Try<std::array<int_fd, 2>> pipes_ = os::pipe();
  ASSERT_SOME(pipes_);
  const std::array<int_fd, 2>& pipes = pipes_.get();

  // Launch the first task that waits for the second task.
  {
    Future<Resources> publish;
    EXPECT_CALL(*process, publish(_, _))
      .WillOnce(DoAll(
          FutureArg<1>(&publish),
          Invoke(process, &MockResourceProviderManagerProcess::_publish)))
      .RetiresOnSaturation();

    RunTaskMessage runTaskMessage;
    runTaskMessage.mutable_framework()->CopyFrom(DEFAULT_FRAMEWORK_INFO);
    runTaskMessage.mutable_framework()->mutable_id()->set_value("framework0");

    // NOTE: We use a non-shell command here to use 'bash -c' to execute
    // the 'read', which deals with the file descriptor, because of a bug
    // in ubuntu dash. Multi-digit file descriptor is not supported in
    // ubuntu dash, which executes the shell command.
    runTaskMessage.mutable_task()->CopyFrom(
        createTask(slaveId, volumes[0], createCommandInfo(
            "/bin/bash",
            {"bash",
             "-c",
             "read <&" + stringify(pipes[0]) + ";"
             "test -f " + path::join("volume", "file")})));

    taskStates.push_back((*promises)[runTaskMessage.task().task_id()].future());
    post(master.get()->pid, slavePid, runTaskMessage);

    AWAIT_READY(publish);

    // Check if the used volume is being published.
    Resources resources = publish.get();
    resources.unallocate();
    EXPECT_TRUE(resources.contains(volumes[0]));
  }

  // Launch the second task.
  {
    Future<Resources> publish;
    EXPECT_CALL(*process, publish(_, _))
      .WillOnce(DoAll(
          FutureArg<1>(&publish),
          Invoke(process, &MockResourceProviderManagerProcess::_publish)))
      .RetiresOnSaturation();

    RunTaskMessage runTaskMessage;
    runTaskMessage.mutable_framework()->CopyFrom(DEFAULT_FRAMEWORK_INFO);
    runTaskMessage.mutable_framework()->mutable_id()->set_value("framework1");

    // NOTE: We use a non-shell command here to use 'bash -c' to execute
    // the 'read', which deals with the file descriptor, because of a bug
    // in ubuntu dash. Multi-digit file descriptor is not supported in
    // ubuntu dash, which executes the shell command.
    runTaskMessage.mutable_task()->CopyFrom(
        createTask(slaveId, volumes[1], createCommandInfo(
            "/bin/bash",
            {"bash",
             "-c",
             "echo >&" + stringify(pipes[1]) + ";"
             "test -f " + path::join("volume", "file")})));

    taskStates.push_back((*promises)[runTaskMessage.task().task_id()].future());
    post(master.get()->pid, slavePid, runTaskMessage);

    AWAIT_READY(publish);

    // Check if both used volumes are being published.
    Resources resources = publish.get();
    resources.unallocate();
    EXPECT_TRUE(resources.contains(volumes));
  }

  AWAIT_EXPECT_EQ(TASK_FINISHED, taskStates[0]);
  AWAIT_EXPECT_EQ(TASK_FINISHED, taskStates[1]);

  // Clean up the volumes so we can remove the sandbox.
  for (int i = 0; i < 2; i++) {
    Future<Call::UpdateOfferOperationStatus> volumeDestroyed;
    EXPECT_CALL(*process, updateOfferOperationStatus(_, _))
      .WillOnce(FutureArg<1>(&volumeDestroyed));

    ApplyOfferOperationMessage message;
    message.mutable_framework_id()->CopyFrom(frameworkId);
    message.set_operation_uuid(UUID::random().toBytes());
    message.mutable_resource_version_uuid()->CopyFrom(resourceVersionUuid);
    message.mutable_operation_info()->set_type(
        Offer::Operation::DESTROY_VOLUME);
    message.mutable_operation_info()->mutable_destroy_volume()->mutable_volume()
      ->CopyFrom(volumes[i]);

    resourceProviderManager.applyOfferOperation(message);

    AWAIT_READY(volumeDestroyed);
  }

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);

  foreach (const ContainerID& containerId, containers.get()) {
    Future<Option<ContainerTermination>> wait =
      containerizer->wait(containerId);
    containerizer->destroy(containerId);
    AWAIT_READY(wait);
  }

  terminate(slave);
  wait(slave);
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
