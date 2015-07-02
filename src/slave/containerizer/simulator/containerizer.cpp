#include <process/process.hpp>
#include <process/future.hpp>

#include "slave/slave.hpp"
#include "slave/state.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/simulator/containerizer.hpp"

using std::string;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

using state::SlaveState;
using state::FrameworkState;
using state::ExecutorState;
using state::RunState;

Try<SimulatorContainerizer*> SimulatorContainerizer::create()
{
  return new SimulatorContainerizer();
}


SimulatorContainerizer::SimulatorContainerizer()
{
  process = new SimulatorContainerizerProcess();
  spawn(process);
}


SimulatorContainerizer::~SimulatorContainerizer()
{
  terminate(process);
  process::wait(process);
  delete process;
}


Future<Nothing> SimulatorContainerizer::recover(
    const Option<state::SlaveState>& state)
{
  return dispatch(process, &SimulatorContainerizerProcess::recover, state);
}


Future<bool> SimulatorContainerizer::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &SimulatorContainerizerProcess::launch,
                  containerId,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<bool> SimulatorContainerizer::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &SimulatorContainerizerProcess::launch,
                  containerId,
                  taskInfo,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> SimulatorContainerizer::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return dispatch(process,
                  &SimulatorContainerizerProcess::update,
                  containerId,
                  resources);
}


Future<ResourceStatistics> SimulatorContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(process, &SimulatorContainerizerProcess::usage, containerId);
}


Future<containerizer::Termination> SimulatorContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(process, &SimulatorContainerizerProcess::wait, containerId);
}


void SimulatorContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(process, &SimulatorContainerizerProcess::destroy, containerId);
}


Future<hashset<ContainerID>> SimulatorContainerizer::containers()
{
  return dispatch(process, &SimulatorContainerizerProcess::containers);
}


SimulatorContainerizerProcess::~SimulatorContainerizerProcess()
{
}


Future<Nothing> SimulatorContainerizerProcess::recover(
    const Option<state::SlaveState>& state)
{
  // Recover each containerizer in parallel.
  list<Future<Nothing>> futures;
  foreach (Containerizer* containerizer, containerizers_) {
    futures.push_back(containerizer->recover(state));
  }

  return collect(futures)
    .then(defer(self(), &Self::_recover));
}


Future<Nothing> SimulatorContainerizerProcess::_recover()
{
  // Now collect all the running containers in order to multiplex.
  list<Future<Nothing>> futures;
  foreach (Containerizer* containerizer, containerizers_) {
    Future<Nothing> future = containerizer->containers()
      .then(defer(self(), &Self::__recover, containerizer, lambda::_1));
    futures.push_back(future);
  }

  return collect(futures)
    .then(lambda::bind(&Self::___recover));
}


Future<Nothing> SimulatorContainerizerProcess::__recover(
    Containerizer* containerizer,
    const hashset<ContainerID>& containers)
{
  foreach (const ContainerID& containerId, containers) {
    Container* container = new Container();
    container->state = LAUNCHED;
    container->containerizer = containerizer;
    containers_[containerId] = container;
  }
  return Nothing();
}


Future<Nothing> SimulatorContainerizerProcess::___recover()
{
  return Nothing();
}


Future<bool> SimulatorContainerizerProcess::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  if (containers_.contains(containerId)) {
    return Failure("Container '" + containerId.value() +
                   "' is already launching");
  }

  // Try each containerizer. If none of them handle the
  // TaskInfo/ExecutorInfo then return a Failure.
  vector<Containerizer*>::iterator containerizer = containerizers_.begin();

  Container* container = new Container();
  container->state = LAUNCHING;
  container->containerizer = *containerizer;
  containers_[containerId] = container;

  return (*containerizer)->launch(
      containerId,
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint)
    .then(defer(self(),
                &Self::_launch,
                containerId,
                None(),
                executorInfo,
                directory,
                user,
                slaveId,
                slavePid,
                checkpoint,
                containerizer,
                lambda::_1));
}


Future<bool> SimulatorContainerizerProcess::_launch(
    const ContainerID& containerId,
    const Option<TaskInfo>& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint,
    vector<Containerizer*>::iterator containerizer,
    bool launched)
{
  // The container struct won't be cleaned up by destroy because
  // in destroy we only forward the destroy, and wait until the
  // launch returns and clean up here.
  CHECK(containers_.contains(containerId));
  Container* container = containers_[containerId];
  if (container->state == DESTROYED) {
    containers_.erase(containerId);
    delete container;
    return Failure("Container was destroyed while launching");
  }

  if (launched) {
    container->state = LAUNCHED;
    return true;
  }

  // Try the next containerizer.
  ++containerizer;

  if (containerizer == containerizers_.end()) {
    containers_.erase(containerId);
    delete container;
    return false;
  }

  container->containerizer = *containerizer;

  Future<bool> f = taskInfo.isSome() ?
      (*containerizer)->launch(
          containerId,
          taskInfo.get(),
          executorInfo,
          directory,
          user,
          slaveId,
          slavePid,
          checkpoint) :
      (*containerizer)->launch(
          containerId,
          executorInfo,
          directory,
          user,
          slaveId,
          slavePid,
          checkpoint);

  return f.then(
      defer(self(),
            &Self::_launch,
            containerId,
            taskInfo,
            executorInfo,
            directory,
            user,
            slaveId,
            slavePid,
            checkpoint,
            containerizer,
            lambda::_1));
}


Future<bool> SimulatorContainerizerProcess::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  if (containers_.contains(containerId)) {
    return Failure("Container '" + stringify(containerId) +
                   "' is already launching");
  }

  // Try each containerizer. If none of them handle the
  // TaskInfo/ExecutorInfo then return a Failure.
  vector<Containerizer*>::iterator containerizer = containerizers_.begin();

  Container* container = new Container();
  container->state = LAUNCHING;
  container->containerizer = *containerizer;
  containers_[containerId] = container;

  return (*containerizer)->launch(
      containerId,
      taskInfo,
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint)
    .then(defer(self(),
                &Self::_launch,
                containerId,
                taskInfo,
                executorInfo,
                directory,
                user,
                slaveId,
                slavePid,
                checkpoint,
                containerizer,
                lambda::_1));
}


Future<Nothing> SimulatorContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not found");
  }

  return containers_[containerId]->containerizer->update(
      containerId, resources);
}


Future<ResourceStatistics> SimulatorContainerizerProcess::usage(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not found");
  }

  return containers_[containerId]->containerizer->usage(containerId);
}


Future<containerizer::Termination> SimulatorContainerizerProcess::wait(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not found");
  }

  return containers_[containerId]->containerizer->wait(containerId);
}


void SimulatorContainerizerProcess::destroy(const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    LOG(WARNING) << "Container '" << containerId.value() << "' not found";
    return;
  }

  Container* container = containers_[containerId];

  if (container->state == DESTROYED) {
    LOG(WARNING) << "Container '" << containerId.value()
                 << "' is already destroyed";
    return;
  }

  // It's ok to forward destroy to any containerizer which is currently
  // launching the container, because we expect each containerizer to
  // handle calling destroy on non-existing container.
  // The composing containerizer will not move to the next
  // containerizer for a container that is destroyed as well.
  container->containerizer->destroy(containerId);

  if (container->state == LAUNCHING) {
    // Record the fact that this container was asked to be destroyed
    // so that we won't try and launch this container using any other
    // containerizers in the event the current containerizer has
    // decided it can't launch the container.
    container->state = DESTROYED;
    return;
  }

  // If the container is launched, then we can simply cleanup.
  containers_.erase(containerId);
  delete container;
}


Future<hashset<ContainerID>> SimulatorContainerizerProcess::containers()
{
  return containers_.keys();
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
