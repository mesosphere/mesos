/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __SIMULATOR_CONTAINERIZER_HPP__
#define __SIMULATOR_CONTAINERIZER_HPP__

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>
#include <mesos/containerizer/containerizer.hpp>

#include <process/future.hpp>

#include <stout/try.hpp>

namespace mesos
{
namespace internal
{
namespace slave
{
class SimulatorContainerizerProcess;

class SimulatorContainerizer : public Containerizer
{
public:
  static Try<SimulatorContainerizer*> create();

  SimulatorContainerizer();

  virtual ~SimulatorContainerizer();

  virtual process::Future<Nothing> recover(
    const Option<state::SlaveState>& state);

  virtual process::Future<bool> launch(const ContainerID& containerId,
                                       const ExecutorInfo& executorInfo,
                                       const std::string& directory,
                                       const Option<std::string>& user,
                                       const SlaveID& slaveId,
                                       const process::PID<Slave>& slavePid,
                                       bool checkpoint);

  virtual process::Future<bool> launch(const ContainerID& containerId,
                                       const TaskInfo& taskInfo,
                                       const ExecutorInfo& executorInfo,
                                       const std::string& directory,
                                       const Option<std::string>& user,
                                       const SlaveID& slaveId,
                                       const process::PID<Slave>& slavePid,
                                       bool checkpoint);

  virtual process::Future<Nothing> update(const ContainerID& containerId,
                                          const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
    const ContainerID& containerId);

  virtual process::Future<containerizer::Termination> wait(
    const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();

private:
  process::Owned<SimulatorContainerizerProcess> process;
};

class SimulatorContainerizerProcess
  : public Process<SimulatorContainerizerProcess>
{
public:
  SimulatorContainerizerProcess() {}

  virtual ~SimulatorContainerizerProcess();

  Future<Nothing> recover(const Option<state::SlaveState>& state);

  Future<bool> launch(const ContainerID& containerId,
                      const ExecutorInfo& executorInfo,
                      const string& directory,
                      const Option<string>& user,
                      const SlaveID& slaveId,
                      const PID<Slave>& slavePid,
                      bool checkpoint);

  Future<bool> launch(const ContainerID& containerId,
                      const TaskInfo& taskInfo,
                      const ExecutorInfo& executorInfo,
                      const string& directory,
                      const Option<string>& user,
                      const SlaveID& slaveId,
                      const PID<Slave>& slavePid,
                      bool checkpoint);

  Future<Nothing> update(const ContainerID& containerId,
                         const Resources& resources);

  Future<ResourceStatistics> usage(const ContainerID& containerId);

  Future<containerizer::Termination> wait(const ContainerID& containerId);

  void destroy(const ContainerID& containerId);

  Future<hashset<ContainerID>> containers();
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __SIMULATOR_CONTAINERIZER_HPP__
