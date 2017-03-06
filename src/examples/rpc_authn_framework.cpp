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

#include <glog/logging.h>

#include <string>
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

#include <process/clock.hpp>
#include <process/defer.hpp>
#include <process/http.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/time.hpp>

#include <process/metrics/counter.hpp>
#include <process/metrics/gauge.hpp>
#include <process/metrics/metrics.hpp>

#include <stout/bytes.hpp>
#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>


using namespace mesos;

using std::string;

using process::Clock;
using process::defer;

using process::metrics::Gauge;
using process::metrics::Counter;


const double CPUS_PER_TASK = 0.1;
const int MEMORY_PER_TASK = 16;
const Bytes DISK_PER_TASK = Megabytes(5);


class Flags : public virtual flags::FlagsBase
{
public:
  Flags()
  {
    add(&Flags::master,
        "master",
        "Master to connect to.");

    add(&Flags::run_once,
        "run_once",
        "Whether this framework should keep running tasks forever.\n"
        "By default the framework will exit after running a single task.\n",
        true);

    add(&Flags::sleep_duration,
        "sleep_duration",
        "The task will terminate after sleeping for specified duration.\n",
        Seconds(10));
  }

  string master;
  bool run_once;
  Duration sleep_duration;
};

// Actor holding the business logic and metrics for the `RPCAuthnScheduler`.
// See `RPCAuthnScheduler` below for intended behavior.
class RPCAuthnSchedulerProcess
  : public process::Process<RPCAuthnSchedulerProcess>
{
public:
  RPCAuthnSchedulerProcess (
      const Flags& _flags,
      const FrameworkInfo& _frameworkInfo)
    : flags(_flags),
      frameworkInfo(_frameworkInfo),
      tasksLaunched(0),
      taskActive(false),
      isRegistered(false),
      metrics(*this)
  {
    start_time = Clock::now();
  }

  void registered()
  {
    isRegistered = true;
  }

  void disconnected()
  {
    isRegistered = false;
  }

  void resourceOffers(
      SchedulerDriver* driver,
      const std::vector<Offer>& offers)
  {
    Resources taskResources = Resources::parse(
        "cpus:" + stringify(CPUS_PER_TASK) +
        ";mem:" + stringify(MEMORY_PER_TASK) +
        ";disk:" + stringify(DISK_PER_TASK.megabytes())).get();
    taskResources.allocate(frameworkInfo.role());

    foreach (const Offer& offer, offers) {
      LOG(INFO) << "Received offer " << offer.id() << " from agent "
                << offer.slave_id() << " (" << offer.hostname() << ") "
                << "with " << offer.resources();

      Resources resources(offer.resources());

      // If we've already launched the task, or if the offer is not
      // big enough, reject the offer.
      if (taskActive || !resources.toUnreserved().contains(taskResources)) {
        Filters filters;
        filters.set_refuse_seconds(600);

        driver->declineOffer(offer.id(), filters);
        continue;
      }

      int taskId = tasksLaunched++;

      // The task sleeps for the amount of seconds specified by the
      // sleep_duration flag.
      static const string command =
          "sleep " + stringify(flags.sleep_duration.secs());

      TaskInfo task;
      task.set_name("Soak test framework task");
      task.mutable_task_id()->set_value(stringify(taskId));
      task.mutable_slave_id()->MergeFrom(offer.slave_id());
      task.mutable_resources()->CopyFrom(taskResources);
      task.mutable_command()->set_shell(true);
      task.mutable_command()->set_value(command);

      LOG(INFO) << "Starting task " << taskId;

      driver->launchTasks(offer.id(), {task});

      taskActive = true;
    }
  }

  void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    if (stringify(tasksLaunched - 1) != status.task_id().value()) {
      // We might receive messages from older tasks. Ignore them.
      LOG(INFO) << "Ignoring status update from older task "
                << status.task_id();
      return;
    }

    switch (status.state()) {
    case TASK_FINISHED:
      if (flags.run_once) {
          driver->stop();
          break;
      }

      taskActive = false;
      ++metrics.tasks_finished;
      break;
    case TASK_FAILED:
      if (flags.run_once) {
          driver->abort();
          break;
      }

      taskActive = false;
      ++metrics.abnormal_terminations;
      break;
    case TASK_KILLED:
    case TASK_LOST:
    case TASK_ERROR:
    case TASK_DROPPED:
    case TASK_UNREACHABLE:
    case TASK_GONE:
    case TASK_GONE_BY_OPERATOR:
      if (flags.run_once) {
        driver->abort();
      }

      taskActive = false;
      ++metrics.abnormal_terminations;
      break;
    case TASK_STARTING:
    case TASK_RUNNING:
    case TASK_STAGING:
    case TASK_KILLING:
    case TASK_UNKNOWN:
      break;
    }
  }

private:
  const Flags flags;
  const FrameworkInfo frameworkInfo;
  int tasksLaunched;
  bool taskActive;

  process::Time start_time;

  double _uptime_secs()
  {
    return (Clock::now() - start_time).secs();
  }

  bool isRegistered;
  double _registered()
  {
    return isRegistered ? 1 : 0;
  }

  struct Metrics
  {
    Metrics(const RPCAuthnSchedulerProcess& _scheduler)
      : uptime_secs(
            "rpc_authn_framework/uptime_secs",
            defer(_scheduler, &RPCAuthnSchedulerProcess::_uptime_secs)),
        registered(
            "rpc_authn_framework/registered",
            defer(_scheduler, &RPCAuthnSchedulerProcess::_registered)),
        tasks_finished(
            "rpc_authn_framework/tasks_finished"),
        abnormal_terminations(
            "rpc_authn_framework/abnormal_terminations")
    {
      process::metrics::add(uptime_secs);
      process::metrics::add(registered);
      process::metrics::add(tasks_finished);
      process::metrics::add(abnormal_terminations);
    }

    ~Metrics()
    {
      process::metrics::remove(uptime_secs);
      process::metrics::remove(registered);
      process::metrics::remove(tasks_finished);
      process::metrics::remove(abnormal_terminations);
    }

    process::metrics::Gauge uptime_secs;
    process::metrics::Gauge registered;

    process::metrics::Counter tasks_finished;
    process::metrics::Counter abnormal_terminations;
  } metrics;
};


// This scheduler starts a simple sleep task.
class RPCAuthnScheduler : public Scheduler
{
public:
  RPCAuthnScheduler(const Flags& _flags, const FrameworkInfo& _frameworkInfo)
    : process(_flags, _frameworkInfo)
  {
    process::spawn(process);
  }

  virtual ~RPCAuthnScheduler()
  {
    process::terminate(process);
    process::wait(process);
  }

  virtual void registered(
      SchedulerDriver*,
      const FrameworkID& frameworkId,
      const MasterInfo&)
  {
    LOG(INFO) << "Registered with framework ID: " << frameworkId;

    process::dispatch(&process, &RPCAuthnSchedulerProcess::registered);
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo&)
  {
    LOG(INFO) << "Reregistered";

    process::dispatch(&process, &RPCAuthnSchedulerProcess::registered);
  }

  virtual void disconnected(SchedulerDriver*)
  {
    LOG(INFO) << "Disconnected";

    process::dispatch(
        &process,
        &RPCAuthnSchedulerProcess::disconnected);
  }

  virtual void resourceOffers(
      SchedulerDriver* driver,
      const std::vector<Offer>& offers)
  {
    LOG(INFO) << "Resource offers received";

    process::dispatch(
         &process,
         &RPCAuthnSchedulerProcess::resourceOffers,
         driver,
         offers);
  }

  virtual void offerRescinded(SchedulerDriver*, const OfferID&)
  {
    LOG(INFO) << "Offer rescinded";
  }

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    LOG(INFO) << "Task " << status.task_id() << " in state "
              << TaskState_Name(status.state())
              << ", Source: " << status.source()
              << ", Reason: " << status.reason()
              << (status.has_message() ? ", Message: " + status.message() : "");

    process::dispatch(
        &process,
        &RPCAuthnSchedulerProcess::statusUpdate,
        driver,
        status);
  }

  virtual void frameworkMessage(
      SchedulerDriver*,
      const ExecutorID&,
      const SlaveID&,
      const string& data)
  {
    LOG(INFO) << "Framework message: " << data;
  }

  virtual void slaveLost(SchedulerDriver*, const SlaveID& slaveId)
  {
    LOG(INFO) << "Agent lost: " << slaveId;
  }

  virtual void executorLost(
      SchedulerDriver*,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      int)
  {
    LOG(INFO) << "Executor '" << executorId << "' lost on agent: " << slaveId;
  }

  virtual void error(SchedulerDriver*, const string& message)
  {
    LOG(INFO) << "Error message: " << message;
  }

private:
  RPCAuthnSchedulerProcess process;
};


int main(int argc, char** argv)
{
  Flags flags;
  Try<flags::Warnings> load = flags.load("MESOS_", argc, argv);

  if (load.isError()) {
    EXIT(EXIT_FAILURE) << flags.usage(load.error());
  }

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill the current user.
  framework.set_name("RPC Authentication Framework (C++)");
  framework.set_checkpoint(true);
  framework.add_capabilities()->set_type(
      FrameworkInfo::Capability::RESERVATION_REFINEMENT);

  RPCAuthnScheduler scheduler(flags, framework);

  MesosSchedulerDriver* driver;

  // TODO(hartem): Refactor these into a common set of flags.
  Option<string> value = os::getenv("MESOS_AUTHENTICATE_FRAMEWORKS");
  if (value.isSome()) {
    LOG(INFO) << "Enabling authentication for the framework";

    value = os::getenv("DEFAULT_PRINCIPAL");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting authentication principal in the environment";
    }

    Credential credential;
    credential.set_principal(value.get());

    framework.set_principal(value.get());

    value = os::getenv("DEFAULT_SECRET");
    if (value.isSome()) {
      credential.set_secret(value.get());
    }

    driver = new MesosSchedulerDriver(
        &scheduler, framework, flags.master, credential);
  } else {
    framework.set_principal("rpc-authn-framework-cpp");

    driver = new MesosSchedulerDriver(
        &scheduler, framework, flags.master);
  }

  int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

  // Ensure that the driver process terminates.
  driver->stop();

  delete driver;
  return status;
}
