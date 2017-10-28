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

#ifndef __TESTS_RESOURCE_PROVIDER_MOCK_MANAGER_HPP__
#define __TESTS_RESOURCE_PROVIDER_MOCK_MANAGER_HPP__

#include <gmock/gmock.h>

#include "resource_provider/manager.hpp"
#include "resource_provider/manager_process.hpp"

namespace mesos {
namespace internal {
namespace tests {

// Definition of a mock resource provider manager process to be used in
// tests with gmock.
class MockResourceProviderManagerProcess
  : public ResourceProviderManagerProcess
{
public:
  MockResourceProviderManagerProcess();
  virtual ~MockResourceProviderManagerProcess();

  MOCK_METHOD1(applyOfferOperation, void(
      const ApplyOfferOperationMessage& message));

  MOCK_METHOD2(publish, process::Future<Nothing>(
      const SlaveID&,
      const Resources&));

  MOCK_METHOD2(subscribe, void(
      const HttpConnection&,
      const mesos::resource_provider::Call::Subscribe&));

  MOCK_METHOD2(updateOfferOperationStatus, void(
      ResourceProvider*,
      const mesos::resource_provider::Call::UpdateOfferOperationStatus&));

  MOCK_METHOD2(updateState, void(
      ResourceProvider*,
      const mesos::resource_provider::Call::UpdateState&));

  MOCK_METHOD2(published, void(
      ResourceProvider*,
      const mesos::resource_provider::Call::Published&));

  void _applyOfferOperation(const ApplyOfferOperationMessage& message);

  process::Future<Nothing> _publish(
      const SlaveID& slaveId,
      const Resources& resources);

  void _subscribe(
      const HttpConnection& http,
      const mesos::resource_provider::Call::Subscribe& subscribe);

  void _updateOfferOperationStatus(
      ResourceProvider* resourceProvider,
      const mesos::resource_provider::Call::UpdateOfferOperationStatus& update);

  void _updateState(
      ResourceProvider* resourceProvider,
      const mesos::resource_provider::Call::UpdateState& update);

  void _published(
      ResourceProvider* resourceProvider,
      const mesos::resource_provider::Call::Published& published);
};


// Definition of a mock resource provider manager to be used in tests
// with gmock.
class MockResourceProviderManager : public ResourceProviderManager
{
public:
  MockResourceProviderManager(
      const process::Owned<ResourceProviderManagerProcess>& process)
    : ResourceProviderManager(process) {}
  virtual ~MockResourceProviderManager() {}
};

} // namespace tests {
} // namespace internal {
} // namespace mesos {

#endif // __TESTS_MOCK_RESOURCE_PROVIDER_MANAGER_HPP__
