
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

#include "tests/resource_provider/mock_manager.hpp"

using ::testing::_;
using ::testing::Invoke;

using mesos::resource_provider::Call;

namespace mesos {
namespace internal {
namespace tests {

MockResourceProviderManagerProcess::MockResourceProviderManagerProcess()
{
  EXPECT_CALL(*this, applyOfferOperation(_))
    .WillRepeatedly(Invoke(
        this, &MockResourceProviderManagerProcess::_applyOfferOperation));

  EXPECT_CALL(*this, subscribe(_, _))
    .WillRepeatedly(Invoke(
        this, &MockResourceProviderManagerProcess::_subscribe));

  EXPECT_CALL(*this, updateOfferOperationStatus(_, _))
    .WillRepeatedly(Invoke(
        this,
        &MockResourceProviderManagerProcess::_updateOfferOperationStatus));

  EXPECT_CALL(*this, updateState(_, _))
    .WillRepeatedly(Invoke(
        this, &MockResourceProviderManagerProcess::_updateState));
}


MockResourceProviderManagerProcess::~MockResourceProviderManagerProcess()
{
}


void MockResourceProviderManagerProcess::_applyOfferOperation(
    const ApplyOfferOperationMessage& message)
{
  ResourceProviderManagerProcess::applyOfferOperation(message);
}


void MockResourceProviderManagerProcess::_subscribe(
    const HttpConnection& http,
    const Call::Subscribe& subscribe)
{
  ResourceProviderManagerProcess::subscribe(http, subscribe);
}


void MockResourceProviderManagerProcess::_updateOfferOperationStatus(
    ResourceProvider* resourceProvider,
    const Call::UpdateOfferOperationStatus& update)
{
  ResourceProviderManagerProcess::updateOfferOperationStatus(
      resourceProvider,
      update);
}


void MockResourceProviderManagerProcess::_updateState(
    ResourceProvider* resourceProvider,
    const Call::UpdateState& update)
{
  ResourceProviderManagerProcess::updateState(resourceProvider, update);
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
