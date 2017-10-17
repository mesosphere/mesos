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

#include "tests/mock_csi_plugin.hpp"

using std::string;
using std::unique_ptr;

using csi::Controller;
using csi::Identity;
using csi::Node;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using testing::_;
using testing::DoAll;
using testing::Invoke;
using testing::Return;

namespace mesos {
namespace internal {
namespace tests {

MockCSIPlugin::MockCSIPlugin()
{
#define DECLARE_MOCK_CSI_METHOD_IMPL(name)      \
  EXPECT_CALL(*this, name(_, _, _))             \
    .WillRepeatedly(DoAll(                      \
        Invoke([](                              \
            ServerContext*,                     \
            const csi::name##Request*,          \
            csi::name##Response* response) {    \
          response->mutable_result();           \
        }),                                     \
        Return(Status::OK)));

  CSI_METHOD_FOREACH(DECLARE_MOCK_CSI_METHOD_IMPL)

#undef DECLARE_MOCK_CSI_METHOD_IMPL
}


Try<Nothing> MockCSIPlugin::Startup(const string& address)
{
  ServerBuilder builder;
  builder.AddListeningPort(address, InsecureServerCredentials());
  builder.RegisterService(static_cast<Identity::Service*>(this));
  builder.RegisterService(static_cast<Controller::Service*>(this));
  builder.RegisterService(static_cast<Node::Service*>(this));

  server = builder.BuildAndStart();
  if (!server) {
    return Error("Unable to start a mock CSI plugin.");
  }

  return Nothing();
}


Try<Nothing> MockCSIPlugin::Shutdown()
{
  server->Shutdown();
  server->Wait();

  return Nothing();
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
