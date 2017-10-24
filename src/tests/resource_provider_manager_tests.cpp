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

#include <gtest/gtest.h>

#include <mesos/http.hpp>
#include <mesos/resources.hpp>

#include <mesos/state/in_memory.hpp>
#include <mesos/state/state.hpp>

#include <mesos/v1/mesos.hpp>

#include <mesos/v1/resource_provider/resource_provider.hpp>

#include <process/clock.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>

#include <process/ssl/flags.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/gtest.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/recordio.hpp>
#include <stout/result.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/http.hpp"
#include "common/recordio.hpp"

#include "internal/devolve.hpp"

#include "resource_provider/manager.hpp"
#include "resource_provider/registrar.hpp"

#include "slave/slave.hpp"

#include "tests/mesos.hpp"

namespace http = process::http;

using mesos::internal::slave::Slave;

using mesos::master::detector::MasterDetector;

using mesos::state::InMemoryStorage;
using mesos::state::State;

using mesos::resource_provider::AdmitResourceProvider;
using mesos::resource_provider::Registrar;
using mesos::resource_provider::RemoveResourceProvider;

using mesos::v1::resource_provider::Call;
using mesos::v1::resource_provider::Event;

using process::Clock;
using process::Future;
using process::Owned;
using process::PID;

using process::http::Accepted;
using process::http::BadRequest;
using process::http::OK;
using process::http::UnsupportedMediaType;

using std::string;

using testing::Values;
using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {

class ResourceProviderManagerHttpApiTest
  : public MesosTest,
    public WithParamInterface<ContentType> {};


// The tests are parameterized by the content type of the request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    ResourceProviderManagerHttpApiTest,
    Values(ContentType::PROTOBUF, ContentType::JSON));


TEST_F(ResourceProviderManagerHttpApiTest, NoContentType)
{
  http::Request request;
  request.method = "POST";
  request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);

  ResourceProviderManager manager;

  Future<http::Response> response = manager.api(request, None());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  AWAIT_EXPECT_RESPONSE_BODY_EQ(
      "Expecting 'Content-Type' to be present",
      response);
}


// This test sends a valid JSON blob that cannot be deserialized
// into a valid protobuf resulting in a BadRequest.
TEST_F(ResourceProviderManagerHttpApiTest, ValidJsonButInvalidProtobuf)
{
  JSON::Object object;
  object.values["string"] = "valid_json";

  http::Request request;
  request.method = "POST";
  request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  request.headers["Accept"] = APPLICATION_JSON;
  request.headers["Content-Type"] = APPLICATION_JSON;
  request.body = stringify(object);

  ResourceProviderManager manager;

  Future<http::Response> response = manager.api(request, None());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  AWAIT_EXPECT_RESPONSE_BODY_EQ(
      "Failed to validate resource_provider::Call: "
      "Expecting 'type' to be present",
      response);
}


TEST_P(ResourceProviderManagerHttpApiTest, MalformedContent)
{
  const ContentType contentType = GetParam();

  http::Request request;
  request.method = "POST";
  request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  request.headers["Accept"] = stringify(contentType);
  request.headers["Content-Type"] = stringify(contentType);
  request.body = "MALFORMED_CONTENT";

  ResourceProviderManager manager;

  Future<http::Response> response = manager.api(request, None());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  switch (contentType) {
    case ContentType::PROTOBUF:
      AWAIT_EXPECT_RESPONSE_BODY_EQ(
          "Failed to parse body into Call protobuf",
          response);
      break;
    case ContentType::JSON:
      AWAIT_EXPECT_RESPONSE_BODY_EQ(
          "Failed to parse body into JSON: "
          "syntax error at line 1 near: MALFORMED_CONTENT",
          response);
      break;
    case ContentType::RECORDIO:
      break;
  }
}


TEST_P(ResourceProviderManagerHttpApiTest, UnsupportedContentMediaType)
{
  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();

  mesos::v1::ResourceProviderInfo* info =
    subscribe->mutable_resource_provider_info();

  info->set_type("org.apache.mesos.rp.test");
  info->set_name("test");

  const ContentType contentType = GetParam();
  const string unknownMediaType = "application/unknown-media-type";

  http::Request request;
  request.method = "POST";
  request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  request.headers["Accept"] = stringify(contentType);
  request.headers["Content-Type"] = unknownMediaType;
  request.body = serialize(contentType, call);

  ResourceProviderManager manager;

  Future<http::Response> response = manager.api(request, None());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(UnsupportedMediaType().status, response);
}


TEST_P(ResourceProviderManagerHttpApiTest, UpdateState)
{
  const ContentType contentType = GetParam();

  ResourceProviderManager manager;

  Option<UUID> streamId;
  Option<mesos::v1::ResourceProviderID> resourceProviderId;

  // First, subscribe to the manager to get the ID.
  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();

    mesos::v1::ResourceProviderInfo* info =
      subscribe->mutable_resource_provider_info();

    info->set_type("org.apache.mesos.rp.test");
    info->set_name("test");

    http::Request request;
    request.method = "POST";
    request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    request.headers["Accept"] = stringify(contentType);
    request.headers["Content-Type"] = stringify(contentType);
    request.body = serialize(contentType, call);

    Future<http::Response> response = manager.api(request, None());

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
    ASSERT_EQ(http::Response::PIPE, response->type);

    ASSERT_TRUE(response->headers.contains("Mesos-Stream-Id"));
    Try<UUID> uuid = UUID::fromString(response->headers.at("Mesos-Stream-Id"));

    CHECK_SOME(uuid);
    streamId = uuid.get();

    Option<http::Pipe::Reader> reader = response->reader;
    ASSERT_SOME(reader);

    recordio::Reader<Event> responseDecoder(
        ::recordio::Decoder<Event>(
            lambda::bind(deserialize<Event>, contentType, lambda::_1)),
        reader.get());

    Future<Result<Event>> event = responseDecoder.read();
    AWAIT_READY(event);
    ASSERT_SOME(event.get());

    // Check event type is subscribed and the resource provider id is set.
    ASSERT_EQ(Event::SUBSCRIBED, event->get().type());

    resourceProviderId = event->get().subscribed().provider_id();

    EXPECT_FALSE(resourceProviderId->value().empty());
  }

  // Then, update the total resources to the manager.
  {
    std::vector<v1::Resource> resources =
      v1::Resources::fromString("disk:4").get();
    foreach (v1::Resource& resource, resources) {
      resource.mutable_provider_id()->CopyFrom(resourceProviderId.get());
    }

    Call call;
    call.set_type(Call::UPDATE_STATE);
    call.mutable_resource_provider_id()->CopyFrom(resourceProviderId.get());

    Call::UpdateState* updateState = call.mutable_update_state();

    updateState->mutable_resources()->CopyFrom(v1::Resources(resources));
    updateState->set_resource_version_uuid(UUID::random().toBytes());

    http::Request request;
    request.method = "POST";
    request.headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    request.headers["Accept"] = stringify(contentType);
    request.headers["Content-Type"] = stringify(contentType);
    request.headers["Mesos-Stream-Id"] = stringify(streamId.get());
    request.body = serialize(contentType, call);

    Future<http::Response> response = manager.api(request, None());

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(Accepted().status, response);

    // The manager will send out a message informing its subscriber
    // about the newly added resources.
    Future<ResourceProviderMessage> message = manager.messages().get();

    AWAIT_READY(message);

    EXPECT_EQ(
        ResourceProviderMessage::Type::UPDATE_TOTAL_RESOURCES,
        message->type);
    EXPECT_EQ(
        devolve(resourceProviderId.get()), message->updateTotalResources->id);
    EXPECT_EQ(devolve(resources), message->updateTotalResources->total);
  }
}


// This test starts an agent and connects directly with its resource
// provider endpoint.
TEST_P(ResourceProviderManagerHttpApiTest, AgentEndpoint)
{
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Slave::__recover);

  Owned<MasterDetector> detector = master.get()->createDetector();

  Try<Owned<cluster::Slave>> agent = StartSlave(detector.get());
  ASSERT_SOME(agent);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();

  mesos::v1::ResourceProviderInfo* info =
    subscribe->mutable_resource_provider_info();

  info->set_type("org.apache.mesos.rp.test");
  info->set_name("test");

  const ContentType contentType = GetParam();

  http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
  headers["Accept"] = stringify(contentType);

  Future<http::Response> response = http::streaming::post(
      agent.get()->pid,
      "api/v1/resource_provider",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ("chunked", "Transfer-Encoding", response);
  ASSERT_EQ(http::Response::PIPE, response->type);

  Option<http::Pipe::Reader> reader = response->reader;
  ASSERT_SOME(reader);

  recordio::Reader<Event> responseDecoder(
      ::recordio::Decoder<Event>(
          lambda::bind(deserialize<Event>, contentType, lambda::_1)),
      reader.get());

  Future<Result<Event>> event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());

  // Check event type is subscribed and the resource provider id is set.
  EXPECT_EQ(Event::SUBSCRIBED, event->get().type());
  EXPECT_FALSE(event->get().subscribed().provider_id().value().empty());
}

class ResourceProviderRegistrarTest : public tests::MesosTest {};


// Test that the agent resource provider registrar works as expected.
TEST_F(ResourceProviderRegistrarTest, AgentRegistrar)
{
  ResourceProviderID resourceProviderId;
  resourceProviderId.set_value("foo");

  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  Owned<MasterDetector> detector = master.get()->createDetector();

  const slave::Flags flags = CreateSlaveFlags();

  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), master.get()->pid, _);

  Future<UpdateSlaveMessage> updateSlaveMessage =
    FUTURE_PROTOBUF(UpdateSlaveMessage(), _, _);

  Try<Owned<cluster::Slave>> slave = StartSlave(detector.get(), flags);
  ASSERT_SOME(slave);

  AWAIT_READY(slaveRegisteredMessage);

  // The agent will send `UpdateSlaveMessage` after it has created its
  // meta directories. Await the message to make sure the agent
  // registrar can create its store in the meta hierarchy.
  AWAIT_READY(updateSlaveMessage);

  Try<Owned<Registrar>> registrar =
    Registrar::create(flags, slaveRegisteredMessage->slave_id());

  ASSERT_SOME(registrar);
  ASSERT_NE(nullptr, registrar->get());

  // Applying operations on a not yet recovered registrar fails.
  AWAIT_FAILED(registrar.get()->apply(Owned<Registrar::Operation>(
      new AdmitResourceProvider(resourceProviderId))));

  AWAIT_READY(registrar.get()->recover());

  AWAIT_READY(registrar.get()->apply(Owned<Registrar::Operation>(
      new AdmitResourceProvider(resourceProviderId))));

  AWAIT_READY(registrar.get()->apply(Owned<Registrar::Operation>(
      new RemoveResourceProvider(resourceProviderId))));
}


// Test that the master resource provider registrar works as expected.
TEST_F(ResourceProviderRegistrarTest, MasterRegistrar)
{
  ResourceProviderID resourceProviderId;
  resourceProviderId.set_value("foo");

  InMemoryStorage storage;
  State state(&storage);
  master::Registrar masterRegistrar(CreateMasterFlags(), &state);

  const MasterInfo masterInfo = protobuf::createMasterInfo({});

  Try<Owned<Registrar>> registrar = Registrar::create(&masterRegistrar);

  ASSERT_SOME(registrar);
  ASSERT_NE(nullptr, registrar->get());

  // Applying operations on a not yet recovered registrar fails.
  AWAIT_FAILED(registrar.get()->apply(Owned<Registrar::Operation>(
      new AdmitResourceProvider(resourceProviderId))));

  AWAIT_READY(masterRegistrar.recover(masterInfo));

  AWAIT_READY(registrar.get()->apply(Owned<Registrar::Operation>(
      new AdmitResourceProvider(resourceProviderId))));

  AWAIT_READY(registrar.get()->apply(Owned<Registrar::Operation>(
      new RemoveResourceProvider(resourceProviderId))));
}


class ResourceProviderManagerTest : public ::testing::Test
{
public:
  ResourceProviderManagerTest()
    : endpointProcess(&resourceProviderManager),
      pid(process::spawn(endpointProcess, false)) {}

  ~ResourceProviderManagerTest()
  {
    process::terminate(pid);
    process::wait(pid);
  }

  struct EndpointProcess : process::Process<EndpointProcess>
  {
    explicit EndpointProcess(ResourceProviderManager* _resourceProviderManager)
      : resourceProviderManager(_resourceProviderManager) {}

    void initialize() override
    {
      route(
          "/api/v1/resource_provider",
          None(),
          defer(self(), [this](const http::Request& request) {
            return resourceProviderManager->api(request, None());
          }));
    }

    ResourceProviderManager* resourceProviderManager;
  };

  ResourceProviderManager resourceProviderManager;
  EndpointProcess endpointProcess;
  const PID<EndpointProcess> pid;
};


TEST_F(ResourceProviderManagerTest, Apply)
{
  v1::MockResourceProvider resourceProvider;

  Future<Nothing> connected;
  EXPECT_CALL(resourceProvider, connected())
    .WillOnce(FutureSatisfy(&connected));

  string scheme = "http";

#ifdef USE_SSL_SOCKET
  if (process::network::openssl::flags().enabled) {
    scheme = "https";
  }
#endif

  http::URL url(
      scheme,
      pid.address.ip,
      pid.address.port,
      pid.id + "/api/v1/resource_provider");

  Owned<EndpointDetector> endpointDetector(new ConstantEndpointDetector(url));

  resourceProvider.start(
      endpointDetector, ContentType::PROTOBUF, v1::DEFAULT_CREDENTIAL);

  AWAIT_READY(connected);

  Future<Event::Subscribed> subscribed;
  EXPECT_CALL(resourceProvider, subscribed(_))
    .WillOnce(FutureArg<0>(&subscribed));

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();

  mesos::v1::ResourceProviderInfo* resourceProviderInfo =
    subscribe->mutable_resource_provider_info();

  resourceProviderInfo->set_type("org.apache.mesos.rp.test");
  resourceProviderInfo->set_name("test");

  resourceProvider.send(call);

  AWAIT_READY(subscribed);

  Resource resource;
  resource.mutable_provider_id()->set_value(subscribed->provider_id().value());
  resource.set_name("disk");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(100);

  Offer::Operation operation;
  operation.set_type(Offer::Operation::CREATE_VOLUME);
  operation.mutable_create_volume()->mutable_source()->CopyFrom(resource);
  operation.mutable_create_volume()->set_target_type(
      Resource::DiskInfo::Source::PATH);

  Future<Event::Operation> event;
  EXPECT_CALL(resourceProvider, operation(_)).WillOnce(FutureArg<0>(&event));

  // We're free to set any value here because they're only important
  // when handled in the master.
  FrameworkID frameworkId;
  frameworkId.set_value("foo");
  const UUID operationUUID = UUID::random();

  resourceProviderManager.apply(frameworkId, operation, operationUUID);

  AWAIT_READY(event);
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
