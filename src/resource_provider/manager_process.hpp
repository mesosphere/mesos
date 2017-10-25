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

#ifndef __RESOURCE_PROVIDER_MANAGER_PROCESS_HPP__
#define __RESOURCE_PROVIDER_MANAGER_PROCESS_HPP__

#include <mesos/mesos.hpp>

#include <mesos/resource_provider/resource_provider.hpp>

#include <mesos/v1/resource_provider/resource_provider.hpp>

#include <process/authenticator.hpp>
#include <process/http.hpp>
#include <process/process.hpp>
#include <process/queue.hpp>

#include <stout/option.hpp>
#include <stout/recordio.hpp>
#include <stout/uuid.hpp>

#include "common/http.hpp"

#include "internal/evolve.hpp"

#include "resource_provider/message.hpp"

namespace mesos {
namespace internal {

// Represents the streaming HTTP connection to a resource provider.
struct HttpConnection
{
  HttpConnection(const process::http::Pipe::Writer& _writer,
                 ContentType _contentType,
                 UUID _streamId)
    : writer(_writer),
      contentType(_contentType),
      streamId(_streamId),
      encoder(lambda::bind(serialize, contentType, lambda::_1)) {}

  // Converts the message to an Event before sending.
  template <typename Message>
  bool send(const Message& message)
  {
    // We need to evolve the internal 'message' into a
    // 'v1::resource_provider::Event'.
    return writer.write(encoder.encode(evolve(message)));
  }

  bool close()
  {
    return writer.close();
  }

  process::Future<Nothing> closed() const
  {
    return writer.readerClosed();
  }

  process::http::Pipe::Writer writer;
  ContentType contentType;
  UUID streamId;
  ::recordio::Encoder<v1::resource_provider::Event> encoder;
};


struct ResourceProvider
{
  ResourceProvider(
      const ResourceProviderInfo& _info,
      const HttpConnection& _http)
    : info(_info),
      http(_http) {}

  ResourceProviderInfo info;
  HttpConnection http;
  Resources resources;
};


class ResourceProviderManagerProcess
  : public process::Process<ResourceProviderManagerProcess>
{
public:
  ResourceProviderManagerProcess();
  virtual ~ResourceProviderManagerProcess();

  process::Future<process::http::Response> api(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>& principal);

  virtual void applyOfferOperation(
      const ApplyOfferOperationMessage& message);

protected:
  virtual void subscribe(
      const HttpConnection& http,
      const resource_provider::Call::Subscribe& subscribe);

  virtual void updateOfferOperationStatus(
      ResourceProvider* resourceProvider,
      const resource_provider::Call::UpdateOfferOperationStatus& update);

  virtual void updateState(
      ResourceProvider* resourceProvider,
      const resource_provider::Call::UpdateState& update);

  ResourceProviderID newResourceProviderId();

private:
  friend class ResourceProviderManager;

  struct ResourceProviders
  {
    hashmap<ResourceProviderID, ResourceProvider> subscribed;
  } resourceProviders;

  process::Queue<ResourceProviderMessage> messages;
};

} // namespace internal {
} // namespace mesos {

#endif // __RESOURCE_PROVIDER_MANAGER_PROCESS_HPP__
