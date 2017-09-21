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

#include "authentication/http/basic_authenticatee.hpp"

#include <string>

#include <mesos/v1/mesos.hpp>

#include <process/id.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/http.hpp>
#include <process/process.hpp>

#include <stout/base64.hpp>
#include <stout/option.hpp>

namespace mesos {
namespace http {
namespace authentication {

class BasicAuthenticateeProcess
  : public process::Process<BasicAuthenticateeProcess>
{
public:
  BasicAuthenticateeProcess()
    : ProcessBase(process::ID::generate("basic_authenticatee")) {}

  process::Future<process::http::Request> authenticate(
      const process::http::Request& request,
      const Option<mesos::v1::Credential>& credential)
  {
    process::http::Request decoratedRequest(request);

    if (credential.isSome()) {
      decoratedRequest.headers["Authorization"] =
        "Basic " +
        base64::encode(credential->principal() + ":" + credential->secret());
    }

    return decoratedRequest;
  }
};


BasicAuthenticatee::BasicAuthenticatee()
  : process_(new BasicAuthenticateeProcess())
{
  spawn(*process_);
}


BasicAuthenticatee::~BasicAuthenticatee()
{
  terminate(*process_);
  wait(*process_);
}


std::string BasicAuthenticatee::scheme() const
{
  return "Basic";
}


process::Future<process::http::Request>
BasicAuthenticatee::authenticate(
    const process::http::Request& request,
    const Option<mesos::v1::Credential>& credential)
{
  return dispatch(
      *process_,
      &BasicAuthenticateeProcess::authenticate,
      request,
      credential);
}

} // namespace authentication {
} // namespace http {
} // namespace mesos {
