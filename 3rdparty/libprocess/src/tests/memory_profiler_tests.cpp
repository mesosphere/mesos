// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#include <gtest/gtest.h>

#include <process/authenticator.hpp>
#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/process.hpp>
#include <process/memory_profiler.hpp>

#include <stout/json.hpp>

namespace authentication = process::http::authentication;
namespace http = process::http;

using authentication::Authenticator;
using authentication::BasicAuthenticator;

using http::BadRequest;
using http::OK;
using http::Response;
using http::Unauthorized;

using process::Future;
using process::READWRITE_HTTP_AUTHENTICATION_REALM;
using process::UPID;

using std::string;


// TODO(greggomann): Move this into a base class in 'mesos.hpp'.
class MemoryProfilerTest : public ::testing::Test
{
protected:
  Future<Nothing> setAuthenticator(
      const string& realm,
      process::Owned<Authenticator> authenticator)
  {
    realms.insert(realm);

    return authentication::setAuthenticator(realm, authenticator);
  }

  virtual void TearDown()
  {
    foreach (const string& realm, realms) {
      // We need to wait in order to ensure that the operation
      // completes before we leave TearDown. Otherwise, we may
      // leak a mock object.
      AWAIT_READY(authentication::unsetAuthenticator(realm));
    }
    realms.clear();
  }

private:
  hashset<string> realms;
};

// Note that /state is the only endpoint that works without jemalloc,
// so it is also the only one that can be tested from inside
// libprocess. (since libprocess itself doesn't bundle jemalloc,
// and the test is designed to be very hard to fool)

// TODO(bennoe): Add a test that verifies all endpoints are correctly
// disabled when LIBPROCESS_MEMORY_PROFILING is set to `disabled`.

// Check that the state endpoint returns valid json
TEST_F(MemoryProfilerTest, TestState)
{
  UPID upid("memory-profiler", process::address());

  Future<Response> response = http::get(upid, "state");
  AWAIT_READY(response);

  Try<JSON::Value> result = JSON::parse(response.get().body);

  EXPECT_TRUE(result.isSome());
}


// Tests that the profiler's HTTP endpoints reject unauthenticated
// requests when HTTP authentication is enabled.
TEST_F(MemoryProfilerTest, StartAndStopAuthenticationEnabled)
{
  process::Owned<Authenticator> authenticator(
    new BasicAuthenticator(
        READWRITE_HTTP_AUTHENTICATION_REALM, {{"foo", "bar"}}));

  AWAIT_READY(
      setAuthenticator(READWRITE_HTTP_AUTHENTICATION_REALM, authenticator));

  UPID upid("memory-profiler", process::address());

  Future<Response> response = http::get(upid, "start");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response);

  response = http::get(upid, "stop");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response);
}
