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

#include <string>

#include <gmock/gmock.h>

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/io.hpp>
#include <process/process.hpp>
#include <process/socket.hpp>

#include <process/ssl/gtest.hpp>

#include <stout/gtest.hpp>
#include <stout/try.hpp>

#include <stout/tests/utils.hpp>

namespace io = process::io;

namespace inet4 = process::network::inet4;
#ifndef __WINDOWS__
namespace unix = process::network::unix;
#endif // __WINDOWS__

using process::Future;
using process::READONLY_HTTP_AUTHENTICATION_REALM;
using process::READWRITE_HTTP_AUTHENTICATION_REALM;

using process::network::inet::Address;
using process::network::inet::Socket;

using std::string;

using testing::WithParamInterface;

namespace process {

// We need to reinitialize libprocess in order to test against different
// configurations, such as when libprocess is initialized with SSL enabled.
void reinitialize(
    const Option<string>& delegate,
    const Option<string>& readonlyAuthenticationRealm,
    const Option<string>& readwriteAuthenticationRealm);

} // namespace process {

class SocketTest : public TemporaryDirectoryTest {};

#ifndef __WINDOWS__
TEST_F(SocketTest, Unix)
{
  Try<unix::Socket> server = unix::Socket::create();
  ASSERT_SOME(server);

  Try<unix::Socket> client = unix::Socket::create();
  ASSERT_SOME(client);

  // Use a path in the temporary directory so it gets cleaned up.
  string path = path::join(sandbox.get(), "socket");

  Try<unix::Address> address = unix::Address::create(path);
  ASSERT_SOME(address);

  ASSERT_SOME(server->bind(address.get()));
  ASSERT_SOME(server->listen(1));

  Future<unix::Socket> accept = server->accept();

  AWAIT_READY(client->connect(address.get()));
  AWAIT_READY(accept);

  unix::Socket socket = accept.get();

  const string data = "Hello World!";

  AWAIT_READY(client->send(data));
  AWAIT_EQ(data, socket.recv(data.size()));

  AWAIT_READY(socket.send(data));
  AWAIT_EQ(data, client->recv(data.size()));
}


// Parameterize the tests with the type of encryption used.
class NetSocketTest : public SSLTemporaryDirectoryTest,
                      public WithParamInterface<string>
{
// These are only needed if libprocess is compiled with SSL support.
#ifdef USE_SSL_SOCKET
protected:
  virtual void SetUp()
  {
    // We must run the parent's `SetUp` first so that we `chdir` into the test
    // directory before SSL helpers like `key_path()` are called.
    SSLTemporaryDirectoryTest::SetUp();

    if (GetParam() == "SSL") {
      generate_keys_and_certs();
      set_environment_variables({
          {"LIBPROCESS_SSL_ENABLED", "true"},
          {"LIBPROCESS_SSL_KEY_FILE", key_path()},
          {"LIBPROCESS_SSL_CERT_FILE", certificate_path()}});
    } else {
      set_environment_variables({});
    }

    process::reinitialize(
        None(),
        READWRITE_HTTP_AUTHENTICATION_REALM,
        READONLY_HTTP_AUTHENTICATION_REALM);
  }

public:
  static void TearDownTestCase()
  {
    set_environment_variables({});
    process::reinitialize(
        None(),
        READWRITE_HTTP_AUTHENTICATION_REALM,
        READONLY_HTTP_AUTHENTICATION_REALM);

    SSLTemporaryDirectoryTest::TearDownTestCase();
  }
#endif // USE_SSL_SOCKET
};

// NOTE: `#ifdef`'ing out the argument `string("SSL")` argument causes a
// build break on Windows, because the preprocessor is not required to to
// process the text it expands.
#ifdef USE_SSL_SOCKET
INSTANTIATE_TEST_CASE_P(
    Encryption,
    NetSocketTest,
    ::testing::Values(
        string("SSL"),
        string("Non-SSL")));
#else
INSTANTIATE_TEST_CASE_P(
    Encryption,
    NetSocketTest,
    ::testing::Values(
        string("Non-SSL")));
#endif // USE_SSL_SOCKET


// This test verifies that if an EOF arrives on a socket when there is no
// pending `recv()` call, the EOF will be correctly received.
TEST_P(NetSocketTest, EOFBeforeRecv)
{
  Try<Socket> client = Socket::create();
  ASSERT_SOME(client);

  const string data = "Lorem ipsum dolor sit amet";

  Try<Socket> server = Socket::create();
  ASSERT_SOME(server);

  Try<Address> server_address = server->bind(inet4::Address::ANY_ANY());
  ASSERT_SOME(server_address);

  ASSERT_SOME(server->listen(1));
  Future<Socket> server_accept = server->accept();

  // Connect to the IP from the libprocess library, but use the port
  // from the `bind` call above. The libprocess IP will always report
  // a locally bindable IP, meaning it will also work for the server
  // socket above.
  //
  // NOTE: We do not use the server socket's address directly because
  // this contains a `0.0.0.0` IP. According to RFC1122, this is an
  // invalid address, except when used to resolve a host's address
  // for the first time.
  // See: https://tools.ietf.org/html/rfc1122#section-3.2.1.3
  AWAIT_READY(
      client->connect(Address(process::address().ip, server_address->port)));

  AWAIT_READY(server_accept);

  Socket server_socket = server_accept.get();

  AWAIT_READY(server_socket.send(data));
  AWAIT_EXPECT_EQ(data, client->recv(data.size()));

  // Shutdown the socket before the final `recv()` is called.
  server_socket.shutdown(Socket::Shutdown::READ_WRITE);

  AWAIT_EXPECT_EQ(string(), client->recv());
}


// This test verifies that if an EOF arrives on a socket when there is a
// pending `recv()` call, the EOF will be correctly received.
TEST_P(NetSocketTest, EOFAfterRecv)
{
  Try<Socket> client = Socket::create();
  ASSERT_SOME(client);

  const string data = "Lorem ipsum dolor sit amet";

  Try<Socket> server = Socket::create();
  ASSERT_SOME(server);

  Try<Address> server_address = server->bind(inet4::Address::ANY_ANY());
  ASSERT_SOME(server_address);

  ASSERT_SOME(server->listen(1));
  Future<Socket> server_accept = server->accept();

  // Connect to the IP from the libprocess library, but use the port
  // from the `bind` call above. The libprocess IP will always report
  // a locally bindable IP, meaning it will also work for the server
  // socket above.
  //
  // NOTE: We do not use the server socket's address directly because
  // this contains a `0.0.0.0` IP. According to RFC1122, this is an
  // invalid address, except when used to resolve a host's address
  // for the first time.
  // See: https://tools.ietf.org/html/rfc1122#section-3.2.1.3
  AWAIT_READY(
      client->connect(Address(process::address().ip, server_address->port)));

  AWAIT_READY(server_accept);

  Socket server_socket = server_accept.get();

  AWAIT_READY(server_socket.send(data));
  AWAIT_EXPECT_EQ(data, client->recv(data.size()));

  // Make the final `recv()` call before the socket is shutdown.
  Future<string> receive = client->recv();

  server_socket.shutdown(Socket::Shutdown::READ_WRITE);

  AWAIT_EXPECT_EQ(string(), receive);
}
#endif // __WINDOWS__


// Tests the semantics of a `Socket::recv()` properly returning when
// the client socket does a read shutdown (versus the remote end doing
// a write shutdown, see ServerSocketShutdown test).
TEST_P(NetSocketTest, ClientSocketShutdown)
{
  Try<Socket> client = Socket::create();
  ASSERT_SOME(client);

  Try<Socket> server = Socket::create();
  ASSERT_SOME(server);

  Try<Address> address = server->bind(inet4::Address::ANY_ANY());
  ASSERT_SOME(address);

  ASSERT_SOME(server->listen(1));
  Future<Socket> accept = server->accept();

  // Connect to the IP from the libprocess library, but use the port
  // from the `bind` call above. The libprocess IP will always report
  // a locally bindable IP, meaning it will also work for the server
  // socket above.
  //
  // NOTE: We do not use the server socket's address directly because
  // this contains a `0.0.0.0` IP. According to RFC1122, this is an
  // invalid address, except when used to resolve a host's address
  // for the first time.
  // See: https://tools.ietf.org/html/rfc1122#section-3.2.1.3
  AWAIT_READY(client->connect(Address(process::address().ip, address->port)));

  Future<string> future = client->recv();

  ASSERT_SOME(client->shutdown());

  AWAIT_READY(future);
}


// Tests the semantics of a `Socket::recv()` properly returning when
// the server does a write shutdown on an accepted socket (versus the
// client end doing a read shutdown, see ClientSocketShutdown test).
TEST_P(NetSocketTest, ServerSocketShutdown)
{
  Try<Socket> client = Socket::create();
  ASSERT_SOME(client);

  Try<Socket> server = Socket::create();
  ASSERT_SOME(server);

  Try<Address> address = server->bind(inet4::Address::ANY_ANY());
  ASSERT_SOME(address);

  ASSERT_SOME(server->listen(1));
  Future<Socket> accept = server->accept();

  // Connect to the IP from the libprocess library, but use the port
  // from the `bind` call above. The libprocess IP will always report
  // a locally bindable IP, meaning it will also work for the server
  // socket above.
  //
  // NOTE: We do not use the server socket's address directly because
  // this contains a `0.0.0.0` IP. According to RFC1122, this is an
  // invalid address, except when used to resolve a host's address
  // for the first time.
  // See: https://tools.ietf.org/html/rfc1122#section-3.2.1.3
  AWAIT_READY(client->connect(Address(process::address().ip, address->port)));

  Future<string> future = client->recv();

  AWAIT_READY(accept);

  Socket socket = accept.get(); // Need non-const copy for `Socket::shutdown()`.

  ASSERT_SOME(socket.shutdown(Socket::Shutdown::WRITE));

  AWAIT_READY(future);
}


// Tests the semantics of a client attempting to do a read shutdown
// after the server does a write shutdown on an accepted socket.
TEST_P(NetSocketTest, ServerClientSocketShutdown)
{
  Try<Socket> client = Socket::create();
  ASSERT_SOME(client);

  Try<Socket> server = Socket::create();
  ASSERT_SOME(server);

  Try<Address> address = server->bind(inet4::Address::ANY_ANY());
  ASSERT_SOME(address);

  ASSERT_SOME(server->listen(1));
  Future<Socket> accept = server->accept();

  // Connect to the IP from the libprocess library, but use the port
  // from the `bind` call above. The libprocess IP will always report
  // a locally bindable IP, meaning it will also work for the server
  // socket above.
  //
  // NOTE: We do not use the server socket's address directly because
  // this contains a `0.0.0.0` IP. According to RFC1122, this is an
  // invalid address, except when used to resolve a host's address
  // for the first time.
  // See: https://tools.ietf.org/html/rfc1122#section-3.2.1.3
  AWAIT_READY(client->connect(Address(process::address().ip, address->port)));

  Future<string> future = client->recv();

  AWAIT_READY(accept);

  Socket socket = accept.get(); // Need non-const copy for `Socket::shutdown()`.

  ASSERT_SOME(socket.shutdown(Socket::Shutdown::WRITE));

  AWAIT_READY(future);

  // On Mac OS X `Socket::shutdown()` will fail if the socket is
  // already shutdown, whether locally or from the other side. Linux
  // lets you do the shutdown even if it's already shutdown.
#ifdef __APPLE__
  ASSERT_ERROR(client->shutdown());
#else
  ASSERT_SOME(client->shutdown());
#endif // __APPLE__
}
