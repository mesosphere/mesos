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

#include "resource_provider/storage/paths.hpp"

#include <process/http.hpp>

#include <stout/check.hpp>
#include <stout/fs.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>

namespace http = process::http;

using std::string;

namespace mesos {
namespace internal {
namespace storage {
namespace paths {

// File names.
const char ENDPOINT_SOCKET_FILE[] = "endpoint.sock";
const char STATE_FILE[] = "state";


const char CSI_DIR[] = "csi";
const char PLUGINS_DIR[] = "plugins";
const char VOLUMES_DIR[] = "volumes";
const char STATES_DIR[] = "states";


const char CSI_ENDPOINT_DIR_SYMLINK[] = "endpoint";
const char CSI_ENDPOINT_DIR[] = "mesos-csi-XXXXXX";


string getCsiPluginPath(
    const string& rootDir,
    const string& pluginName)
{
  return path::join(rootDir, CSI_DIR, PLUGINS_DIR, pluginName);
}


Try<string> getCsiEndpointPath(
    const string& rootDir,
    const string& pluginName)
{
  const string plugin = getCsiPluginPath(rootDir, pluginName);

  Try<Nothing> mkdir = os::mkdir(plugin);
  if(mkdir.isError()) {
    return Error(
        "Failed to create plugin directory '" + plugin + "': " + mkdir.error());
  }

  const string endpoint = path::join(plugin, CSI_ENDPOINT_DIR_SYMLINK);

  Result<string> realpath = os::realpath(endpoint);
  if (realpath.isSome()) {
    return path::join(realpath.get(), ENDPOINT_SOCKET_FILE);
  }

  if (os::exists(endpoint)) {
    Try<Nothing> rm = os::rm(endpoint);
    if (rm.isError()) {
      return Error(
          "Failed to remove endpoint symlink '" + endpoint + "': " +
          rm.error());
    }
  }

  // NOTE: We do not use `os::temp()` here since $TMPDIR may be long.
  string temp = path::join("", "tmp");

  Try<string> mkdtemp = os::mkdtemp(path::join(temp, CSI_ENDPOINT_DIR));
  if (mkdtemp.isError()) {
    return Error(
        "Failed to create endpoint directory in '" + temp + "': " +
        mkdtemp.error());
  }

  Try<Nothing> symlink = fs::symlink(mkdtemp.get(), endpoint);
  if (symlink.isError()) {
    return Error(
        "Failed to symlink directory '" + mkdtemp.get() + "' to '" + endpoint +
        "': " + symlink.error());
  }

  return path::join(mkdtemp.get(), ENDPOINT_SOCKET_FILE);
}


string getCsiVolumeRootDir(const string& rootDir)
{
  return path::join(rootDir, CSI_DIR, VOLUMES_DIR);
}


string getCsiVolumePath(const string& rootDir, const string& volumeId)
{
  return path::join(getCsiVolumeRootDir(rootDir), http::encode(volumeId));
}


string getCsiVolumeStatePath(const string& rootDir, const string& volumeId)
{
  return path::join(
     rootDir,
     CSI_DIR,
     STATES_DIR,
     http::encode(volumeId),
     STATE_FILE);
}

} // namespace paths {
} // namespace storage {
} // namespace internal {
} // namespace mesos {
