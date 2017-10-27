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

#ifndef __RESOURCE_PROVIDER_STORAGE_PATHS_HPP__
#define __RESOURCE_PROVIDER_STORAGE_PATHS_HPP__

#include <string>

#include <stout/try.hpp>

namespace mesos {
namespace internal {
namespace storage {
namespace paths {

// The file system layout is as follows:
//
//   root ('--work_dir' flag)
//   |-- resource_providers
//       |-- <type>
//           |-- <name>
//               |-- csi
//                   |-- plugins
//                   |   |-- <plugin_name>
//                   |       |-- endpoint (symlink to /tmp/mesos-csi-XXXXXX)
//                   |           |-- endpoint.sock
//                   |-- volumes
//                   |   |-- <volume_id> (mount point)
//                   |-- states
//                       |-- <volume_id>
//                            |-- state


std::string getCsiPluginPath(
    const std::string& rootDir,
    const std::string& pluginName);


// Returns the resolved path to the endpoint socket, even if the socket
// file itself does not exist. Creates and symlinks the endpoint
// directory if necessary.
Try<std::string> getCsiEndpointPath(
    const std::string& rootDir,
    const std::string& pluginName);


std::string getCsiVolumeRootDir(const std::string& rootDir);


std::string getCsiVolumePath(
    const std::string& rootDir,
    const std::string& volumeId);


std::string getCsiVolumeStatePath(
    const std::string& rootDir,
    const std::string& volumeId);

} // namespace paths {
} // namespace storage {
} // namespace internal {
} // namespace mesos {

#endif // __RESOURCE_PROVIDER_STORAGE_PATHS_HPP__
