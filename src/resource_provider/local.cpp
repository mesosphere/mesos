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

#include "resource_provider/local.hpp"

#include "resource_provider/storage/provider.hpp"

using std::string;

using process::Owned;

namespace mesos {
namespace internal {

Try<Owned<LocalResourceProvider>> LocalResourceProvider::create(
    const process::http::URL& url,
    const ResourceProviderInfo& info,
    const Option<string>& authToken)
{
  // TODO(jieyu): Document the built-in local resource providers.
  if (info.type() == "org.apache.mesos.rp.local.storage") {
    Try<Owned<LocalResourceProvider>> provider =
      StorageLocalResourceProvider::create(url, info, authToken);

    if (provider.isError()) {
      return Error(
          "Failed to create storage local resource provider: " +
          provider.error());
    }

    return provider.get();
  }

  return Error("Unknown resource provider type '" + info.type() + "'");
}

} // namespace internal {
} // namespace mesos {
