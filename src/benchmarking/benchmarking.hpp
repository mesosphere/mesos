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

#ifndef __BENCHMARKING_HPP__
#define __BENCHMARKING_HPP__

#include <cstdint>


struct timespec;

namespace process {
namespace http {

struct Request;
struct Response;

} // namespace http {
} // namespace process {


namespace mesos {
namespace internal {
namespace benchmarking {
namespace state_json {


void logStateRequest(
    const process::http::Request& request,
    const process::http::Response& response);


} // namespace state_json {
} // namespace benchmarking {
} // namespace internal {
} // namespace mesos {

#endif
