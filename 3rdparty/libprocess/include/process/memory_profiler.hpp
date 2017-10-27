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

#ifndef __PROCESS_MEMORYPROFILER_HPP__
#define __PROCESS_MEMORYPROFILER_HPP__

#include <string>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/process.hpp>

#include <stout/option.hpp>
#include <stout/path.hpp>

namespace process {

// This class provides support for taking advantage of the introspection
// and memory profiling capabilities of the jemalloc memory allocator.
//
// For user-facing documentation on how to use the facilities provided
// by this class, see `docs/memory-profiling.md` in the mesos repository.
//
// For more details about the implementation, see the comment
// in `memory_profiler.cpp`.

class MemoryProfiler : public process::Process<MemoryProfiler>
{
public:
  MemoryProfiler(const Option<std::string>& authenticationRealm);
  virtual ~MemoryProfiler() {}

protected:
  virtual void initialize();

private:
  // HTTP endpoints.

  // Activate memory profiling. This will increase memory usage of the running
  // process because the heap profiles need to be stored. The additional space
  // requirements are expected to be logarithmic.
  // Parameters:
  //   - duration: How long  (default: 5mins)
  Future<http::Response> start(
      const http::Request& request,
      const Option<http::authentication::Principal>&);

  // Deactivate memory profiling and dump collected data.
  // TODO(bennoe): Add a way to dump an intermediate profile without
  // interrupting the data collection.
  Future<http::Response> stop(
      const http::Request& request,
      const Option<http::authentication::Principal>&);

  // Return heap profile as file that can be passed to jeprof
  // for post-processing.
  // See the `jemalloc(3)` manpage for information about the file format.
  process::Future<process::http::Response> downloadRaw(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>&);

  // Generate and return a call graph in svg format.
  process::Future<process::http::Response> downloadGraph(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>&);

  // Generate and return a symbolized heap profile.
  process::Future<process::http::Response> downloadTextProfile(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>&);

  // Memory allocation statistics.
  // TODO(bennoe): Allow passing custom options via query parameters
  process::Future<process::http::Response> statistics(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>&);

  // Current memory profiler state.
  process::Future<process::http::Response> state(
    const process::http::Request& request,
    const Option<process::http::authentication::Principal>&);

  // Internal functions and data members.

  void route_(
    const Option<std::string>& authenticationRealm,
    Future<http::Response>(MemoryProfiler::*memfn)(
      const http::Request&, const Option<http::authentication::Principal>&),
    const std::string& name,
    const char* tldr,
    const char* description);

  // Returns an error on failure, or the path of the dumped profile
  // on success.
  Try<Path> stop_();

  Try<Path> getTemporaryDirectoryPath();

  Try<Path> generateGraph(time_t id);

  Try<Path> generateTextProfile(time_t id);

  // The authentication realm that the profiler's HTTP endpoints will be
  // installed into.
  Option<std::string> authenticationRealm;

  // Profile and graph files are stored here. Generated lazily on first use
  // and never changed afterwards.
  // Usually, each instance of this class will operate on its own temporary
  // directory.
  Option<Path> temporaryDirectory;

  // These are used to track whether the versions of the stored files
  // correspond to each other.
  Try<time_t> rawId, graphId, textId;

  // Stores the time left for the current profiling run.
  Option<Timer> profilingTimer;

  // Whether heap profiling is enabled.
  bool heapProfilingEnabled;

  // Whether memory backtrace sampling is currently active.
  bool heapProfilingActive;
};

} // namespace process {

#endif // __PROCESS_MEMORYPROFILER_HPP__
