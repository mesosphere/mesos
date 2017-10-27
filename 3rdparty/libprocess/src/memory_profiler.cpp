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


#include <process/memory_profiler.hpp>

#include <process/future.hpp>
#include <process/help.hpp>
#include <process/http.hpp>

#include <stout/assert.hpp>
#include <stout/format.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include <glog/logging.h>

#include <chrono>
#include <fstream>
#include <sstream>
#include <string>

using process::Future;
using process::HELP;
using process::TLDR;
using process::DESCRIPTION;
using process::AUTHENTICATION;


// The main workflow to generate and download a heap profile
// goes through the sequence  /start -> /stop -> /download
//
// The generated files are typically stored under the directory
// `/tmp/libprocess.XXXXXX/jemalloc.{txt,svg,dump}`, where XXXXXX
// stands for a random combination of letters. This directory, as well
// as the containing files, are created lazily the first time it is
// accessed.
//
// To avoid running out of disk space, every time a new file is
// generated, the previous one is overwritten. The members rawId,
// graphId and textId track which version, if any, of the corresponding
// artifact is currently available on disk.
//
// As a library, libprocess shouldn't force a specific malloc
// implementation on the binary.
// Instead, we define the relevant functions as weak symbols,
// and check at runtime whether we're running under jemalloc
// or not.

extern "C" __attribute__((__weak__)) void malloc_stats_print(
  void (*writecb)(void*, const char*),
  void* opaque,
  const char* opts);

extern "C" __attribute__((__weak__)) int mallctl(
  const char* opt, void* oldp, size_t* oldsz, void* newp, size_t newsz);

extern "C" __attribute__((__weak__)) const char* malloc_conf;


namespace {

constexpr char LIBPROCESS_DEFAULT_TMPDIR[] = "/tmp/libprocess.XXXXXX";

constexpr char HEAP_PROFILING_DISABLED_MESSAGE[] = R"_(
Heap profiling support was manually disabled.

If this libprocess is running inside a mesos binary, consider setting the
--memory-profiling flag to `enabled`
)_";


constexpr char JEMALLOC_NOT_DETECTED_MESSAGE[] = R"_(
The current binary doesn't seem to be linked against jemalloc,
or the currently used jemalloc library was compiled without
support for statistics collection.

If the current binary was not compiled against jemalloc,
consider adding the path to libjemalloc to the LD_PRELOAD
environment variable, for example LD_PRELOAD=/usr/lib/libjemalloc.so

If you're running a mesos binary, and want to have it linked
against jemalloc by default, consider enabling the
--enable-memory-profiling configuration option.)_";


constexpr char JEMALLOC_PROFILING_NOT_ENABLED[] = R"_(
The current process seems to be using jemalloc, but
profiling couldn't be enabled.

If you're using a custom version of libjemalloc, make sure
that MALLOC_CONF="prof:true" is part of the environment. (The
/status endpoint can be used to double-check the current malloc settings)

If the environment looks correct, make sure jemalloc was built with
the --enable-stats and --enable-prof options enabled.

If you're running a mesos binary that was built with the
--enable-memory-profiling option enabled and you're still seeing this
message, please consider filing a bug report.
)_";


// Size in bytes of the dummy file that gets written when hitting '/start'.
constexpr int DUMMY_FILE_SIZE = 64 * 1024; // 64 KiB


// The `detectJemalloc()`function below was taken from the folly library
// (called `usingJEMalloc()` there), originally distributed by Facebook, Inc
// under the Apache License.
//
// The original author complains about the code, and since
// libprocess is only used by Mesos we might be able to get away
// with a simpler check. On the other hand, it would seem arrogant to
// implement a custom check that is likely more error-prone than the
// battle-tested version below.
bool detectJemalloc() noexcept {
  // Checking for rallocx != NULL is not sufficient; we may be in a dlopen()ed
  // module that depends on libjemalloc, so rallocx is resolved, but the main
  // program might be using a different memory allocator.
  // How do we determine that we're using jemalloc? In the hackiest
  // way possible. We allocate memory using malloc() and see if the
  // per-thread counter of allocated memory increases. This makes me
  // feel dirty inside. Also note that this requires jemalloc to have
  // been compiled with --enable-stats.

  static const bool result = [] () noexcept {
    // Some platforms (*cough* OSX *cough*) require weak symbol checks to be
    // in the form if (mallctl != nullptr). Not if (mallctl) or if (!mallctl)
    // (!!). http://goo.gl/xpmctm
    if (mallctl == nullptr || malloc_stats_print == nullptr) {
      return false;
    }

    // "volatile" because gcc optimizes out the reads from *counter, because
    // it "knows" malloc doesn't modify global state...
    volatile uint64_t* counter;
    size_t counterLen = sizeof(uint64_t*);

    if (mallctl("thread.allocatedp", static_cast<void*>(&counter), &counterLen,
                nullptr, 0) != 0) {
      return false;
    }

    if (counterLen != sizeof(uint64_t*)) {
      return false;
    }

    uint64_t origAllocated = *counter;

    // Static because otherwise clever compilers will find out that
    // the ptr is not used and does not escape the scope, so they will
    // just optimize away the malloc.
    static const void* ptr = malloc(1);
    if (!ptr) {
      // wtf, failing to allocate 1 byte
      return false;
    }

    return (origAllocated != *counter);
  }();

  return result;
}


template<typename T>
Try<T> readJemallocSetting(const char* name)
{
  if (!detectJemalloc()) {
    return Error(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  T value;
  size_t size = sizeof(value);
  int error = mallctl(name, &value, &size, nullptr, 0);

  if (error) {
    return Error(strings::format(
       "Couldn't read option %s: %s", name, ::strerror(error)).get());
  }

  return value;
}


// Returns an error on failure or the previous value on success.
template<typename T>
Try<T> updateJemallocSetting(const char* name, const T& value)
{
  if (!detectJemalloc()) {
    return Error(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  T previous;
  size_t size = sizeof(previous);
  int error = mallctl(
      name, &previous, &size, const_cast<T*>(&value), sizeof(value));

  if (error) {
    return Error(strings::format(
        "Couldn't write value %s for option %s: %s",
        value, name, ::strerror(error)).get());
  }

  return previous;
}


// We cannot just use `updateJemallocSetting()` and ignore the result, because
// not every setting has a previous value to read (in particular prof.dump).
template<typename T>
Try<Nothing> writeJemallocSetting(const char* name, const T& value)
{
  if (!detectJemalloc()) {
    return Error(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  int error = mallctl(
      name, nullptr, nullptr, const_cast<T*>(&value), sizeof(value));

  if (error) {
    return Error(strings::format(
        "Couldn't write value %s for option %s: %s",
        value, name, ::strerror(error)).get());
  }

  return Nothing();
}


Path getRawProfilePath(const Path& directoryPath)
{
  return Path {path::join(directoryPath, "/jemalloc.dump")};
}


Path getTextProfilePath(const Path& directoryPath)
{
  return Path {path::join(directoryPath, "/jemalloc.txt")};
}


Path getGraphPath(const Path& directoryPath)
{
  return Path {path::join(directoryPath, "/jemalloc.svg")};
}


Try<Nothing> jemallocDumpProfile(const std::string& profilePath)
{
  // Even if this is the first actual dump, we should have written a dummy
  // file when hitting /start.
  if (!os::stat::isfile(profilePath)) {
    return Error(strings::format(
        "Cannot verify dump file location %s.", profilePath).get());
  }

  return writeJemallocSetting("prof.dump", profilePath.c_str());
}


enum class JeprofFormat {
  TEXT, GRAPH
};


Try<Nothing> generateJeprofFile(
    const Path& inputPath,
    JeprofFormat format,
    const Path& outputPath)
{
  std::string outputFormat;
  switch (format) {
  case JeprofFormat::TEXT: {
    outputFormat = "--text";
    break;
  }
  case JeprofFormat::GRAPH: {
    outputFormat = "--svg";
    break;
  }
  default:
    UNREACHABLE();
  }

  // As jeprof doesn't have an option to specify an output file, we actually
  // need `os::shell()` here instead of `os::spawn()`.
  // Note that the three parameters *MUST NOT* be controllable by the user
  // hitting the endpoints, otherwise this call could be trivially exploited
  // to gain arbitrary remote code execution.
  // Apart from that, we dont need to be as careful here as with the actual heap
  // profile dump, because a failure will not crash the whole process.
  Try<std::string> result = os::shell(strings::format(
      "jeprof %s /proc/self/exe %s >%s",
      outputFormat,
      inputPath.string(),
      outputPath.string()).get());

  if (result.isError()) {
    return Error(
      "Error trying to run jeprof: " + result.error() + "\n"
      "Is the specified input file empty?");
  }

  return Nothing();
}


process::http::OK htmlResponse(
    const std::string& body,
    const std::string& head = "")
{
  return process::http::OK(
      "<html>"
       "  <head>" + head + "</head>"
       "  <body>" + body + "</body>"
       "</html>",
       "text/html");
}


process::http::OK redirect(
    const Duration& duration,
    const std::string& target,
    const std::string& text)
{
  int seconds = duration.secs();
  std::string redirectTag = "<meta http-equiv=\"refresh\" "
      "content=\"" + stringify(seconds) + ";url=" + target + "\" />";

  return htmlResponse(text, redirectTag);
}


std::string hyperlink(
    const std::string& text,
    const std::string& target)
{
  return "<a href=\"" + target + "\">" + text + "</a>";
}


time_t idFromRequest(
    const process::http::Request& request,
    Try<time_t> fallback)
{
  Option<std::string> idParameter = request.url.query.get("id");
  if (idParameter.isSome()) {
    return std::atoi(idParameter->c_str());
  } else if (fallback.isSome()) {
    return fallback.get();
  } else {
    return 0;
  }
}


process::http::Response fileDownloadResponse(const Path& path)
{
  process::http::OK response;
  response.type = response.PATH;
  response.path = path;
  response.headers["Content-Type"] = "application/octet-stream";
  response.headers["Content-Disposition"] =
      strings::format("attachment; filename=%s", path).get();

  return response;
}

}  // end namespace {


namespace process {

void MemoryProfiler::route_(
    const Option<std::string>& authenticationRealm,
    Future<http::Response>(MemoryProfiler::*memfn)(
      const http::Request&, const Option<http::authentication::Principal>&),
    const std::string& name,
    const char* tldr,
    const char* description)
{
  const std::string help = HELP(
      TLDR(tldr),
      DESCRIPTION(description),
      AUTHENTICATION(true));

  if (authenticationRealm.isSome()) {
    route(name,
          authenticationRealm.get(),
          help,
          memfn);
  } else {
    route(name,
          help,
          [this, memfn](const http::Request& request) {
            return (this->*memfn)(request, None());
          });
  }
}


void MemoryProfiler::initialize()
{
  route_(authenticationRealm, &MemoryProfiler::start,
    "/start",
    "Start collection of stack traces.",
    "Instruct the memory profiler to start collecting data that can "
    "be used to generate heap profile dumps.");

  route_(authenticationRealm, &MemoryProfiler::stop,
    "/stop",
    "Stop collection of stack traces.",
    "Instruct the memory profiler to stop collecting data.");

  route_(authenticationRealm, &MemoryProfiler::downloadRaw,
    "/download/raw",
    "Dump a memory profile.",
    "Returns a file containing the collected backtrace samples "
    "since the previous dump.");

  route_(authenticationRealm, &MemoryProfiler::downloadTextProfile,
    "/download/text",
    "Dump a memory profile.",
    "Generates a symbolized text profile. "
    "Requires the running process to be built with symbols and jeprof "
    "to be installed on the host machine.");

  route_(authenticationRealm, &MemoryProfiler::downloadGraph,
    "/download/graph",
    "Dump a malloc call graph.",
    "Tries to generate a graph from the most recently generated "
    "heap-profile on a best-effort basis. Note that this can take "
    "around 20-30 seconds.");

  route_(authenticationRealm, &MemoryProfiler::state,
    "/state",
    "Show the configuration of the memory-profiler process.",
    "Returns the state of the memory profiler.");

  route_(authenticationRealm,  &MemoryProfiler::statistics,
    "/statistics",
    "Show memory allocation statistics.",
    "Returns current memory allocation statistics in JSON format "
    "as shown by `malloc_stats_print()`.");

  // Try to read current value of "prof.active" so that heapProfilingActive
  // reflects reality in case the user was enabling it via MALLOC_CONF or some
  // other way without telling us.
  if (heapProfilingEnabled && detectJemalloc()) {
    Try<bool> active = readJemallocSetting<bool>("prof.active");

    if (active.isError()) {
      // At this point, the user didn't yet request any memory profiling
      // functionality, so printing this as WARN() seems excessive.
      VLOG(1) << "Couldn't determine heap profile setting: " << active.error();
    } else {
      heapProfilingActive = active.get();
    }
  }
}


MemoryProfiler::MemoryProfiler(const Option<std::string>& _authenticationRealm)
  : ProcessBase("memory-profiler"),
    authenticationRealm(_authenticationRealm),
    rawId(Error("Not generated")),
    graphId(Error("Not generated")),
    textId(Error("Not generated")),
    heapProfilingEnabled(true),
    heapProfilingActive(false)
{
  std::string setting = os::getenv("LIBPROCESS_MEMORY_PROFILING")
    .getOrElse("enabled");

  // We are liberal in what we accept
  if (setting == "disabled" || setting == "0" || setting == "") {
    heapProfilingEnabled = false;
  }
}


// TODO(bennoe): Could we return Try<const Path&> from this?
Try<Path> MemoryProfiler::getTemporaryDirectoryPath()
{
  if (temporaryDirectory.isSome()) {
    return temporaryDirectory.get();
  }

  std::string path = LIBPROCESS_DEFAULT_TMPDIR;

  // TODO(bennoe): Add a libprocess-specific override for the system-wide
  // `TMPDIR`, i.e. `LIBPROCESS_TMPDIR`. Also, this should probably become
  // libprocess-global once there is another class creating temporary files.
  Option<std::string> environmentTmpdir = os::getenv("TMPDIR");
  if (environmentTmpdir.isSome()) {
    path = path::join(environmentTmpdir.get(), "libprocess.XXXXXX");
  }

  // TODO(bennoe): Add an atexit-handler that cleans up the directory.
  // TODO(bennoe): Maybe this path should be made available libprocess-global.
  Try<std::string> dir = os::mkdtemp(path);
  if (dir.isError()) {
    return Error(dir.error());
  }

  temporaryDirectory = dir.get();

  VLOG(1) << "Using path " << dir.get() << " to store temporary files.";

  return temporaryDirectory.get();
}


// TODO(bennoe): Add query parameter to configure the sampling interval.
Future<http::Response> MemoryProfiler::start(
  const http::Request& request,
  const Option<http::authentication::Principal>& principal)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  if (!detectJemalloc()) {
    return http::BadRequest(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  Try<Path> tmpdirPath = getTemporaryDirectoryPath();
  if (tmpdirPath.isError()) {
    return http::BadRequest(tmpdirPath.error());
  }

  std::string profilePath = getRawProfilePath(tmpdirPath.get());

  Duration duration = Minutes(5);

  Option<std::string> durationParameter = request.url.query.get("duration");
  if (durationParameter.isSome()) {
    Try<Duration> parsed = Duration::parse(durationParameter.get());
    if (parsed.isError()) {
      return http::BadRequest(
          "Could not parse parameter 'duration': " + parsed.error());
    }
    duration = parsed.get();
  }

  // Enforce sane defaults.
  // TODO(bennoe): Introduce `Duration::clamp()`.
  if (duration > Hours(24)) {
    duration = Hours(24);
  } else if (duration < Seconds(1)) {
    duration = Seconds(1);
  }

  // Make sure we actually have permissions to write to the file and that
  // there is at least a little bit space left on the device.
  std::string data(DUMMY_FILE_SIZE, '\0');
  Try<Nothing> written = os::write(profilePath, data);
  if (written.isError()) {
    return http::BadRequest(written.error());
  }

  Try<bool> wasActive = updateJemallocSetting("prof.active", true);
  if (wasActive.isError()) {
    return http::BadRequest(
        "Could not activate heap profiling: " + wasActive.error());
  }

  heapProfilingActive = true;

  Duration redirectTime;
  std::string message;
  if (wasActive.get()) {
    message += "Heap profiling was already active.<br/>";
    redirectTime = profilingTimer->timeout().remaining();
  } else {
    message += "Started new heap profiling run.<br/>";
    // We make this slightly longer than the redirect delay so that the user
    // will see a message saying "Success!" by default.
    redirectTime = duration;
    profilingTimer = Clock::timer(duration + Seconds(2), [&] { stop_(); });
  }

  // If the profiling was started externally, e.g. through environment
  // variables, we might have never initialized a timer.
  if (!profilingTimer.isSome()) {
    return htmlResponse(message);
  }

  message += "You will be redirected to the result in ";
  message += stringify(redirectTime);

  return redirect(redirectTime, "./stop", message);
}


Future<http::Response> MemoryProfiler::stop(
    const http::Request& request,
    const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  if (!detectJemalloc()) {
    return http::BadRequest(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  Clock::cancel(profilingTimer.get());

  Try<bool> wasActive = readJemallocSetting<bool>("prof.active");
  if (wasActive.isError()) {
    return http::BadRequest(wasActive.error());
  }

  Try<Path> profilePath = stop_();
  if (profilePath.isError()) {
    return http::BadRequest(profilePath.error());
  }

  ASSERT(rawId.isSome());

  std::string idString = stringify(rawId.get());
  std::string downloadMessage = strings::format(
      "Click here to download profile: %s<br/><br/>"
      "If jeprof is installed on the host machine and symbols "
      "were not stripped from the binary, you can also:<br/>"
      "Download a symbolized profile: %s<br/>"
      "Download a graph: %s<br/>"
      "(These can take up to several minutes to generate)<br/>",
      hyperlink("Link", "./download/raw?id=" + idString),
      hyperlink("Link", "./download/text?id=" + idString),
      hyperlink("Link", "./download/graph?id=" + idString)).get();

  if (wasActive.get()) {
    downloadMessage =
      "Successfully stopped heap profiling.<br/>" + downloadMessage;
  }

  return htmlResponse(downloadMessage);
}


Try<Path> MemoryProfiler::stop_()
{
  ASSERT(heapProfilingEnabled);
  ASSERT(detectJemalloc());

  Try<bool> result = updateJemallocSetting("prof.active", false);

  // Don't give up. There's a slim chance that whatever caused this to fail
  // will disappear in the future, but at least it will be clearly visible
  // in the logs.
  if (result.isError()) {
    LOG(WARNING) << "Failed to stop memory profiling: " << result.error();
    profilingTimer = Clock::timer(Seconds(5), [&] { MemoryProfiler::stop_(); });
    return Error(result.error());
  }

  heapProfilingActive = false;

  // We don't retry after this point: We're not actively sampling any more,
  // and if the user still cares about this profile we will get the data
  // with the next run.

  Try<Path> tmpdirPath = getTemporaryDirectoryPath();
  if (tmpdirPath.isError()) {
    LOG(WARNING) << "Could not create temporary directory: " << result.error();
    return tmpdirPath;
  }

  Path rawProfilePath = getRawProfilePath(tmpdirPath.get());
  Try<Nothing> dumped = jemallocDumpProfile(rawProfilePath);
  if (dumped.isError()) {
    std::string errorMessage = "Could not dump profile: " + dumped.error();
    rawId = Error(errorMessage);
    LOG(WARNING) << errorMessage;
    return Error(errorMessage);
  }

  // This is actually unique because the minimum sample duration is 1s.
  rawId = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());

  return rawProfilePath;
}

Try<Path> MemoryProfiler::generateGraph(time_t id)
{
  const Try<Path>& tmpdir = getTemporaryDirectoryPath();
  if (tmpdir.isError()) {
    return tmpdir;
  }

  const Path& graphPath = getGraphPath(tmpdir.get());

  // We can handle only two cases corectly: Either the graph version that's
  // currently stored on disk is requested, in which case we can just return
  // the file path, or the version corresponding to the current raw profile
  // is requested, in which case we can run jeprof to generate the requested
  // file.
  if (id == graphId.get()) {
    return graphPath;
  }

  if (id != rawId.get()) {
    return Error("Cannot generate graph for id #" + stringify(id));
  }

  Try<Nothing> result = generateJeprofFile(
      getRawProfilePath(tmpdir.get()), JeprofFormat::GRAPH, graphPath);

  if (result.isError()) {
    return Error(result.error());
  }

  graphId = rawId;
  return graphPath;
}


Try<Path> MemoryProfiler::generateTextProfile(time_t id)
{
  Try<Path> tmpdir = getTemporaryDirectoryPath();
  if (tmpdir.isError()) {
    return tmpdir;
  }

  const Path& textProfilePath = getTextProfilePath(tmpdir.get());

  // We can handle only two cases corectly, see also the equivalent comment
  // in `generateGraph()`.
  if (textId.isSome() && textId.get() == id) {
    return textProfilePath;
  }

  if (rawId.isError() || rawId.get() != id) {
    return Error(
        "Cannot generate symbolized heap profile for id #" + stringify(id));
  }

  Try<Nothing> result = generateJeprofFile(
      getRawProfilePath(tmpdir.get()), JeprofFormat::TEXT, textProfilePath);

  if (result.isError()) {
    return Error(result.error());
  }

  textId = rawId;
  return textProfilePath;
}


Future<http::Response> MemoryProfiler::downloadRaw(
    const http::Request& request,
    const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  time_t id = idFromRequest(request, rawId);
  if (rawId.isError() || id != rawId.get()) {
    return http::BadRequest(
        "Cannot serve raw profile #" + stringify(rawId.get()));
  }

  Try<Path> tmpdirPath = getTemporaryDirectoryPath();
  if (tmpdirPath.isError()) {
    return http::BadRequest(tmpdirPath.error());
  }

  Path rawProfilePath = getRawProfilePath(tmpdirPath.get());

  return fileDownloadResponse(rawProfilePath);
}


Future<http::Response> MemoryProfiler::downloadGraph(
    const http::Request& request,
    const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  time_t id = idFromRequest(request, graphId);

  Try<Path> graphPath = generateGraph(id);

  if (graphPath.isError()) {
    return http::BadRequest(graphPath.error());
  }

  return fileDownloadResponse(graphPath.get());
}


Future<http::Response> MemoryProfiler::downloadTextProfile(
    const http::Request& request,
    const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  time_t id = idFromRequest(request, textId);
  Try<Path> textProfilePath = generateTextProfile(id);

  if (textProfilePath.isError()) {
    return http::BadRequest(textProfilePath.error());
  }

  return fileDownloadResponse(textProfilePath.get());
}


Future<http::Response> MemoryProfiler::statistics(
    const http::Request& request,
    const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  if (!detectJemalloc()) {
    return http::BadRequest(JEMALLOC_NOT_DETECTED_MESSAGE);
  }

  std::string options = "J";  // 'J' selects JSON output format.

  std::string statistics;
  ::malloc_stats_print([](void* opaque, const char* msg) {
      std::string* statistics = static_cast<std::string*>(opaque);
      *statistics += msg;
    }, &statistics, options.c_str());

  return http::OK(statistics, "application/json; charset=utf-8");
}


Future<http::Response> MemoryProfiler::state(
  const http::Request& request,
  const Option<http::authentication::Principal>&)
{
  if (!heapProfilingEnabled) {
    return http::BadRequest(HEAP_PROFILING_DISABLED_MESSAGE);
  }

  bool detected = detectJemalloc();

  // State unrelated to jemalloc
  JSON::Object profilerState;
  profilerState.values["jemalloc_detected"] = detected;
  profilerState.values["profiling_enabled"] = heapProfilingEnabled;
  profilerState.values["profiling_active"] = heapProfilingActive;

  Path tmpdir = temporaryDirectory.getOrElse(Path {"Not yet generated."});
  profilerState.values["tmpdir"] = tmpdir.string();

  JSON::Object state;
  state.values["memory_profiler"] = std::move(profilerState);

  if (!detected) {
    return http::OK(state);
  }

  // Holds malloc configuration from various sources.
  JSON::Object mallocConf;

  // User-specified malloc configuration that was added via the `MALLOC_CONF`
  // environment variable.
  mallocConf.values["environment"] = os::getenv("MALLOC_CONF").getOrElse("");

  // Compile-time malloc configuration that was added at build time via
  // the `--with-malloc-conf` flag.
  Try<const char*> builtinMallocConf = readJemallocSetting<const char*>(
      "config.malloc_conf");

  if (builtinMallocConf.isError()) {
    mallocConf.values["build"] = builtinMallocConf.error();
  } else {
    mallocConf.values["build"] = builtinMallocConf.get();
  }

  // TODO(bennoe): System-wide jemalloc settings can be specified by creating
  // a symlink at /etc/malloc.conf whose pointed-to value is read as an option
  // string. We should also display this here.

  // Application-specific jemalloc settings that were specified by defining
  // a global variable `malloc_conf`.
  mallocConf.values["application"] = (malloc_conf ? malloc_conf : "");

  // Holds relevant parts of the current jemalloc state.
  JSON::Object jemallocState;
  jemallocState.values["malloc_conf"] = std::move(mallocConf);

  // Whether jemalloc was compiled with support for heap profiling
  Try<bool> profilingSupported = readJemallocSetting<bool>("config.prof");

  if (profilingSupported.isError()) {
    jemallocState.values["profiling_enabled"] = profilingSupported.error();
  } else {
    jemallocState.values["profiling_enabled"] = profilingSupported.get();
  }

  // Whether profiling is currently active.
  Try<bool> profilingActive = readJemallocSetting<bool>("prof.active");

  if (profilingActive.isError()) {
    jemallocState.values["profiling_active"] = profilingActive.error();
  } else {
    jemallocState.values["profiling_active"] = profilingActive.get();
  }

  state.values["jemalloc"] = std::move(jemallocState);

  return http::OK(state);
}

} // namespace process {
