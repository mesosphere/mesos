---
title: Apache Mesos - Memory Profiling
layout: documentation
---

# Memory profiling with Mesos and Jemalloc

Libprocess comes with some built-in support for profiling the memory usage
of a running process which relies on the capabilities of the jemalloc
memory allocator.

This relies on the facilities provided by jemalloc. It works by detecting, at
run-time, whether the current process is using jemalloc as its memory allocator,
and if so provides a number of HTTP endpoints that allow operators to
start and stop the collection of profiling data at runtime.

The profiling done by jemalloc works by sampling all calls to `malloc()`
according to a configured probability distribution, and storing stack
traces for the sampled calls in a separate memory area. These can then
be dumped into files on the filesystem, so-called heap profiles.


# Requirements

There are two ways to enjoy the memory profiling features of libprocess.

The easiest, "batteries-included" method is to specify the `--enable-jemalloc`
compile time flag, which causes the `mesos-master` and `mesos-agent` binaries to
be statically linked against a bundled version of jemalloc will be
compiled with the correct flags.

If a suitable version of jemalloc is already present, it is of course also
possible to use that instead by pointing `--with-jemalloc=` to it, where
suitable means that it should have been built with the `--enable-stats`
and `--enable-prof` flags.

The second way is to use the `LD_PRELOAD` mechanism to preload
a `libjemalloc.so` shared library that is present on the system at run-time.
The MemoryProfiler class in libprocess will automatically detect this and
enable its memory profiling support.

By default, the generated profile dumps will be written to a random directory
under `/tmp`, except when the `TMPDIR` environment variable points to another
directory.


# Usage

If mesos was configured with the `--enable-default-jemalloc` option, all
that is required to start a profiling run is to


## Memory statistics

The `/statistics` endpoint returns general information about the memory usage
in JSON format, for example the number of bytes currently allocated and the
size distribution of these allocations.

        http://example.org:5050/memory-profiler/statistics


## Heap profiling

It is also possible to profile , for example:

        http://example.org:5050/memory-profiler/start?duration=5mins

The profile collection can be stopped again by hitting the `/stop`
endpoint:

        http://example.org:5050/memory-profiler/stop

To analyze the generated profiling data, the results are offered in three
different formats.

  1) Raw profile

        http://example.org:5050/memory-profiler/download/raw

     This returns a file in a plain text format containing the raw backtraces
     collected , i.e. lists of memory addresses. It can be interactively analyzed
     and rendered using the `jeprof` tool provided by the jemalloc project.
     For more information on this file format, see also

       http://jemalloc.net/jemalloc.3.html#heap_profile_format

  2) Symbolized profile

        http://example.org:5050/memory-profiler/download/text

     This is similar to the raw format above, except that jeprof is called on
     the host machine to attempt to read debug information

     Usage of this endpoint requires that jeprof is present on the host machine
     and on the `PATH`, and no useful information will be generated unless the
     binary contains symbol information.

  2) Call graph

        http://example.org:5050/memory-profiler/download/graph

     This endpoint returns an image in svg format that shows a graphical
     representation of the samples backtraces.

     Usage of this endpoint requires that jeprof is present on the host machine
     and on the `PATH` of mesos, and no useful information will be generated
     unless the binary contains symbol information.


Which of these is needed will depend on the circumstances of the application
deployment and of the bug that is investigated:

In many debian-like environments, symbols information is stripped by default
to save space, and shipped in separate packages. In such an environment, only
the raw profile format will be usable.

The call graph presents information in the most immediately useful form,
but is difficult to filter and post-process if non-default output options
are desired.


# Additional sampling modes

The jemalloc allocator provides more profiling options and settings that are
not exposed by libprocess.

For example, this includes an option to generate heap profiles automatically
after a certain amount of memory has been allocated, or whenever memory usage
reaches a new high-water mark.

However, it is still possible to use these features by restarting the process
while adding the desired settings into the `MALLOC_CONF` environment variable,
for example

    MALLOC_CONF="prof:true,prof_prefix:/path/to/folder,lg_prof_interval=20"


# Disabling memory profiling

All features described in this document can be disabled by specifying
the `--memory_profiling=disabled` flag or by setting the corresponding
environment variable `LIBPROCESS_MEMORY_PROFILING=disabled` to disable all
features described in this document.
