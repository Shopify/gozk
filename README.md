# gozk - Go zookeeper library via C bindings

Forked from https://github.com/youtube/vitess/tree/master/third_party/go/launchpad.net/gozk/zookeeper

## Requirements

- zookeeper c library headers: when installed via homebrew you'll find the includes under `/opt/homebrew/include` and the shared libraries in `/opt/homebrew/lib/`. Other
  systems such as Linux use different paths but are likely to place them under `/usr/include` and `/usr/lib`.
- C compiler

The default flags defined in zk.go **should be sufficient** for builds on Linux or MacOSX. But If you zookeeper includes and shared libraries are not in the default
search locations (`/usr/include` and `/usr/lib`) you'll have to set environment variables to point the compiler at the right path. For example when using homebrew,
you'll need to specify:

```
export CGO_CFLAGS='-I/opt/homebrew/include/zookeeper'
export CGO_LDFLAGS='-L/opt/homebrew/lib'
```

On Debian Linux, used by the official go docker container, you'll needthe following libraries:

* `libzookeeper-mt-dev`: header files
* `libzookeeper-mt2`: shared libraries

Set the `CGO` flags:

```
export CGO_CFLAGS='-I/usr/include/zookeeper'
export CGO_LDFLAGS='-L/usr/lib/aarch64-linux-gnu'
```

Then:

```
go build
```

### Build Errors

#### `zookeeper.h` not found
Error message looks like this:
```
./zk.go:21:11: fatal error: zookeeper.h: No such file or directory
   21 | // #include <zookeeper.h>
      |           ^~~~~~~~~~~~~
compilation terminated.
```

The include search path does not include the zookeeper includes. Check that `CGO_CFLAGS=-I` is set to a directory that contains the zookeeper includes.

#### `ld: library not found`
Error message:
```
ld: library not found for -lzookeeper_mt
collect2: error: ld returned 1 exit status
```

The linker can't find the shared library. Ensure that `CGO_LDFLAGS=-L` points to a directory that contains the zookeeper libraries.

## Historic Notes

Check out https://wiki.ubuntu.com/gozk

Forked from https://github.com/youtube/vitess/tree/master/third_party/go/launchpad.net/gozk/zookeeper
to fix panic issues in some of the watch functions.
