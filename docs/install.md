# Installing

Every way to get the server, each ending in the same ten-second check. The
binary is the same on every channel; what differs is who starts it and
where it keeps its data. Every flag it takes is in
[operations.md](operations.md); what it answers is in
[compatibility.md](compatibility.md).

The packaged defaults, wherever a package starts the server for you, are
the server's own: it listens on loopback, takes no password, and keeps its
log under a directory the package owns. Anything else — a bind outside
loopback, a password, a memory ceiling — is a flag, and the package says
where to put it. The Helm chart is the one exception: on Kubernetes the
server must listen beyond loopback to be reached at all, so the chart binds
every interface and asks for a password, or for its absence, said
deliberately.

## Container image

```sh
docker run -p 6379:6379 ghcr.io/stefanobaldo/seedstone:<tag> --bind 0.0.0.0:6379 --no-auth
```

The image holds nothing but the binary and runs as a non-root user (uid
65532), so the options above are the container's arguments.
`--bind 0.0.0.0:6379` is what makes the published port reachable;
`--no-auth` says, deliberately, that no password guards it. For a
password, pass `SEEDSTONE_REQUIREPASS` in the environment instead of
`--no-auth`: the server refuses both together.

For persistence, give `--data-dir` a directory that user can write. A new
named volume is owned by root, so hand it over once before the first start:

```sh
docker volume create seedstone-data
docker run --rm -v seedstone-data:/data busybox chown 65532:65532 /data
docker run -p 6379:6379 -v seedstone-data:/data ghcr.io/stefanobaldo/seedstone:<tag> \
  --bind 0.0.0.0:6379 --no-auth --data-dir /data
```

The image is built for `linux/amd64` and `linux/arm64` under one tag. A
deployment pins the digest of that index, which
`docker buildx imagetools inspect ghcr.io/stefanobaldo/seedstone:<tag>`
prints.

The same image is on Docker Hub as `<namespace>/seedstone:<tag>` for every
final release; a release candidate is on GHCR only.

## Homebrew (macOS and Linux)

```sh
brew install seedstonelabs/tap/seedstone
seedstone --version
brew services start seedstone      # as a service on macOS: loopback, no password
```

The formula installs the release archive for the machine (Apple silicon
or Intel; Linux x86_64 or aarch64) and verifies its checksum; nothing is
compiled. On macOS, `brew services` runs the server with `--data-dir
$(brew --prefix)/var/seedstone`, and its output goes to
`$(brew --prefix)/var/log/seedstone.log`. A password or any other flag
means running the binary yourself. The tap carries final releases only.

## Cargo

```sh
cargo binstall seedstone        # the release archive for this machine, in seconds
cargo install seedstone         # from source: a release build, minutes
seedstone --version
```

`cargo binstall` (from the `cargo-binstall` crate) downloads, over HTTPS,
the archive a release carries for the host; `cargo install`
compiles the crate and its three library crates from crates.io with the
toolchain on the machine. Both put the binary under `~/.cargo/bin`. The
library crates — `seedstone-core`, `seedstone-resp`, `seedstone-service` —
are published because the server is; their APIs carry no stability promise.

## Debian, Ubuntu (`.deb`) and Fedora, RHEL (`.rpm`)

A release carries a package for each family and each architecture. It
installs the binary at `/usr/bin/seedstone` and a systemd unit, and starts
the service:

```sh
sudo apt install ./seedstone_<version>-1_<arch>.deb        # Debian, Ubuntu
sudo dnf install ./seedstone-<version>-1.<arch>.rpm        # Fedora, RHEL
systemctl status seedstone
```

`<arch>` is `amd64` or `arm64` in a `.deb`'s name and `x86_64` or
`aarch64` in an `.rpm`'s.

The service runs as a dynamic user, listens on `127.0.0.1:6379` with no
password, keeps its log under `/var/lib/seedstone`, and writes its events
to the journal (`journalctl -u seedstone`). A password and any further
flag go in the environment file — `/etc/default/seedstone` on Debian,
`/etc/sysconfig/seedstone` on RPM systems — followed by
`systemctl restart seedstone`; the file's comments say which variable is
which. A flag given twice takes its last value, so a `--bind` there
replaces the unit's. The service can write nowhere but its own directory:
to move the data directory, override the unit (`systemctl edit seedstone`).
The environment file is readable by root alone, so a password there is not
visible to other users on the host.

An upgrade restarts the service if it is running and leaves it alone if it
is not; a service you disabled stays disabled. Removing the package stops
the service and keeps the environment file and the data; purging a `.deb`
removes the environment file too, and still keeps the data.

The packages need glibc 2.34 or newer, as the archives do.

## Archives

A release carries four archives, one per platform, each with its `sha256`:

| platform | archive |
|---|---|
| Linux x86_64 (glibc) | `seedstone-<version>-x86_64-unknown-linux-gnu.tar.gz` |
| Linux aarch64 (glibc) | `seedstone-<version>-aarch64-unknown-linux-gnu.tar.gz` |
| macOS Apple silicon | `seedstone-<version>-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `seedstone-<version>-x86_64-apple-darwin.tar.gz` |

```sh
v=<version>; t=<target>
curl -fsSLO "https://github.com/stefanobaldo/seedstone/releases/download/v$v/seedstone-$v-$t.tar.gz"
curl -fsSLO "https://github.com/stefanobaldo/seedstone/releases/download/v$v/seedstone-$v-$t.tar.gz.sha256"
sha256sum -c "seedstone-$v-$t.tar.gz.sha256"      # shasum -a 256 -c on macOS
tar -xzf "seedstone-$v-$t.tar.gz"
"./seedstone-$v-$t/seedstone" --version
```

The Linux archives need glibc 2.34 or newer: Ubuntu 22.04, Debian 12 and
the distributions released since.

## Kubernetes (Helm)

```sh
kubectl create secret generic seedstone-auth --from-literal=password='change-me'
helm install seedstone oci://ghcr.io/stefanobaldo/charts/seedstone --version <version> \
  --set auth.existingSecret=seedstone-auth
```

One node, a StatefulSet with a volume for `--data-dir` (`persistence.size`,
`persistence.storageClass`; `persistence.enabled=false` for a cache that
may start empty), the password from a Secret you own, TCP probes on the
port. `auth.noAuth=true` runs without a password, deliberately. The image
is GHCR's, at the tag the chart was released with; `image.digest` pins it.
The chart's version is the server's. Every value is in the chart's
[README](../charts/seedstone/README.md); the ten-second check runs from a
client pod, as `helm install` prints.

## The ten-second check

With the server up on the default port:

```console
$ redis-cli
127.0.0.1:6379> set greeting hello ex 60
OK
127.0.0.1:6379> ttl greeting
(integer) 60
127.0.0.1:6379> get greeting
"hello"
```
