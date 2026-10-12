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
where to put it.

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

The same image is on Docker Hub as `<namespace>/seedstone:<tag>`.

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
