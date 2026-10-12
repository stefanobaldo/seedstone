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
`--no-auth` says, deliberately, that no password guards it — pass
`SEEDSTONE_REQUIREPASS` in the environment for one.

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
