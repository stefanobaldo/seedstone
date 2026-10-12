# seedstone Helm chart

One seedstone node on Kubernetes: a StatefulSet with one replica, a Service,
and an optional volume for `--data-dir`.

```sh
kubectl create secret generic seedstone-auth --from-literal=password='change-me'
helm install seedstone oci://ghcr.io/stefanobaldo/charts/seedstone --version <version> \
  --set auth.existingSecret=seedstone-auth
```

The chart's version is the server's: chart `0.3.3` runs image `v0.3.3`.

## The password

The server refuses to listen outside loopback with no password, and so does
the chart: it fails to render until exactly one of these is set.

- `auth.existingSecret` — a Secret you own; the password is its key
  `auth.secretKey`. The way to run it.
- `auth.password` — written in the values; the chart puts it in a Secret it
  creates. It is then in the release's values too, readable by anyone who can
  read the release.
- `auth.noAuth=true` — no password, said deliberately: every client that can
  reach the Service can use the server, `SHUTDOWN` included.

## The server does not replicate

`replicas` is not a value. The StatefulSet holds one pod and exists for the
stable volume, not for scale; a second replica would be a second, unrelated
keyspace behind the same Service.

## Values

| Key | Default | |
|---|---|---|
| `image.repository` | `ghcr.io/stefanobaldo/seedstone` | |
| `image.tag` | `""` | empty: `v` + the chart's `appVersion` |
| `image.digest` | `""` | when set, the image is pinned by digest and `tag` is ignored |
| `image.pullPolicy` | `IfNotPresent` | |
| `service.type` | `ClusterIP` | |
| `service.port` | `6379` | the port the server binds and the Service exposes |
| `auth.existingSecret` | `""` | Secret holding the password |
| `auth.secretKey` | `password` | key of the password in that Secret |
| `auth.password` | `""` | password for a Secret the chart creates |
| `auth.noAuth` | `false` | run with `--no-auth` |
| `persistence.enabled` | `true` | `false`: no volume, no `--data-dir`, a restart is an empty keyspace |
| `persistence.size` | `8Gi` | |
| `persistence.storageClass` | `""` | empty: the cluster's default |
| `persistence.mountPath` | `/data` | where the volume is mounted and `--data-dir` points |
| `fsync` | `interval` | `always`, `interval` or `never`; applies with persistence only |
| `startupTimeoutSeconds` | `600` | how long a start, log replay included, may take before the pod is restarted |
| `extraArgs` | `[]` | further flags, verbatim, e.g. `["--maxmemory", "2gb"]` |
| `resources` | `{}` | |
| `nodeSelector` | `{}` | |
| `tolerations` | `[]` | |
| `affinity` | `{}` | |
| `podAnnotations` | `{}` | |

Startup, readiness and liveness are TCP probes on the port: the image has no
shell and no client to run a command with. The server listens only once it
has replayed its log, so the startup probe gives it `startupTimeoutSeconds`
before liveness takes over.
