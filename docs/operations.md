# Operations

How this server is run and watched: its command line, what it writes, the
signals it answers, how its password is delivered and rotated, what running
without one means, and what `INFO` gives a monitor. Every claim here is
measured on the binary of the version it was written for, and a test
(`crates/seedstone-service/tests/operations_page.rs`) holds the table in
*Output* to the code, so a line cannot gain or lose a field without this page
saying so.

## Command line and environment

_Written with the password rotation._

## Output

Everything the server writes about itself goes to **stderr**, one JSON object
per line. Stdout carries only the answers to `--version` and `--help`. The
one exception is a refused command line — an unknown flag, a missing value,
a contradiction such as `--no-auth` beside a password — which is answered in
plain text with the usage, and exit code 2: that is the command line talking
back to whoever typed it, before a server exists.

Every line opens with the same three fields, in this order: `ts`, Unix time
in milliseconds; `level`, one of `info`, `warn` or `error`; and `evt`, the
event's name from the table below. The event's own fields follow. Two lines
as the server writes them:

```json
{"ts":1757800000000,"level":"info","evt":"listening","version":"0.1.1","bind":"0.0.0.0","port":6379}
{"ts":1757800000317,"level":"warn","evt":"error_reply","code":"ERR","cmd":"flushall","msg":"ERR unknown command 'flushall'"}
```

| Event | Level | Fields | When |
|---|---|---|---|
| `listening` | `info` | `version`, `bind`, `port` | the listener is bound; `bind` and `port` are what the kernel gave, as `CONFIG GET bind` reports them |
| `bind_failed` | `error` | `bind`, `port`, `error` | the address could not be bound; the process exits 1 after this line |
| `error_reply` | `warn` | `code`, `cmd`, `msg` | one error reply was sent to a client; `code` is the reply's first word, `cmd` the command it answered |
| `stopping` | `info` | `signal` | the server is leaving on `SIGTERM` or `SIGINT` |
| `password_reloaded` | `info` | `passwords` | `SIGHUP` re-read the password file; `passwords` is how many lines it holds now, 1 or 2 |
| `password_reload_failed` | `error` | `error` | `SIGHUP` re-read the password file and refused it; the previous passwords stay in force |
| `password_reload_skipped` | `warn` | — | `SIGHUP` arrived, but the password came from the environment or there is none, so there was nothing to re-read |

`error_reply` is `warn` and not `error` on purpose: an `ERR unknown command`
is the client's mistake or the deployment's, and the server that reported it
is healthy. An alert on `level:error` therefore fires for the server's own
failures and not for a client's.

**What is promised.** The three common fields and the event names above are
stable: removing or renaming one is a breaking change, recorded under
*Changed* in the changelog and released as a minor version while the project
is `0.x`. A new field may appear on any line, and a new event may appear, in
any version, without notice — a consumer selects lines by `evt` and reads
fields by name, and does not assume either list is closed. The text of `msg`
and `error` is for people and is not an interface: it may change without
notice, and nothing should match on it.

## Signals

_Written with the password rotation._

## Password and rotation

_Written with the password rotation._

## Running without a password

_Written with the password rotation._

## What `INFO` gives a monitor

_Written with the password rotation._
