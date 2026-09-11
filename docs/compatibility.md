# Compatibility

What this server answers, what it refuses, and where it deliberately differs
from Redis. Every claim about Redis on this page names the version it was read
against — `6.2.24` and `8.10.1`, the two the project measures with — and a test
(`crates/seedstone-service/tests/compatibility_page.rs`) keeps the first two
tables in step with what `COMMAND` reports, so a command cannot be added or
removed without this page saying so.

The surface is a named list chosen for the workloads this project targets: a
Django application's cache backend and the clients behind it. Anything outside
the list is refused with an error naming the command, never answered
approximately.

## What it answers

| Command | Differs from Redis |
|---|---|
| `GET key` | — |
| `SET key value [EX seconds \| PX milliseconds \| EXAT unix-seconds \| PXAT unix-milliseconds \| KEEPTTL] [NX \| XX] [GET]` | — |
| `SETEX key seconds value` | — |
| `SETNX key value` | — |
| `PSETEX key milliseconds value` | — |
| `MGET key [key …]` | — |
| `INCRBY key increment` | — |
| `STRLEN key` | — |
| `TYPE key` | answers `string` or `none`, the only two a keyspace of strings has |
| `DEL key [key …]` | — |
| `EXISTS key [key …]` | — |
| `EXPIRE key seconds` | no `NX`/`XX`/`GT`/`LT`. The option form is refused as a wrong number of arguments, in the same words 6.2.24 refuses it in — the four options are 8.10.1's, and it applies all four |
| `PEXPIRE key milliseconds` | as `EXPIRE` |
| `EXPIREAT key unix-seconds` | as `EXPIRE`. A moment already past deletes the key and answers `1`; a value whose multiplication by a thousand leaves a signed 64-bit millisecond clock is an invalid expire time, so the command is bounded at both ends — which is what 6.2.24 and 8.10.1 do, down to the boundary itself |
| `PEXPIREAT key unix-milliseconds` | as `EXPIRE`. No bound below `i64::MAX` or above `i64::MIN`, because a millisecond count is never multiplied — the same asymmetry with `EXPIREAT` that 6.2.24 and 8.10.1 have |
| `TTL key` | — |
| `PTTL key` | — |
| `PERSIST key` | — |
| `DBSIZE` | — |
| `KEYS pattern` | a walk is at-least-once rather than a snapshot, and a reply above a per-request size ceiling is refused with `ERR KEYS reply exceeds the per-request limit; use SCAN` rather than truncated. Both are below |
| `SCAN cursor [MATCH pattern] [COUNT count]` | no `TYPE` option, which 6.2.24 and 8.10.1 both take: it is refused with `ERR syntax error`. The walk is at-least-once, and the cursor is one this server issued, accepted only in the decimal it was issued in |
| `FLUSHDB` | no `ASYNC` or `SYNC`, which 6.2.24 and 8.10.1 both accept. There is one behaviour here, so the word is refused as a wrong number of arguments rather than accepted and ignored |
| `PING [message]` | — |
| `ECHO message` | — |
| `AUTH [username] password` | one password for the `default` user; no ACL users |
| `HELLO [protover [AUTH username password]]` | RESP2 only: `HELLO 3` is `NOPROTO unsupported protocol version`. `SETNAME` is refused, where 6.2.24 and 8.10.1 accept it, because there is no client name to set here. A refused version or option is answered before the connection's authentication is considered — the order 6.2.24 and 8.10.1 decide it in — so an unauthenticated client is told which of the two it got wrong |
| `QUIT` | — |
| `CLIENT SETNAME \| SETINFO` | both answered `OK` and both ignored: there is no `CLIENT LIST` here to show a name in. redis-py 8.1.0 and go-redis 9.7.3 send `SETINFO` while establishing a connection |
| `INFO [section]` | sections `server`, `clients`, `memory`, `stats`, `keyspace` and `commandstats`, each carrying only the fields this node can state truthfully. `memory` is `used_memory`, `used_memory_human`, `maxmemory`, `maxmemory_human` and `maxmemory_policy`, and no resident-set family — see below |
| `CONFIG GET parameter [parameter …]` | a fixed table of the parameters that describe how the node was started, matched without regard to case; `CONFIG SET` is refused. See below for which spelling comes back |
| `SLOWLOG GET \| LEN \| RESET` | answers as a monitor that is switched off: an empty list, `0`, `OK` |
| `LATENCY LATEST \| HISTORY \| RESET \| HISTOGRAM` | as `SLOWLOG`, and for the same reason: the empty answers of a monitor that is off, so a scrape completes instead of logging a refusal every pass |
| `COMMAND [COUNT \| DOCS]` | `COUNT` is the length of the server's own command table rather than a literal beside it, and the test that guards this page holds that table and the rows above together. `DOCS` answers an empty array, there being nothing to say about any of these; `redis-cli` 8.10.0 asks for it when it starts interactively, to offer hints |

Every expiry span — `SET … EX`/`PX`, `SETEX`, `PSETEX`, `EXPIRE`, `PEXPIRE` —
is bounded by the clock rather than by a constant: `now` plus the span must fit
a signed 64-bit millisecond clock. That is the bound 6.2.24 and 8.10.1 apply,
so the spans refused here are the spans they refuse, and the boundary moves by
one every second. An ordinary span never reads the clock to be judged.

## What it refuses

Each with the reply a client receives, read from this server on the day this
page was written. One shape covers all of them — `ERR unknown command
'<NAME>'`, with the name in the case the client sent it, and nothing after it.

| Command | Reply | Why |
|---|---|---|
| `EVAL`, `EVALSHA`, `SCRIPT` | `ERR unknown command 'EVAL'` | server-side scripting is not planned before the server persists data. `SCRIPT LOAD` is refused on `SCRIPT`, the container command, not on the subcommand |
| `MULTI`, `EXEC` | `ERR unknown command 'MULTI'` | transactions, for the same reason |
| `SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SREM`, `SSCAN`, `SUNION`, `SUNIONSTORE` | `ERR unknown command 'SADD'` | no set type: the keyspace holds strings |
| `HSET`, `HDEL`, `HEXISTS`, `HKEYS`, `HLEN` | `ERR unknown command 'HSET'` | no hash type, as above |
| `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZPOPMAX`, `ZPOPMIN`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZSCORE` | `ERR unknown command 'ZADD'` | no sorted-set type, as above |
| `SELECT` | `ERR unknown command 'SELECT'` | one database. A client configured with `db=0` never sends it |
| `UNLINK`, `TOUCH`, `GETDEL`, `GETEX` | `ERR unknown command 'UNLINK'` | outside the named list. The list is what the workloads this server targets put on the wire, and a name outside it is refused rather than answered approximately |

## Where it deliberately differs

- **RESP2 only.** `HELLO 3` is answered `NOPROTO unsupported protocol version`.
  redis-py 8.1.0 opens every connection with `HELLO 3` and does not fall back,
  so configure it with `protocol=2`; go-redis 9.7.3 sends the same handshake
  and falls back on its own when it is refused. redis-py 5.0.8 and earlier
  default to RESP2 and send no `HELLO` at all.

- **A keyspace walk is at-least-once, not a snapshot.** `KEYS` and `SCAN`
  answer with a set that was the keyspace at no single instant: a key created
  while a walk is in flight may be missed, and a key deleted while it is in
  flight may still appear. The keyspace here is spread across shards that no
  lock spans, and a global instant would cost a barrier that every single-key
  command would pay for. `KEYS` removes the duplicates that follow, so it never
  reports a key twice; `SCAN` may, which is what a `SCAN` loop tolerates on any
  server. `KEYS` does not block the server and is still `O(keyspace)` — the
  reasoning, and what a `SCAN` cursor means here, are in
  [ARCHITECTURE.md](ARCHITECTURE.md).

- **`INFO memory` has no `used_memory_rss`, `used_memory_peak` or
  `mem_fragmentation_ratio`.** `used_memory` is an accounting formula over the
  keyspace, not an allocator reading, and this server does not read its
  resident set size — so there is nothing to divide by. 6.2.24 and 8.10.1
  compute the ratio as `used_memory_rss / used_memory`, and on idle containers
  it read 14.57 and 14.89 respectively, nowhere near 1: a field this server
  could only fill with a constant is absent rather than approximated.

- **`CONFIG` has `GET` and no `SET`.** Every parameter it reports is a fact
  fixed at startup, and accepting a new one at runtime would mean moving a
  keyspace under it. A parameter name is matched without regard to case, and
  the reply names it in this server's own lower-case spelling whatever case was
  asked for — which is what 6.2.24 does. 8.10.1 echoes the spelling the client
  used when the request named a parameter exactly, and its own when the request
  was a glob.

- **Authentication** is one password for the `default` user, sent as `AUTH
  password`, `AUTH default password` or `HELLO 2 AUTH default password`. There
  are no ACL users. A `HELLO` that names a version this server does not speak,
  or an option it does not take, is refused with its own error before the
  connection's authentication is considered — the order 6.2.24 and 8.10.1
  decide it in. The `NOAUTH` sentence a bare unauthenticated `HELLO 2` gets is
  6.2.24's wording; 8.10.1 spells the same refusal with `the HELLO <proto> AUTH
  <user> <pass> option`.
