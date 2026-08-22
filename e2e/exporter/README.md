# The exporter lane

Everything else in `e2e/` is a client this project chose. This lane is the
other kind of third party: a metrics exporter the operators of a deployment
already run, pointed at this server with no flag it would not be given for
Redis, and held to one standard — it must scrape cleanly.

`run.sh` starts the server with a password and a memory ceiling, writes a
small keyspace, runs the exporter in a container and scrapes it once.
`check.sh` then asserts three things:

1. `redis_up 1` — the exporter reached the server and read `INFO`.
2. A named list of metrics is present with the values this lane can predict
   from what it wrote and how it started the server.
3. **The exporter logged no error.** A command this server refuses is
   something an exporter reports, so an empty error log is one half of what
   "without adjustment" means mechanically. Only one half: it does not report
   every one of them, which is why the second assertion exists beside this —
   see "What the error log does not say" below.

## The password is a literal

The server this lane starts requires one, and the exporter authenticates with
it the way it would against a Redis deployment with a `requirepass` — which is
the configuration these metrics are usually scraped from. **The password is
`lane-password` and it protects nothing**: it is written beside the lane when
CI has not handed one over.

`--redis.password-file` is not a file holding a password. It reads a JSON
object mapping each `--redis.addr` the exporter was given to that server's
password, and a file holding the bare password is refused at startup with
`password file format error`. The lane therefore writes that map itself, keyed
by the address it is about to pass, and derives it from the same file the
server is started with — so the two secrets cannot drift apart.

## The pin

The container is selected by its **manifest-list digest** — the digest of the
multi-architecture index, not of one platform's image — because this project
is developed on one architecture and gated on another, and a platform digest
would pin the lane to whichever one resolved it. The tag is recorded beside
the digest so a human can read which release it is; the digest is what runs.

`expected-errors.txt` lists error lines the exporter is tolerated in logging,
one per line, matched as substrings. It exists so the lane can be red-honest
without being red: an error listed there is tolerated, an error not listed
fails the lane, and a listed error that stops appearing fails the lane until
its line is removed. **It is empty**, which is the lane's gate — the exporter
asks this server for nothing it will not answer — and it is what activates the
value assertions in `check.sh`, which are inert while anything is tolerated.

## What the error log does not say

The exporter does not log every command it fails to get answered. It prints a
`WARNING, LOGGED ONCE ONLY` line for some, and silently drops the metrics that
depended on the rest — a scrape can therefore be missing whole families of
metrics while the log stays quiet. That is why the second assertion exists and
why it names the metrics it wants: the log catches the refusals the exporter
chooses to report, and the named values catch the ones it does not.

One family is missing on purpose. This exporter reads a `cmdstat_` line by
position rather than by name: it drops any line carrying fewer than three
comma-separated fields, and otherwise takes whatever sits in the second field
as microseconds, whatever that field is called. `INFO commandstats` here
carries a call count and nothing else, because nothing in this server times a
handler — so the line is dropped, and `redis_commands_total` never appears.
Padding it to three fields would make the exporter publish a
`redis_commands_duration_seconds_total` derived from a number that is not a
duration, which is a worse answer than none. `check.sh` asserts that metric is
**absent**, so the day someone pads the line, this lane says so.
