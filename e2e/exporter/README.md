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
3. **The exporter logged no error.** An exporter logs every command the
   server refused, so an empty error log is what "without adjustment" means
   mechanically.

## The pin

The container is selected by its **manifest-list digest** — the digest of the
multi-architecture index, not of one platform's image — because this project
is developed on one architecture and gated on another, and a platform digest
would pin the lane to whichever one resolved it. The tag is recorded beside
the digest so a human can read which release it is; the digest is what runs.

`expected-errors.txt` lists error lines the exporter is expected to log today,
one per line, matched as substrings. It exists so the lane can be red-honest
without being red: an error listed there is tolerated, an error not listed
fails the lane, and a listed error that stops appearing fails the lane until
its line is removed. The lane's gate is that file being empty.

## What the error log does not say

The exporter does not log every command it fails to get answered. It prints a
`WARNING, LOGGED ONCE ONLY` line for some, and silently drops the metrics that
depended on the rest — a scrape can therefore be missing whole families of
metrics while the log stays quiet. That is why the second assertion exists and
why it names the metrics it wants: the log catches the refusals the exporter
chooses to report, and the named values catch the ones it does not.
