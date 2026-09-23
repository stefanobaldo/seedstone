# Benchmarks

**Release measured:** `v0.2.0` (commit `ee844c1`). **Date:** 2026-09-23.
**Machine:** GCP `c4a-standard-16`. **Engines:** Redis 8.10.0, Valkey 9.1.1,
Dragonfly `df-v1.40.2`, Garnet 2.1.5 — each as the engine itself reports its
version, not as its release archive is named. **Warm-up runs discarded per arm
per cell (`W`):** 3, derived by the calibration below.

The method below was committed before the run; the tables were added after
it. The raw logs the tables are computed from are in
[`bench/results/v0.2.0/`](../bench/results/v0.2.0/), and
`python3 bench/report.py bench/results/v0.2.0/0[3-7]-*.log` regenerates every
table and every pair line on this page. The only readings it does not produce
are the `v0.1.0` ones quoted under the pairs whose word changed; the same
command over that run's logs produces those. A number here without its method
beside it would be a marketing number, so the method comes first.

**The run these tables replace is still here.** The `v0.1.0` run's raw logs stay
under [`bench/results/v0.1.0/`](../bench/results/v0.1.0/), the same command over
them regenerates that run's tables unchanged, and wherever a pair's reading
changed between the two runs, a line under that pair says what it read then.

## What was measured, and on what

**The machine.** One GCP `c4a-standard-16`: Google Axion (ARM Neoverse-V2),
16 physical cores with one thread each, a single NUMA node, 62.7 GiB of
memory. Ubuntu 24.04. The distribution, the memory and the kernel version are
echoed at the head of every log, so the three facts in this paragraph can be
read off the logs rather than taken on trust. The server under test is pinned
to ten cores (`0-9`) and the load generator to the other six (`10-15`), and the
two talk over loopback. Nothing else runs on the machine.

**The load generator.** `redis-benchmark`, from the Redis release measured
(8.10.0), is the only client. Its own CPU is on every row of every table, so a
row where the client and not the server was the bottleneck can be seen rather
than suspected. Its highest reading across the 225 kept runs behind the tables
below is 1.02 cores, on one of them; no row of any table, each being a median of
three, reaches above 1.01. If a re-run shows more, that row is about the
client.

**The engines, and how each was configured.** Each engine receives the
configuration that matches the hardware it is given, where it has a knob for
that, and nothing else — no allocator, hugepage or affinity tuning, for any
of them, this server included.

| arm | version and provenance | started as |
|---|---|---|
| seedstone | `v0.2.0`, built on the machine with `cargo build --release --locked -p seedstone` | `--bind 127.0.0.1:6390 --max-clients 2000 --no-auth` |
| redis, `io-threads 1` | 8.10.0, built from the source release | `--save '' --appendonly no --io-threads 1` |
| redis, `io-threads 4` | the same binary | `--save '' --appendonly no --io-threads 4` |
| valkey, `io-threads 1` | 9.1.1, built from the source release | `--save '' --appendonly no --io-threads 1` |
| valkey, `io-threads 4` | the same binary | `--save '' --appendonly no --io-threads 4` |
| dragonfly | `df-v1.40.2`, the `aarch64` release archive, sha256 verified against the release | `--proactor_threads=10 --dbfilename= --logtostderr=false` |
| garnet | 2.1.5, the `linux-arm64` release archive, sha256 verified against the release | defaults |

Redis and Valkey are measured at their default of one I/O thread and at four,
the value their documentation cites for multicore machines; publishing only
the default would compare against a configuration neither project recommends
for this hardware. Dragonfly gets one proactor per core of the server's
cpuset, which is how it documents its own deployment and the analogue of this
server's one executor per core; left at its default it would size itself from
whatever CPU count it detects under the pin. Garnet sizes its threads itself.
Nobody from any of these projects tuned their engine for this run.

Redis and Valkey are the primary comparison, because Redis is the baseline
this server measures itself against. Dragonfly and Garnet follow under *Other
engines*. KeyDB is absent because it has had no release since 2023 and adds
nothing Valkey at four I/O threads does not show; Redict because it is the
single-threaded Redis 7.2 architecture, which Redis at one I/O thread already
represents.

**Why these shapes.** The cells are sized from a production deployment this
server serves: a Django cache behind django-redis, holding rendered pages and
JSON, values around ten kilobytes, a keyspace that carries no TTL and is held
only by a memory ceiling with LRU eviction, a client that does not pipeline,
on the order of ninety million commands a week. Pipeline depth 1 is therefore
the regime that deployment actually runs; depth 64 is where a
message-passing design has something to amortise its overhead against. The
10 KB writes under a ceiling are that deployment's write path. The multi-key
read is here because it was the shape this server read furthest behind Redis
on when these tables were first published, and it is kept so that the same
cell can be read again. The `KEYS` walk is the newest cell and the only one
whose cost is set by the size of the keyspace rather than by the size of a
value: it is the shape a prefix invalidation takes against that cache.

## How

**One run** is a million operations of `redis-benchmark` against a server
already up, with 50 connections, keys spread uniformly over 100 000 keys
(`-r 100000`), and the server's CPU read once from `/proc/<pid>/stat`
immediately before and after — user and system time over every thread, at the
kernel's clock tick. Read once around a million operations the window is
thousands of ticks wide; read around a single operation it would be quantised
to nothing. `bench/cell.sh` is one run. The `KEYS` cell is the one exception to
the count and the key distribution, and states its own on the section below.

**Populated, and declared.** Before every read cell the keyspace is written
by the same step (`-t set -n 300000 -c 50 -P 64 -d 64 -r 100000`) and probed
by reading keys back; the probe's hit count is in the log. A `GET` against an
empty keyspace measures the miss path, which is a different and faster path.
Two cells are the exception and each says so: the eviction cell starts empty
and fills past its ceiling, and the `KEYS` cell is loaded by
`bench/keys-load.sh` with a keyspace that is a function of nothing but its own
index, so every arm and every run walks the identical 7 000 keys.

**Spread keys, and declared.** Every row states its key distribution. A load
aimed at a single key exercises one shard task of this server and would
report a number about the harness, not the server.

**Warm-ups, calibrated rather than chosen.** Before the cells, every arm runs
the reference shape (GET, 64 B, depth 64) twelve times. Per arm, the first
run `i` such that runs `i`, `i+1`, `i+2` lie within 2 % of each other marks
where it settles; the arm needs `i − 1` discarded runs. `W` is the largest of
those across all arms, applied uniformly to every cell. `W` for this run is
in the calibration log and at the head of every table. Discarded runs are
printed in the logs, not hidden.

**Three kept runs, medians per column.** Throughput, user, system and total
CPU per operation are each the median of their own three readings, so a row's
user and system need not sum to its total.

**One server at a time**, on its own lifetime per cell. No idle wait between
arms; the one-minute load average is printed before every start instead.

**The canary.** Before anything else, Redis at one I/O thread runs the
reference shape, and its median is compared with the figure the same
configuration produced on the reference machine when this baseline was first
measured — 2 551 021 operations per second — with a tolerance of ±5 %. A run
whose canary lands outside that interval was not made on a comparable machine,
and
its figures are not comparable to the tables below. The harness checks this
itself and stops.

**The harness checked against its predecessor.** `bench/cell.sh` is a
rewrite of the script that produced the canary's reference figure. Before the
first run published under this method it was measured against that script on
one Redis lifetime, the two alternating, and agreed within 2 % on throughput
and on CPU per operation. The new instrument measures what the old one
measured.

**What was discarded, and where it is.** The calibration runs (all of them),
the warm-up runs (`W` per arm per cell), and the fill that drives the
eviction cell past its ceiling. Every one is in the raw logs, marked.

## The tables

Every table and every pair line below is the output of

```sh
python3 bench/report.py bench/results/v0.2.0/0[3-7]-*.log
```

over the raw logs in [`bench/results/v0.2.0/`](../bench/results/v0.2.0/),
spliced into this page unedited. `W` is **3** for every cell in this run,
derived by the calibration and not chosen: Garnet needed three discarded runs
before it settled, where every other arm settled on its first or second
(`02-calibrate.log`).

**How to read a pair line.** Under each table is one line per comparator. `r` is
this server's median divided by the comparator's, computed separately for
throughput and for total CPU per operation. `s` is the larger of the two arms'
own within-arm spreads, `(max − min) / median` over their three kept runs. Where
`|r − 1|` is no greater than `s` or 2 %, whichever is larger, the pair is
**indistinguishable** and no ratio is printed; otherwise it is **ahead** or
**behind** on throughput and **cheaper** or **more expensive per operation** on
CPU, with `r` to two decimals. The two percentages at the end of each line are
the spreads `s` was taken from, throughput first. The rule was fixed before the
run. There is no adjective anywhere on this page: a pair gets one of those words
or it gets *indistinguishable*, and where throughput and CPU point opposite ways
a sentence below the lines says so.

**Where a reading changed since `v0.1.0`, a short list under the pair lines
says so.** It names only the pairs whose word moved and what that pair read in
the earlier run; a pair whose word is the same gets nothing, and the `KEYS`
cell gets nothing because `v0.1.0` never ran it.

**The `evicted/op` column is printed in every table, and outside the eviction
cell it reads `0.000` in every row.** That is kept deliberately. Every engine
that reports `evicted_keys` at all reports it whether or not a ceiling is set,
so a zero there is a measured declaration that nothing was evicted, not a blank
waiting to be filled; Garnet does not report the field at all and prints `-`,
which is a different statement. Keeping the column means the eviction cell's
measurement sits in the same place, under the same name, as the zeros the other
three cells declare.

### GET 64 B — a small value read at four pipeline depths

`GET` of a 64-byte value at pipeline depths 1, 4, 16 and 64, 50 connections,
keys spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs (the probe's hit count is in `03-field.log`). `W` = 3
discarded runs per arm per depth, then three kept.

#### Depth 1

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 143 906 | 4.420 | 5.640 | 10.140 | 1.46 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 136 649 | 1.410 | 4.720 | 6.130 | 0.84 | 1.00 | 0.000 | 1.053 |
| redis-iot4 | 127 698 | 3.600 | 11.500 | 15.130 | 1.93 | 1.00 | 0.000 | 1.127 |
| valkey-iot1 | 135 630 | 1.240 | 4.800 | 6.040 | 0.82 | 1.00 | 0.000 | 1.061 |
| valkey-iot4 | 141 623 | 9.210 | 4.630 | 13.840 | 1.96 | 1.00 | 0.000 | 1.016 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 143 906 | 4.420 | 5.640 | 10.140 | 1.46 | 1.00 | 0.000 | 1.000 |
| dragonfly | 131 735 | 20.100 | 10.040 | 30.250 | 3.99 | 1.00 | 0.000 | 1.092 |
| garnet | 126 326 | 25.990 | 18.850 | 44.580 | 5.64 | 1.00 | - | 1.139 |

- seedstone vs redis-iot1: ahead 1.05x on throughput; more expensive per operation 1.65x (spreads 3.30 % / 3.43 %)
- seedstone vs redis-iot4: ahead 1.13x on throughput; cheaper per operation 0.67x (spreads 2.28 % / 0.89 %)
- seedstone vs valkey-iot1: ahead 1.06x on throughput; more expensive per operation 1.68x (spreads 0.71 % / 0.89 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; cheaper per operation 0.73x (spreads 2.19 % / 2.89 %)
- seedstone vs dragonfly: ahead 1.09x on throughput; cheaper per operation 0.34x (spreads 2.02 % / 0.89 %)
- seedstone vs garnet: ahead 1.14x on throughput; cheaper per operation 0.23x (spreads 0.59 % / 2.02 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.1.0` this pair read *ahead* 1.04× on throughput.

#### Depth 4

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 513 084 | 3.470 | 2.760 | 6.220 | 3.19 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 536 769 | 0.550 | 1.150 | 1.700 | 0.92 | 1.00 | 0.000 | 0.956 |
| redis-iot4 | 477 783 | 1.160 | 2.900 | 4.040 | 1.94 | 1.00 | 0.000 | 1.074 |
| valkey-iot1 | 547 945 | 0.580 | 1.140 | 1.710 | 0.94 | 1.00 | 0.000 | 0.936 |
| valkey-iot4 | 542 888 | 2.530 | 1.150 | 3.670 | 1.98 | 1.00 | 0.000 | 0.945 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 513 084 | 3.470 | 2.760 | 6.220 | 3.19 | 1.00 | 0.000 | 1.000 |
| dragonfly | 507 872 | 8.180 | 3.420 | 11.600 | 5.89 | 1.00 | 0.000 | 1.010 |
| garnet | 480 538 | 7.200 | 5.140 | 12.250 | 5.89 | 1.00 | - | 1.068 |

- seedstone vs redis-iot1: behind 0.96x on throughput; more expensive per operation 3.66x (spreads 2.81 % / 2.94 %)
- seedstone vs redis-iot4: ahead 1.07x on throughput; more expensive per operation 1.54x (spreads 3.29 % / 2.48 %)
- seedstone vs valkey-iot1: behind 0.94x on throughput; more expensive per operation 3.64x (spreads 3.91 % / 3.51 %)
- seedstone vs valkey-iot4: behind 0.95x on throughput; more expensive per operation 1.69x (spreads 2.81 % / 1.63 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.54x (spreads 2.81 % / 0.96 %)
- seedstone vs garnet: ahead 1.07x on throughput; cheaper per operation 0.51x (spreads 2.81 % / 11.43 %)

The two quantities point opposite ways against `redis-iot4`: this server is
ahead on throughput and more expensive per operation, and both readings are of
the same runs.

Against `redis-iot1`, `valkey-iot1` and `valkey-iot4` on this row the two
quantities agree rather than disagree — behind on throughput and more expensive
per operation is one direction, and this row is not a trade against any of the
three. It is the only row on this page carrying more than one *behind* against
the four Redis and Valkey arms.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot4`: in `v0.1.0` this pair read *indistinguishable* on throughput.
- `valkey-iot1`: in `v0.1.0` this pair read *indistinguishable* on throughput.

#### Depth 16

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 805 054 | 2.000 | 0.750 | 2.770 | 4.99 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 694 915 | 0.320 | 0.270 | 0.590 | 1.00 | 0.90 | 0.000 | 1.065 |
| redis-iot4 | 1 666 667 | 0.560 | 0.640 | 1.200 | 2.00 | 1.00 | 0.000 | 1.083 |
| valkey-iot1 | 1 597 444 | 0.400 | 0.230 | 0.630 | 1.00 | 0.83 | 0.000 | 1.130 |
| valkey-iot4 | 1 851 852 | 0.810 | 0.280 | 1.090 | 2.02 | 1.01 | 0.000 | 0.975 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 805 054 | 2.000 | 0.750 | 2.770 | 4.99 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 795 332 | 2.790 | 1.030 | 3.820 | 6.86 | 1.01 | 0.000 | 1.005 |
| garnet | 1 639 344 | 2.190 | 1.560 | 3.770 | 6.12 | 1.00 | - | 1.101 |

- seedstone vs redis-iot1: ahead 1.06x on throughput; more expensive per operation 4.69x (spreads 1.72 % / 1.69 %)
- seedstone vs redis-iot4: ahead 1.08x on throughput; more expensive per operation 2.31x (spreads 1.17 % / 1.08 %)
- seedstone vs valkey-iot1: ahead 1.13x on throughput; more expensive per operation 4.40x (spreads 1.27 % / 1.59 %)
- seedstone vs valkey-iot4: behind 0.97x on throughput; more expensive per operation 2.54x (spreads 0.19 % / 1.08 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.73x (spreads 2.15 % / 1.57 %)
- seedstone vs garnet: ahead 1.10x on throughput; cheaper per operation 0.73x (spreads 1.97 % / 4.24 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

Against `valkey-iot4` on this row they agree — behind on throughput and more
expensive per operation — and the row is not a trade against it.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.1.0` this pair read *indistinguishable* on throughput.
- `dragonfly`: in `v0.1.0` this pair read *ahead* 1.07× on throughput.
- `garnet`: in `v0.1.0` this pair read *indistinguishable* on CPU per operation.

#### Depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 587 156 | 1.200 | 0.230 | 1.430 | 6.50 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 624 672 | 0.300 | 0.080 | 0.380 | 1.00 | 0.54 | 0.000 | 1.748 |
| redis-iot4 | 3 322 259 | 0.370 | 0.090 | 0.470 | 1.55 | 0.78 | 0.000 | 1.381 |
| valkey-iot1 | 2 272 727 | 0.380 | 0.060 | 0.440 | 1.00 | 0.47 | 0.000 | 2.018 |
| valkey-iot4 | 3 663 004 | 1.020 | 0.080 | 1.100 | 4.04 | 0.80 | 0.000 | 1.252 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 587 156 | 1.200 | 0.230 | 1.430 | 6.50 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 672 897 | 1.210 | 0.350 | 1.540 | 7.20 | 1.01 | 0.000 | 0.982 |
| garnet | 4 385 965 | 0.960 | 0.580 | 1.540 | 6.75 | 1.01 | - | 1.046 |

- seedstone vs redis-iot1: ahead 1.75x on throughput; more expensive per operation 3.76x (spreads 2.30 % / 2.80 %)
- seedstone vs redis-iot4: ahead 1.38x on throughput; more expensive per operation 3.04x (spreads 2.30 % / 2.80 %)
- seedstone vs valkey-iot1: ahead 2.02x on throughput; more expensive per operation 3.25x (spreads 2.30 % / 2.80 %)
- seedstone vs valkey-iot4: ahead 1.25x on throughput; more expensive per operation 1.30x (spreads 2.30 % / 2.80 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.93x (spreads 2.75 % / 2.80 %)
- seedstone vs garnet: ahead 1.05x on throughput; indistinguishable (spreads 2.30 % / 12.34 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `dragonfly`: in `v0.1.0` this pair read *behind* 0.91× on throughput and *more expensive per operation* 1.41×.
- `garnet`: in `v0.1.0` this pair read *behind* 0.97× on throughput and *more expensive per operation* 1.49×.

### SET 64 B, without and with `EX 60` — a write, and the same write carrying a deadline

`SET` of a 64-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, run twice per arm: once plain, once as
`SET … EX 60`. `W` = 3 discarded runs per arm per row, then three kept. On the
`EX` row every one of the seven arms answers the `TTL` probe with
`ttl probe: 60 (a deadline reached the server)` (`04-expiry.log`), so the
deadline reached the server and was not dropped on the way.

#### Without `EX`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 608 295 | 1.330 | 0.250 | 1.580 | 7.28 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 087 683 | 0.400 | 0.080 | 0.480 | 1.00 | 0.44 | 0.000 | 2.207 |
| redis-iot4 | 2 433 090 | 0.500 | 0.110 | 0.600 | 1.46 | 0.57 | 0.000 | 1.894 |
| valkey-iot1 | 1 779 360 | 0.500 | 0.060 | 0.560 | 1.00 | 0.37 | 0.000 | 2.590 |
| valkey-iot4 | 2 631 579 | 1.430 | 0.100 | 1.530 | 4.03 | 0.61 | 0.000 | 1.751 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 608 295 | 1.330 | 0.250 | 1.580 | 7.28 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 672 897 | 1.290 | 0.240 | 1.530 | 7.16 | 1.01 | 0.000 | 0.986 |
| garnet | 4 405 286 | 1.000 | 0.510 | 1.510 | 6.65 | 1.01 | - | 1.046 |

- seedstone vs redis-iot1: ahead 2.21x on throughput; more expensive per operation 3.29x (spreads 1.38 % / 2.08 %)
- seedstone vs redis-iot4: ahead 1.89x on throughput; more expensive per operation 2.63x (spreads 1.38 % / 1.90 %)
- seedstone vs valkey-iot1: ahead 2.59x on throughput; more expensive per operation 2.82x (spreads 1.38 % / 1.90 %)
- seedstone vs valkey-iot4: ahead 1.75x on throughput; indistinguishable (spreads 1.38 % / 9.15 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.03x (spreads 1.38 % / 1.90 %)
- seedstone vs garnet: ahead 1.05x on throughput; indistinguishable (spreads 2.16 % / 11.92 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.1.0` this pair read *more expensive per operation* 1.84×.
- `garnet`: in `v0.1.0` this pair read *indistinguishable* on throughput and *more expensive per operation* 1.32×.

#### With `EX 60`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 587 156 | 1.450 | 0.240 | 1.690 | 7.74 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 1 560 062 | 0.560 | 0.090 | 0.640 | 1.00 | 0.34 | 0.000 | 2.940 |
| redis-iot4 | 1 782 531 | 0.670 | 0.100 | 0.770 | 1.37 | 0.41 | 0.000 | 2.573 |
| valkey-iot1 | 1 172 333 | 0.780 | 0.070 | 0.860 | 1.00 | 0.25 | 0.000 | 3.913 |
| valkey-iot4 | 1 658 375 | 2.320 | 0.110 | 2.430 | 4.03 | 0.40 | 0.000 | 2.766 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 587 156 | 1.450 | 0.240 | 1.690 | 7.74 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 651 162 | 1.470 | 0.200 | 1.670 | 7.80 | 1.01 | 0.000 | 0.986 |
| garnet | 4 310 345 | 1.060 | 0.640 | 1.700 | 7.39 | 1.01 | - | 1.064 |

- seedstone vs redis-iot1: ahead 2.94x on throughput; more expensive per operation 2.64x (spreads 2.70 % / 1.56 %)
- seedstone vs redis-iot4: ahead 2.57x on throughput; more expensive per operation 2.19x (spreads 2.70 % / 2.60 %)
- seedstone vs valkey-iot1: ahead 3.91x on throughput; more expensive per operation 1.97x (spreads 2.70 % / 2.33 %)
- seedstone vs valkey-iot4: ahead 2.77x on throughput; cheaper per operation 0.70x (spreads 2.70 % / 2.47 %)
- seedstone vs dragonfly: indistinguishable on throughput; indistinguishable (spreads 2.70 % / 1.80 %)
- seedstone vs garnet: ahead 1.06x on throughput; indistinguishable (spreads 2.70 % / 20.59 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Every pair line on this row prints the same throughput spread, 2.70 %, and it
is this server's own.** Its three kept runs here are 4 484 305, 4 587 156 and
4 608 295 operations per second: a spread of 2.70 %, wider on this row than any
comparator's, so it is the one the rule takes six times out of six. What decides
the throughput words on this row is therefore this server's own run-to-run
variation under `SET … EX`, and neither a property of the machine nor a property
of the comparators. The same was true in `v0.1.0`, where that spread was
11.34 %.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.1.0` this pair read *more expensive per operation* 1.24×.
- `dragonfly`: in `v0.1.0` this pair read *behind* 0.63× on throughput and *more expensive per operation* 1.85×.
- `garnet`: in `v0.1.0` this pair read *behind* 0.66× on throughput and *more expensive per operation* 1.93×.

### SET 10 240 B past a 384 MB ceiling — the write path under eviction

`SET` of a 10 240-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, against a server started with a 384 MB memory
ceiling. Unlike every other cell this one starts empty: the keyspace is filled
past the ceiling first and that fill is discarded, then `W` = 3 discarded runs,
then three kept — so every kept run is in steady-state eviction rather than
still filling. The `evicted/op` column is the cell's own declaration that this
held: between 0.61 and 0.68 keys evicted per operation, in every arm.

**The ceiling and the policy, per arm.** This server, Redis and Valkey each run
`--maxmemory 384mb --maxmemory-policy allkeys-lru`; the start line of every arm
is in `05-eviction.log`.

**Two engines are absent from this table, and each absence is that engine's
property, not this cell's.** Garnet 2.1.5 bounds memory by a log size with tail
reclamation rather than by a ceiling with LRU eviction, so the comparable cell
does not exist for it. Dragonfly `df-v1.40.2` requires 256 MiB of `maxmemory`
per proactor thread and refuses to start below that: at the ten proactor
threads this hardware gives it, the smallest ceiling it accepts is 2.50 GiB,
which is above
the roughly 0.95 GiB this cell's keyspace can hold — so it would have evicted
nothing, and raising the ceiling to admit it would have stopped every other arm
evicting too, while the cell still looked like an eviction cell. The ceiling was
declared before the run and was not moved to accommodate an engine. This table
therefore carries five arms where every other table carries seven, and there is
no *Other engines* table under it.

**The ratios on this row are loose, and the reason is on the row.** Each engine
accounts for its own memory by its own formula, so "full" arrives at a different
key count in each of them; the number of values resident behind the same 384 MB
ceiling is not the same across arms, and neither is what one eviction costs.
`evicted/op` is the column that says so, and it is printed on every row.

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 615 006 | 4.690 | 2.980 | 7.670 | 4.72 | 1.01 | 0.613 | 1.000 |
| redis-iot1 | 267 094 | 1.680 | 2.070 | 3.740 | 1.00 | 0.53 | 0.678 | 2.303 |
| redis-iot4 | 640 205 | 2.830 | 2.970 | 5.780 | 3.76 | 1.01 | 0.678 | 0.961 |
| valkey-iot1 | 262 605 | 1.710 | 2.100 | 3.820 | 1.00 | 0.53 | 0.678 | 2.342 |
| valkey-iot4 | 638 570 | 4.410 | 1.810 | 6.210 | 3.93 | 1.01 | 0.679 | 0.963 |

- seedstone vs redis-iot1: ahead 2.30x on throughput; more expensive per operation 2.05x (spreads 4.23 % / 0.80 %)
- seedstone vs redis-iot4: indistinguishable on throughput; more expensive per operation 1.33x (spreads 4.23 % / 1.56 %)
- seedstone vs valkey-iot1: ahead 2.34x on throughput; more expensive per operation 2.01x (spreads 4.23 % / 0.52 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.24x (spreads 4.23 % / 0.97 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot4`: in `v0.1.0` this pair read *behind* 0.96× on throughput.

### MGET of 1, 4 and 16 keys — the multi-key read

`MGET` of 1, 4 and 16 spread keys at pipeline depth 64, 50 connections, keys
spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs. `W` = 3 discarded runs per arm per row, then three kept.

**`ops/s` on these rows is requests per second, not keys per second.** The
16-key row moves sixteen times the keys of the 1-key row at the same figure, so
the three rows are not comparable with each other on throughput. Each row is
comparable across arms, which is what these tables are for.

#### 1 key

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 731 343 | 1.420 | 0.300 | 1.710 | 6.46 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 531 646 | 0.320 | 0.080 | 0.390 | 0.99 | 0.64 | 0.000 | 1.474 |
| redis-iot4 | 3 164 557 | 0.370 | 0.110 | 0.480 | 1.52 | 0.88 | 0.000 | 1.179 |
| valkey-iot1 | 2 207 506 | 0.360 | 0.090 | 0.460 | 1.01 | 0.56 | 0.000 | 1.690 |
| valkey-iot4 | 3 533 569 | 1.060 | 0.090 | 1.150 | 4.06 | 0.92 | 0.000 | 1.056 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 731 343 | 1.420 | 0.300 | 1.710 | 6.46 | 1.01 | 0.000 | 1.000 |
| dragonfly | 3 875 969 | 1.720 | 0.300 | 2.020 | 7.79 | 1.01 | 0.000 | 0.963 |
| garnet | 3 636 364 | 1.000 | 0.620 | 1.640 | 5.96 | 1.01 | - | 1.026 |

- seedstone vs redis-iot1: ahead 1.47x on throughput; more expensive per operation 4.38x (spreads 1.90 % / 2.56 %)
- seedstone vs redis-iot4: ahead 1.18x on throughput; more expensive per operation 3.56x (spreads 1.90 % / 1.17 %)
- seedstone vs valkey-iot1: ahead 1.69x on throughput; more expensive per operation 3.72x (spreads 1.90 % / 2.17 %)
- seedstone vs valkey-iot4: ahead 1.06x on throughput; more expensive per operation 1.49x (spreads 1.90 % / 1.17 %)
- seedstone vs dragonfly: behind 0.96x on throughput; cheaper per operation 0.85x (spreads 1.90 % / 1.17 %)
- seedstone vs garnet: indistinguishable on throughput; indistinguishable (spreads 2.89 % / 8.54 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs. They point
opposite ways against `dragonfly` too, the other way round: behind on
throughput and cheaper per operation.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot4`: in `v0.1.0` this pair read *behind* 0.91× on throughput.
- `valkey-iot4`: in `v0.1.0` this pair read *behind* 0.81× on throughput.
- `dragonfly`: in `v0.1.0` this pair read *more expensive per operation* 1.72×.
- `garnet`: in `v0.1.0` this pair read *behind* 0.76× on throughput and *more expensive per operation* 2.11×.

#### 4 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 923 077 | 3.200 | 0.250 | 3.450 | 6.63 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 1 124 859 | 0.770 | 0.110 | 0.880 | 1.00 | 0.57 | 0.000 | 1.710 |
| redis-iot4 | 1 273 885 | 0.890 | 0.150 | 1.040 | 1.33 | 0.70 | 0.000 | 1.510 |
| valkey-iot1 | 1 026 694 | 0.870 | 0.110 | 0.970 | 1.00 | 0.53 | 0.000 | 1.873 |
| valkey-iot4 | 1 470 588 | 1.950 | 0.110 | 2.050 | 3.02 | 0.77 | 0.000 | 1.308 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 923 077 | 3.200 | 0.250 | 3.450 | 6.63 | 1.01 | 0.000 | 1.000 |
| dragonfly | 1 239 157 | 6.150 | 0.890 | 7.050 | 8.73 | 0.68 | 0.000 | 1.552 |
| garnet | 1 858 736 | 1.790 | 0.730 | 2.510 | 4.67 | 1.01 | - | 1.035 |

- seedstone vs redis-iot1: ahead 1.71x on throughput; more expensive per operation 3.92x (spreads 1.80 % / 2.27 %)
- seedstone vs redis-iot4: ahead 1.51x on throughput; more expensive per operation 3.32x (spreads 0.64 % / 1.92 %)
- seedstone vs valkey-iot1: ahead 1.87x on throughput; more expensive per operation 3.56x (spreads 0.57 % / 1.03 %)
- seedstone vs valkey-iot4: ahead 1.31x on throughput; more expensive per operation 1.68x (spreads 0.57 % / 0.49 %)
- seedstone vs dragonfly: ahead 1.55x on throughput; cheaper per operation 0.49x (spreads 0.57 % / 0.29 %)
- seedstone vs garnet: ahead 1.03x on throughput; more expensive per operation 1.37x (spreads 3.26 % / 11.16 %)

The two quantities point opposite ways against all four Redis and Valkey arms
and against `garnet`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot1`: in `v0.1.0` this pair read *behind* 0.78× on throughput.
- `redis-iot4`: in `v0.1.0` this pair read *behind* 0.71× on throughput.
- `valkey-iot1`: in `v0.1.0` this pair read *behind* 0.81× on throughput.
- `valkey-iot4`: in `v0.1.0` this pair read *behind* 0.56× on throughput.
- `dragonfly`: in `v0.1.0` this pair read *behind* 0.69× on throughput and *more expensive per operation* 1.63×.
- `garnet`: in `v0.1.0` this pair read *behind* 0.44× on throughput.

#### 16 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 627 353 | 11.220 | 0.980 | 12.250 | 7.62 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 339 328 | 2.660 | 0.270 | 2.940 | 1.00 | 0.53 | 0.000 | 1.849 |
| redis-iot4 | 397 141 | 4.610 | 4.840 | 9.460 | 3.76 | 0.62 | 0.000 | 1.580 |
| valkey-iot1 | 313 185 | 2.910 | 0.280 | 3.190 | 1.00 | 0.49 | 0.000 | 2.003 |
| valkey-iot4 | 442 870 | 8.760 | 0.290 | 9.050 | 4.01 | 0.70 | 0.000 | 1.417 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 627 353 | 11.220 | 0.980 | 12.250 | 7.62 | 1.00 | 0.000 | 1.000 |
| dragonfly | 465 766 | 15.030 | 2.380 | 17.410 | 8.11 | 0.76 | 0.000 | 1.347 |
| garnet | 625 782 | 5.180 | 1.250 | 6.430 | 4.02 | 1.00 | - | 1.003 |

- seedstone vs redis-iot1: ahead 1.85x on throughput; more expensive per operation 4.17x (spreads 1.25 % / 3.18 %)
- seedstone vs redis-iot4: ahead 1.58x on throughput; more expensive per operation 1.29x (spreads 1.25 % / 3.18 %)
- seedstone vs valkey-iot1: ahead 2.00x on throughput; more expensive per operation 3.84x (spreads 1.25 % / 3.18 %)
- seedstone vs valkey-iot4: ahead 1.42x on throughput; more expensive per operation 1.35x (spreads 1.25 % / 3.18 %)
- seedstone vs dragonfly: ahead 1.35x on throughput; cheaper per operation 0.70x (spreads 1.25 % / 3.18 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.91x (spreads 1.30 % / 3.18 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.1.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot1`: in `v0.1.0` this pair read *behind* 0.77× on throughput.
- `redis-iot4`: in `v0.1.0` this pair read *behind* 0.66× on throughput.
- `valkey-iot1`: in `v0.1.0` this pair read *behind* 0.83× on throughput.
- `valkey-iot4`: in `v0.1.0` this pair read *behind* 0.59× on throughput.
- `dragonfly`: in `v0.1.0` this pair read *behind* 0.58× on throughput and *more expensive per operation* 2.03×.
- `garnet`: in `v0.1.0` this pair read *behind* 0.40× on throughput.

### KEYS over 7 000 keys — a page cache's walk

`KEYS` against a glob at pipeline depth 1, 50 connections, 20 000 calls per run,
over a keyspace of 7 000 keys of 10 240 bytes under 64 prefixes, 199 bytes a
key. The keyspace is written by `bench/keys-load.sh` rather than by
`redis-benchmark`, and key `i` is a function of `i` alone, so every arm and
every run walks the identical 7 000 keys; each arm's `dbsize` is read back and
printed in `07-keys.log`. One fixed prefix of the 64 is matched per call, so
every call answers the same 110 keys. Depth 1 because the deployment these
shapes are sized from does not pipeline a `KEYS` call. `W` = 3 discarded runs
per arm, then three kept.

**This cell is new in this run.** `v0.1.0` did not run it, so there is no
earlier reading to put beside these lines.

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 579 | 2749.500 | 32.500 | 2783.500 | 9.96 | 0.07 | 0.000 | 1.000 |
| redis-iot1 | 688 | 1444.000 | 10.000 | 1454.000 | 1.00 | 0.01 | 0.000 | 5.205 |
| redis-iot4 | 697 | 1443.500 | 27.500 | 1471.500 | 1.03 | 0.02 | 0.000 | 5.135 |
| valkey-iot1 | 699 | 1420.500 | 9.500 | 1429.500 | 1.00 | 0.02 | 0.000 | 5.117 |
| valkey-iot4 | 711 | 2804.000 | 10.500 | 2814.500 | 2.00 | 0.02 | 0.000 | 5.037 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 579 | 2749.500 | 32.500 | 2783.500 | 9.96 | 0.07 | 0.000 | 1.000 |
| dragonfly | 2 132 | 2257.500 | 83.500 | 2341.000 | 4.99 | 0.04 | 0.000 | 1.679 |
| garnet | 1 569 | 6306.000 | 63.500 | 6366.000 | 9.98 | 0.03 | - | 2.281 |

- seedstone vs redis-iot1: ahead 5.20x on throughput; more expensive per operation 1.91x (spreads 0.38 % / 0.36 %)
- seedstone vs redis-iot4: ahead 5.14x on throughput; more expensive per operation 1.89x (spreads 0.38 % / 0.65 %)
- seedstone vs valkey-iot1: ahead 5.12x on throughput; more expensive per operation 1.95x (spreads 0.38 % / 0.36 %)
- seedstone vs valkey-iot4: ahead 5.04x on throughput; indistinguishable (spreads 0.38 % / 0.36 %)
- seedstone vs dragonfly: ahead 1.68x on throughput; more expensive per operation 1.19x (spreads 0.89 % / 0.36 %)
- seedstone vs garnet: ahead 2.28x on throughput; cheaper per operation 0.44x (spreads 3.39 % / 3.20 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`,
`valkey-iot1` and `dragonfly`: this server is ahead on throughput and more
expensive per operation against each of them, and both readings are of the same
runs.

**This server answers 5.2× the `KEYS` calls per second of Redis 8.10.0 at one
I/O thread and spends 1.91× the CPU on each of them, and that is the shape of
the command rather than an overhead on it.** `KEYS` here is a concurrent walk of
every shard, in steps bounded so that no single request can occupy an executor
for longer than one step, with the matching names gathered at the end; Redis
8.10.0 walks one dictionary on one thread. Ten cores against one is what buys
the calls per second, and the walk's own machinery — the wake path, the
per-step bookkeeping, the gather — is what the extra CPU per call pays for. The
cost was measured before it was published and the shape is kept: a walk that
cannot monopolise an executor is a property this server is not willing to trade
for a cheaper call, and the column that prices the trade is on the table.

**One declaration about the load on this cell.** `dragonfly`'s keyspace loader
reported six errors and 7 005 replies where the other six arms each reported no
error and 7 000. The keyspace every arm then ran against is the same — 7 000
keys of 10 240 B over 64 prefixes, `dbsize=7000`, read back from each server and
printed in `07-keys.log`, `dragonfly` included. The six sit in the loader's
exchange, before the measurement, and the measurement ran on the same keyspace
as every other arm's. It is stated here rather than left in the log.

## What these numbers do not say

- The client and the server share one machine over loopback; there is no
  network.
- One machine class — ARM Neoverse-V2 (Google Axion), 16 cores, one thread
  per core. A re-run on x86, or on a machine with SMT, is a different table.
- `redis-benchmark` is the only load generator, and it is the load generator
  of the Redis version measured.
- No latency percentiles. No memory footprint, resident or accounted. No
  persistence, on any arm.
- One process per engine: N Redis processes against one seedstone was not
  measured.
- No engine was configured by its authors; the configuration policy above is
  the whole of the tuning.
- Two payloads (64 B, 10 240 B) and two key distributions: 100 000 spread keys
  everywhere except the `KEYS` cell, which walks 7 000 keys under 64 prefixes.
  Pipeline depth at most 64, 50 connections throughout.
- The `KEYS` cell matches one prefix that answers 110 of 7 000 keys. A
  glob that matches nothing, a glob that matches everything, a keyspace an
  order of magnitude larger, and a `KEYS` call concurrent with write traffic
  are four different cells, and none of them was run.
- Garnet and Dragonfly are both absent from the eviction table, each for the
  reason stated there: Garnet 2.1.5 bounds memory by a log size with tail
  reclamation rather than a ceiling with LRU, and Dragonfly `df-v1.40.2`
  refuses to start below 256 MiB of `maxmemory` per proactor thread, which at
  ten threads puts
  the smallest ceiling it accepts above everything this cell's keyspace can
  hold. The ceiling was declared before the run and was not moved to
  accommodate an engine.
- **The two runs compared on this page ran on different kernels.** This run
  stands on `7.0.0-1011-gcp`; the `v0.1.0` figures quoted under the changed
  pairs were measured on `6.17.0-1022-gcp`, on the same machine class. Each
  kernel is echoed at the head of its run's logs. The canary is the only
  instrument this page has for deciding whether two runs are comparable at all,
  and it passed here at `+4.26 %` against the same reference the `v0.1.0` run
  read `+0.77 %` against — the wider of the two margins, inside the ±5 % fixed
  before either run. What sanctions the comparison is that reading and nothing
  else.
- A throughput figure that does not state its key distribution is a figure
  about the harness. Every figure here states it.

## Reproducing, and reading a re-run

On a Linux machine with `redis-benchmark`, `redis-cli`, `taskset`, and the
engines installed, with the paths and cpusets in `bench/campaign.sh`
overridden by environment variables where they differ:

```sh
bash bench/campaign.sh canary     > 01-canary.log      # stops if not comparable
bash bench/campaign.sh calibrate  > 02-calibrate.log
python3 bench/report.py --calibrate 02-calibrate.log   # prints W
WARMUP=<W> bash bench/campaign.sh field     > 03-field.log
WARMUP=<W> bash bench/campaign.sh expiry    > 04-expiry.log
WARMUP=<W> bash bench/campaign.sh eviction  > 05-eviction.log
WARMUP=<W> bash bench/campaign.sh multikey  > 06-multikey.log
WARMUP=<W> bash bench/campaign.sh keys      > 07-keys.log
python3 bench/report.py 0[3-7]-*.log
```

`W` was 3 for this run, and the calibration derives it again rather than
assuming it. The canary decides whether a re-run is comparable to the tables on
this page. A re-run on other hardware is a different table, not a correction of
this one, and is read on its own terms.

## Re-measurement

The same harness runs at every minor release. The new tables replace these,
and this run's raw logs stay under `bench/results/v0.2.0/`.
