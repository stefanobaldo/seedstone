# Benchmarks

**Release measured:** `v0.3.2` (commit `0d4f8df`). **Date:** 2026-10-11.
**Machine:** GCP `c4a-standard-16`. **Engines:** Redis 8.10.0, Valkey 9.1.1,
Dragonfly `df-v1.40.2`, Garnet 2.1.5 — each as the engine itself reports its
version, not as its release archive is named. **Warm-up runs discarded per arm
per cell (`W`):** 1, derived by the calibration below.

The method below was committed before the run; the tables were added after
it. The raw logs the tables are computed from are in
[`bench/results/v0.3.2/`](../bench/results/v0.3.2/), and
`python3 bench/report.py bench/results/v0.3.2/0[3-8]-*.log` regenerates every
table and every pair line on this page. The only readings it does not produce
are the `v0.2.0` ones quoted under the pairs whose word changed; the same
command over that run's logs produces those. A number here without its method
beside it would be a marketing number, so the method comes first.

**The runs these tables replace are still here.** The raw logs of the `v0.2.0`
and `v0.1.0` runs stay under [`bench/results/v0.2.0/`](../bench/results/v0.2.0/)
and [`bench/results/v0.1.0/`](../bench/results/v0.1.0/), the same command over
either regenerates that run's tables unchanged, and wherever a pair's reading
changed since `v0.2.0`, a line under that pair says what it read then. Two runs
were measured and not published: `v0.3.0` and `v0.3.1` both read this server's
`KEYS` figure lower than `v0.2.0` did
([#102](https://github.com/stefanobaldo/seedstone/issues/102)), which `v0.3.2`
resolved. Their logs stay under
[`bench/results/v0.3.0/`](../bench/results/v0.3.0/) and
[`bench/results/v0.3.1/`](../bench/results/v0.3.1/); the `v0.3.1` run has no
durability log.

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
than suspected. Its highest reading across the 279 kept runs behind the tables
below is 1.01 cores, and no row of any table, each being a median of three,
reaches above it. If a re-run shows more, that row is about the client.

**The engines, and how each was configured.** Each engine receives the
configuration that matches the hardware it is given, where it has a knob for
that, and nothing else — no allocator, hugepage or affinity tuning, for any
of them, this server included.

| arm | version and provenance | started as |
|---|---|---|
| seedstone | `v0.3.2`, built on the machine with `cargo build --release --locked -p seedstone` | `--bind 127.0.0.1:6390 --max-clients 2000 --no-auth` |
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
cell can be read again. The `KEYS` walk is the only cell whose cost is set by
the size of the keyspace rather than by the size of a value: it is the shape a
prefix invalidation takes against that cache. The durability cell is the
newest, and the only one where a server writes to a disk: the same small write
under each setting of the log, against Redis with its own log at the matching
setting.

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
python3 bench/report.py bench/results/v0.3.2/0[3-8]-*.log
```

over the raw logs in [`bench/results/v0.3.2/`](../bench/results/v0.3.2/),
spliced into this page unedited. `W` is **1** for every cell in this run,
derived by the calibration and not chosen: `valkey-iot4` and Garnet each needed
one discarded run before they settled, and every other arm settled on its
first (`02-calibrate.log`).

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

**Where a reading changed since `v0.2.0`, a short list under the pair lines
says so.** It names only the pairs whose word moved and what that pair read in
the earlier run; a pair whose word is the same gets nothing, and the
durability cell gets nothing because `v0.2.0` never ran it. Seven of the 64
pairs the two runs share changed: five on throughput and four on CPU per
operation, two pairs on both. Of the CPU words that moved, two moved from
*indistinguishable* to *more expensive per operation* and two, both against
Garnet, from *indistinguishable* to *cheaper per operation*.

**The `evicted/op` column is printed in every table, and outside the eviction
cell it reads `0.000` in every row.** That is kept deliberately. Every engine
that reports `evicted_keys` at all reports it whether or not a ceiling is set,
so a zero there is a measured declaration that nothing was evicted, not a blank
waiting to be filled; Garnet does not report the field at all and prints `-`,
which is a different statement. Keeping the column means the eviction cell's
measurement sits in the same place, under the same name, as the zeros every
other cell declares.

### GET 64 B — a small value read at four pipeline depths

`GET` of a 64-byte value at pipeline depths 1, 4, 16 and 64, 50 connections,
keys spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs (the probe's hit count is in `03-field.log`). `W` = 1
discarded run per arm per depth, then three kept.

#### Depth 1

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 142 959 | 4.850 | 5.810 | 10.660 | 1.53 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 137 419 | 1.420 | 4.680 | 6.140 | 0.84 | 1.00 | 0.000 | 1.040 |
| redis-iot4 | 128 982 | 3.680 | 12.370 | 16.050 | 2.06 | 1.00 | 0.000 | 1.108 |
| valkey-iot1 | 137 798 | 1.260 | 4.730 | 5.990 | 0.83 | 1.00 | 0.000 | 1.037 |
| valkey-iot4 | 144 823 | 8.880 | 4.550 | 13.430 | 1.96 | 1.00 | 0.000 | 0.987 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 142 959 | 4.850 | 5.810 | 10.660 | 1.53 | 1.00 | 0.000 | 1.000 |
| dragonfly | 130 770 | 20.330 | 10.070 | 30.400 | 3.99 | 1.00 | 0.000 | 1.093 |
| garnet | 127 796 | 27.060 | 18.950 | 46.070 | 5.89 | 1.00 | - | 1.119 |

- seedstone vs redis-iot1: ahead 1.04x on throughput; more expensive per operation 1.74x (spreads 2.12 % / 1.63 %)
- seedstone vs redis-iot4: ahead 1.11x on throughput; cheaper per operation 0.66x (spreads 2.12 % / 1.31 %)
- seedstone vs valkey-iot1: ahead 1.04x on throughput; more expensive per operation 1.78x (spreads 2.12 % / 1.31 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; cheaper per operation 0.79x (spreads 2.12 % / 9.23 %)
- seedstone vs dragonfly: ahead 1.09x on throughput; cheaper per operation 0.35x (spreads 2.12 % / 1.31 %)
- seedstone vs garnet: ahead 1.12x on throughput; cheaper per operation 0.23x (spreads 2.12 % / 4.86 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

#### Depth 4

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 526 593 | 3.800 | 2.790 | 6.610 | 3.47 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 547 345 | 0.540 | 1.160 | 1.700 | 0.93 | 1.00 | 0.000 | 0.962 |
| redis-iot4 | 482 393 | 1.230 | 3.020 | 4.270 | 2.06 | 1.00 | 0.000 | 1.092 |
| valkey-iot1 | 552 486 | 0.610 | 1.090 | 1.700 | 0.94 | 1.00 | 0.000 | 0.953 |
| valkey-iot4 | 553 710 | 2.560 | 1.150 | 3.680 | 1.99 | 1.00 | 0.000 | 0.951 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 526 593 | 3.800 | 2.790 | 6.610 | 3.47 | 1.00 | 0.000 | 1.000 |
| dragonfly | 507 099 | 8.300 | 3.340 | 11.640 | 5.90 | 1.00 | 0.000 | 1.038 |
| garnet | 481 464 | 7.340 | 5.120 | 12.460 | 6.01 | 1.00 | - | 1.094 |

- seedstone vs redis-iot1: behind 0.96x on throughput; more expensive per operation 3.89x (spreads 1.16 % / 1.06 %)
- seedstone vs redis-iot4: ahead 1.09x on throughput; more expensive per operation 1.55x (spreads 1.45 % / 1.17 %)
- seedstone vs valkey-iot1: behind 0.95x on throughput; more expensive per operation 3.89x (spreads 1.62 % / 1.18 %)
- seedstone vs valkey-iot4: behind 0.95x on throughput; more expensive per operation 1.80x (spreads 3.73 % / 30.98 %)
- seedstone vs dragonfly: ahead 1.04x on throughput; cheaper per operation 0.57x (spreads 1.33 % / 1.06 %)
- seedstone vs garnet: ahead 1.09x on throughput; cheaper per operation 0.53x (spreads 1.16 % / 8.03 %)

The two quantities point opposite ways against `redis-iot4`: this server is
ahead on throughput and more expensive per operation, and both readings are of
the same runs.

This row carries three *behind* readings, against `redis-iot1`, `valkey-iot1`
and `valkey-iot4`, by 0.95× to 0.96×, as it did in `v0.2.0`; it is the only
row of the field cell with more than one.
[#32](https://github.com/stefanobaldo/seedstone/issues/32) measured what
keeping the executor threads awake between requests did to that reading and
what it cost per operation, and why that trade was declined.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `dragonfly`: in `v0.2.0` this pair read *indistinguishable* on throughput.

#### Depth 16

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 814 882 | 2.150 | 0.830 | 2.960 | 5.37 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 763 668 | 0.360 | 0.210 | 0.570 | 1.00 | 0.92 | 0.000 | 1.029 |
| redis-iot4 | 1 680 672 | 0.580 | 0.660 | 1.240 | 2.08 | 1.01 | 0.000 | 1.080 |
| valkey-iot1 | 1 584 786 | 0.390 | 0.240 | 0.630 | 1.00 | 0.83 | 0.000 | 1.145 |
| valkey-iot4 | 1 923 077 | 1.040 | 0.280 | 1.340 | 2.58 | 1.01 | 0.000 | 0.944 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 814 882 | 2.150 | 0.830 | 2.960 | 5.37 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 808 318 | 2.810 | 0.990 | 3.810 | 6.89 | 1.00 | 0.000 | 1.004 |
| garnet | 1 644 737 | 2.290 | 1.610 | 3.890 | 6.40 | 1.00 | - | 1.103 |

- seedstone vs redis-iot1: ahead 1.03x on throughput; more expensive per operation 5.19x (spreads 1.26 % / 1.75 %)
- seedstone vs redis-iot4: ahead 1.08x on throughput; more expensive per operation 2.39x (spreads 1.26 % / 0.68 %)
- seedstone vs valkey-iot1: ahead 1.15x on throughput; more expensive per operation 4.70x (spreads 1.26 % / 1.59 %)
- seedstone vs valkey-iot4: behind 0.94x on throughput; more expensive per operation 2.21x (spreads 1.26 % / 40.30 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.78x (spreads 1.44 % / 0.79 %)
- seedstone vs garnet: ahead 1.10x on throughput; cheaper per operation 0.76x (spreads 1.26 % / 3.86 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

#### Depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.200 | 0.260 | 1.450 | 6.74 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 645 503 | 0.310 | 0.070 | 0.380 | 1.01 | 0.54 | 0.000 | 1.750 |
| redis-iot4 | 3 311 258 | 0.380 | 0.090 | 0.470 | 1.56 | 0.77 | 0.000 | 1.398 |
| valkey-iot1 | 2 283 105 | 0.370 | 0.070 | 0.440 | 1.00 | 0.47 | 0.000 | 2.028 |
| valkey-iot4 | 3 663 004 | 1.050 | 0.070 | 1.120 | 4.10 | 0.80 | 0.000 | 1.264 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.200 | 0.260 | 1.450 | 6.74 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 651 162 | 1.210 | 0.340 | 1.550 | 7.21 | 1.01 | 0.000 | 0.995 |
| garnet | 4 366 812 | 0.940 | 0.560 | 1.490 | 6.51 | 1.01 | - | 1.060 |

- seedstone vs redis-iot1: ahead 1.75x on throughput; more expensive per operation 3.82x (spreads 0.93 % / 2.63 %)
- seedstone vs redis-iot4: ahead 1.40x on throughput; more expensive per operation 3.09x (spreads 0.93 % / 0.69 %)
- seedstone vs valkey-iot1: ahead 2.03x on throughput; more expensive per operation 3.30x (spreads 0.93 % / 0.69 %)
- seedstone vs valkey-iot4: ahead 1.26x on throughput; more expensive per operation 1.29x (spreads 0.93 % / 0.89 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.94x (spreads 1.40 % / 0.69 %)
- seedstone vs garnet: ahead 1.06x on throughput; indistinguishable (spreads 0.93 % / 16.11 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

### SET 64 B, without and with `EX 60` — a write, and the same write carrying a deadline

`SET` of a 64-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, run twice per arm: once plain, once as
`SET … EX 60`. `W` = 1 discarded run per arm per row, then three kept. On the
`EX` row every one of the seven arms answers the `TTL` probe with
`ttl probe: 60 (a deadline reached the server)` (`04-expiry.log`), so the
deadline reached the server and was not dropped on the way.

#### Without `EX`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.350 | 0.260 | 1.610 | 7.42 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 092 050 | 0.410 | 0.070 | 0.480 | 1.00 | 0.43 | 0.000 | 2.213 |
| redis-iot4 | 2 487 562 | 0.490 | 0.100 | 0.590 | 1.47 | 0.58 | 0.000 | 1.861 |
| valkey-iot1 | 1 779 360 | 0.490 | 0.070 | 0.570 | 1.01 | 0.37 | 0.000 | 2.602 |
| valkey-iot4 | 2 617 801 | 1.450 | 0.100 | 1.550 | 4.06 | 0.62 | 0.000 | 1.769 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.350 | 0.260 | 1.610 | 7.42 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 651 162 | 1.290 | 0.250 | 1.540 | 7.16 | 1.01 | 0.000 | 0.995 |
| garnet | 4 629 630 | 1.870 | 0.260 | 2.160 | 10.00 | 1.00 | - | 1.000 |

- seedstone vs redis-iot1: ahead 2.21x on throughput; more expensive per operation 3.35x (spreads 0.93 % / 1.86 %)
- seedstone vs redis-iot4: ahead 1.86x on throughput; more expensive per operation 2.73x (spreads 0.93 % / 1.86 %)
- seedstone vs valkey-iot1: ahead 2.60x on throughput; more expensive per operation 2.82x (spreads 0.93 % / 3.51 %)
- seedstone vs valkey-iot4: ahead 1.77x on throughput; more expensive per operation 1.04x (spreads 0.93 % / 1.86 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.05x (spreads 0.93 % / 1.86 %)
- seedstone vs garnet: indistinguishable on throughput; cheaper per operation 0.75x (spreads 9.49 % / 9.26 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.2.0` this pair read *indistinguishable* on CPU per operation.
- `garnet`: in `v0.2.0` this pair read *ahead* 1.05× on throughput and *indistinguishable* on CPU per operation.

#### With `EX 60`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 608 295 | 1.500 | 0.230 | 1.740 | 7.96 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 1 557 632 | 0.550 | 0.090 | 0.640 | 1.00 | 0.33 | 0.000 | 2.959 |
| redis-iot4 | 1 818 182 | 0.650 | 0.110 | 0.760 | 1.38 | 0.42 | 0.000 | 2.535 |
| valkey-iot1 | 1 180 638 | 0.770 | 0.080 | 0.840 | 0.99 | 0.25 | 0.000 | 3.903 |
| valkey-iot4 | 1 652 892 | 2.330 | 0.110 | 2.430 | 4.02 | 0.40 | 0.000 | 2.788 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 608 295 | 1.500 | 0.230 | 1.740 | 7.96 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 629 630 | 1.450 | 0.210 | 1.660 | 7.64 | 1.01 | 0.000 | 0.995 |
| garnet | 4 310 345 | 1.100 | 0.800 | 1.880 | 8.14 | 1.01 | - | 1.069 |

- seedstone vs redis-iot1: ahead 2.96x on throughput; more expensive per operation 2.72x (spreads 1.83 % / 1.56 %)
- seedstone vs redis-iot4: ahead 2.53x on throughput; more expensive per operation 2.29x (spreads 1.83 % / 1.32 %)
- seedstone vs valkey-iot1: ahead 3.90x on throughput; more expensive per operation 2.07x (spreads 1.83 % / 1.19 %)
- seedstone vs valkey-iot4: ahead 2.79x on throughput; cheaper per operation 0.72x (spreads 1.83 % / 7.82 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.05x (spreads 1.83 % / 1.20 %)
- seedstone vs garnet: ahead 1.07x on throughput; indistinguishable (spreads 2.54 % / 30.32 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `dragonfly`: in `v0.2.0` this pair read *indistinguishable* on CPU per operation.

### SET 10 240 B past a 384 MB ceiling — the write path under eviction

`SET` of a 10 240-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, against a server started with a 384 MB memory
ceiling. Unlike every other cell this one starts empty: the keyspace is filled
past the ceiling first and that fill is discarded, then `W` = 1 discarded run,
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
| seedstone | 625 391 | 4.950 | 3.110 | 8.060 | 5.04 | 1.01 | 0.613 | 1.000 |
| redis-iot1 | 264 690 | 1.710 | 2.080 | 3.780 | 1.00 | 0.53 | 0.678 | 2.363 |
| redis-iot4 | 640 615 | 2.770 | 3.020 | 5.760 | 3.73 | 1.01 | 0.678 | 0.976 |
| valkey-iot1 | 264 760 | 1.740 | 2.050 | 3.780 | 1.00 | 0.52 | 0.678 | 2.362 |
| valkey-iot4 | 624 610 | 4.370 | 1.840 | 6.280 | 3.92 | 1.01 | 0.678 | 1.001 |

- seedstone vs redis-iot1: ahead 2.36x on throughput; more expensive per operation 2.13x (spreads 0.94 % / 1.32 %)
- seedstone vs redis-iot4: behind 0.98x on throughput; more expensive per operation 1.40x (spreads 1.16 % / 1.56 %)
- seedstone vs valkey-iot1: ahead 2.36x on throughput; more expensive per operation 2.13x (spreads 0.94 % / 0.50 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.28x (spreads 9.40 % / 5.73 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot4`: in `v0.2.0` this pair read *indistinguishable* on throughput.

### MGET of 1, 4 and 16 keys — the multi-key read

`MGET` of 1, 4 and 16 spread keys at pipeline depth 64, 50 connections, keys
spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs. `W` = 1 discarded run per arm per row, then three kept.

**`ops/s` on these rows is requests per second, not keys per second.** The
16-key row moves sixteen times the keys of the 1-key row at the same figure, so
the three rows are not comparable with each other on throughput. Each row is
comparable across arms, which is what these tables are for.

#### 1 key

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 802 281 | 1.510 | 0.310 | 1.820 | 6.92 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 525 252 | 0.320 | 0.080 | 0.400 | 1.01 | 0.63 | 0.000 | 1.506 |
| redis-iot4 | 3 154 574 | 0.370 | 0.120 | 0.490 | 1.55 | 0.88 | 0.000 | 1.205 |
| valkey-iot1 | 2 232 143 | 0.380 | 0.070 | 0.450 | 1.00 | 0.56 | 0.000 | 1.703 |
| valkey-iot4 | 3 484 320 | 1.080 | 0.080 | 1.170 | 4.08 | 0.91 | 0.000 | 1.091 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 802 281 | 1.510 | 0.310 | 1.820 | 6.92 | 1.01 | 0.000 | 1.000 |
| dragonfly | 3 831 418 | 1.720 | 0.310 | 2.040 | 7.78 | 1.01 | 0.000 | 0.992 |
| garnet | 3 584 229 | 2.060 | 0.600 | 2.660 | 9.86 | 1.01 | - | 1.061 |

- seedstone vs redis-iot1: ahead 1.51x on throughput; more expensive per operation 4.55x (spreads 0.75 % / 2.50 %)
- seedstone vs redis-iot4: ahead 1.21x on throughput; more expensive per operation 3.71x (spreads 0.75 % / 2.04 %)
- seedstone vs valkey-iot1: ahead 1.70x on throughput; more expensive per operation 4.04x (spreads 0.75 % / 4.44 %)
- seedstone vs valkey-iot4: ahead 1.09x on throughput; more expensive per operation 1.56x (spreads 1.37 % / 17.95 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.89x (spreads 1.88 % / 0.55 %)
- seedstone vs garnet: ahead 1.06x on throughput; cheaper per operation 0.68x (spreads 5.95 % / 27.82 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `dragonfly`: in `v0.2.0` this pair read *behind* 0.96× on throughput.
- `garnet`: in `v0.2.0` this pair read *indistinguishable* on throughput and *indistinguishable* on CPU per operation.

#### 4 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 926 782 | 3.250 | 0.290 | 3.540 | 6.81 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 127 396 | 0.780 | 0.110 | 0.890 | 1.00 | 0.58 | 0.000 | 1.709 |
| redis-iot4 | 1 267 427 | 0.900 | 0.160 | 1.060 | 1.34 | 0.69 | 0.000 | 1.520 |
| valkey-iot1 | 1 033 058 | 0.860 | 0.100 | 0.970 | 1.00 | 0.53 | 0.000 | 1.865 |
| valkey-iot4 | 1 485 884 | 2.590 | 0.120 | 2.710 | 4.03 | 0.81 | 0.000 | 1.297 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 926 782 | 3.250 | 0.290 | 3.540 | 6.81 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 221 001 | 6.240 | 0.910 | 7.140 | 8.72 | 0.67 | 0.000 | 1.578 |
| garnet | 1 848 429 | 1.860 | 0.730 | 2.550 | 4.74 | 1.01 | - | 1.042 |

- seedstone vs redis-iot1: ahead 1.71x on throughput; more expensive per operation 3.98x (spreads 0.97 % / 0.56 %)
- seedstone vs redis-iot4: ahead 1.52x on throughput; more expensive per operation 3.34x (spreads 1.13 % / 1.89 %)
- seedstone vs valkey-iot1: ahead 1.87x on throughput; more expensive per operation 3.65x (spreads 0.97 % / 1.03 %)
- seedstone vs valkey-iot4: ahead 1.30x on throughput; more expensive per operation 1.31x (spreads 0.97 % / 0.74 %)
- seedstone vs dragonfly: ahead 1.58x on throughput; cheaper per operation 0.50x (spreads 0.97 % / 0.56 %)
- seedstone vs garnet: ahead 1.04x on throughput; more expensive per operation 1.39x (spreads 1.11 % / 13.73 %)

The two quantities point opposite ways against all four Redis and Valkey arms
and against `garnet`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

#### 16 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 625 000 | 11.750 | 1.110 | 12.800 | 8.00 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 336 022 | 2.700 | 0.290 | 2.990 | 1.00 | 0.53 | 0.000 | 1.860 |
| redis-iot4 | 397 772 | 4.540 | 4.850 | 9.410 | 3.73 | 0.63 | 0.000 | 1.571 |
| valkey-iot1 | 313 676 | 2.920 | 0.260 | 3.180 | 1.00 | 0.49 | 0.000 | 1.993 |
| valkey-iot4 | 441 501 | 8.760 | 0.320 | 9.080 | 4.01 | 0.70 | 0.000 | 1.416 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 625 000 | 11.750 | 1.110 | 12.800 | 8.00 | 1.00 | 0.000 | 1.000 |
| dragonfly | 463 177 | 15.100 | 2.420 | 17.500 | 8.11 | 0.76 | 0.000 | 1.349 |
| garnet | 628 536 | 5.160 | 1.300 | 6.460 | 4.06 | 1.00 | - | 0.994 |

- seedstone vs redis-iot1: ahead 1.86x on throughput; more expensive per operation 4.28x (spreads 1.07 % / 1.95 %)
- seedstone vs redis-iot4: ahead 1.57x on throughput; more expensive per operation 1.36x (spreads 0.31 % / 3.61 %)
- seedstone vs valkey-iot1: ahead 1.99x on throughput; more expensive per operation 4.03x (spreads 0.53 % / 1.95 %)
- seedstone vs valkey-iot4: ahead 1.42x on throughput; more expensive per operation 1.41x (spreads 0.31 % / 1.95 %)
- seedstone vs dragonfly: ahead 1.35x on throughput; cheaper per operation 0.73x (spreads 1.07 % / 1.95 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.98x (spreads 0.31 % / 1.95 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

### KEYS over 7 000 keys — a page cache's walk

`KEYS` against a glob at pipeline depth 1, 50 connections, 20 000 calls per run,
over a keyspace of 7 000 keys of 10 240 bytes under 64 prefixes, 199 bytes a
key. The keyspace is written by `bench/keys-load.sh` rather than by
`redis-benchmark`, and key `i` is a function of `i` alone, so every arm and
every run walks the identical 7 000 keys; each arm's `dbsize` is read back and
printed in `07-keys.log`. One fixed prefix of the 64 is matched per call, so
every call answers the same 110 keys. Depth 1 because the deployment these
shapes are sized from does not pipeline a `KEYS` call. `W` = 1 discarded run
per arm, then three kept.

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 537 | 2785.500 | 33.500 | 2819.000 | 9.96 | 0.07 | 0.000 | 1.000 |
| redis-iot1 | 686 | 1449.500 | 9.500 | 1458.000 | 1.00 | 0.01 | 0.000 | 5.159 |
| redis-iot4 | 696 | 1448.000 | 44.000 | 1492.000 | 1.04 | 0.02 | 0.000 | 5.080 |
| valkey-iot1 | 701 | 1418.000 | 9.000 | 1427.500 | 1.00 | 0.01 | 0.000 | 5.047 |
| valkey-iot4 | 710 | 2805.500 | 10.000 | 2815.500 | 2.00 | 0.02 | 0.000 | 4.978 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 537 | 2785.500 | 33.500 | 2819.000 | 9.96 | 0.07 | 0.000 | 1.000 |
| dragonfly | 2 137 | 2263.000 | 86.500 | 2349.500 | 5.02 | 0.04 | 0.000 | 1.655 |
| garnet | 1 517 | 6462.000 | 96.500 | 6558.500 | 9.95 | 0.03 | - | 2.332 |

- seedstone vs redis-iot1: ahead 5.16x on throughput; more expensive per operation 1.93x (spreads 0.32 % / 0.30 %)
- seedstone vs redis-iot4: ahead 5.08x on throughput; more expensive per operation 1.89x (spreads 0.32 % / 1.64 %)
- seedstone vs valkey-iot1: ahead 5.05x on throughput; more expensive per operation 1.97x (spreads 0.32 % / 0.30 %)
- seedstone vs valkey-iot4: ahead 4.98x on throughput; indistinguishable (spreads 0.32 % / 0.30 %)
- seedstone vs dragonfly: ahead 1.66x on throughput; more expensive per operation 1.20x (spreads 1.41 % / 0.30 %)
- seedstone vs garnet: ahead 2.33x on throughput; cheaper per operation 0.43x (spreads 0.72 % / 1.04 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`,
`valkey-iot1` and `dragonfly`: this server is ahead on throughput and more
expensive per operation against each of them, and both readings are of the
same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

**This server's own figure on this row is within about 1 % of `v0.2.0`'s.** It
answered 3 579 calls per second at 2 783.5 µs of CPU a call then and 3 537 at
2 819.0 µs now; `redis-iot1` answered 688 then and 686 now. Both differences
are under the 2 % floor the pair rule reads as *indistinguishable*. The `v0.3.0`
and `v0.3.1` runs read this figure lower, which is why neither was published
([#102](https://github.com/stefanobaldo/seedstone/issues/102)).

**This server answers 5.16× the `KEYS` calls per second of Redis 8.10.0 at one
I/O thread and spends 1.93× the CPU on each of them, and that is the shape of
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
reported seven errors and 7 006 replies where the other six arms each reported no
error and 7 000. The keyspace every arm then ran against is the same — 7 000
keys of 10 240 B over 64 prefixes, `dbsize=7000`, read back from each server and
printed in `07-keys.log`, `dragonfly` included. The seven sit in the loader's
exchange, before the measurement, and the measurement ran on the same keyspace
as every other arm's. It is stated here rather than left in the log.

### SET 64 B under a synced log — the write path with a durability promise

`SET` of a 64-byte value at pipeline depths 64 and 1, 50 connections, keys
spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs, under each setting of each engine's log: this server's
`--fsync never`, `interval` and `always` against Redis 8.10.0 with
`appendonly yes` and `appendfsync no`, `everysec` and `always`, and both
engines with no log at all (`seedstone` and `redis-iot1`, started as in every
other cell). Then `GET` of a 64-byte value at depth 64 with this server's log
synced on every write, against no log. Every Redis arm runs at one I/O thread.
Each arm writes into a directory of its own on the machine's boot disk — a GCP
`hyperdisk-balanced` volume of 40 GB, provisioned at 3 240 IOPS and 200 MB/s —
emptied before the arm starts; the device the directory is on is printed at
the head of `08-durability.log`. `W` = 1 discarded run per arm per row, then
three kept.

**Why these pairs.** Each line pairs the two settings that make the nearest
promise. `--fsync always` against `appendfsync always` is the same promise on
both sides: this server acknowledges a write only once it is on disk, and
Redis 8.10.0's `redis.conf` describes `always` as an fsync after every write
to its log. `--fsync interval` against `appendfsync everysec` is **not** the
same promise: this server syncs once 100 ms have passed since its last sync,
Redis once a second, so the window a crash can cost is a tenth as wide on one
side as on the other. The pair is shown because each is its engine's default —
`interval` here, `everysec` in Redis 8.10.0's `redis.conf` — which is the
setting an operator who does not choose one is running. `--fsync never`
against `appendfsync no` leaves the sync to the kernel on both, except that
this server also syncs when its log moves to its next file and at a clean
stop.

**Why only Redis.** The harness pairs a log with Redis alone. Valkey is a fork
of Redis (Valkey 9.1.1 reports `redis_version:7.2.4`), and the harness
measures that log's design once, on Redis. Dragonfly `df-v1.40.2` and Garnet
2.1.5 have no arm here because the harness has none that pairs with a log
synced on every write; that says what this harness runs, not what either
engine can do.

The tables carry no `×seedstone` column: each line pairs an arm with the arm
at the matching setting, not with one reference.

#### GET at depth 64, with the log synced on every write

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op |
|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.170 | 0.290 | 1.450 | 6.71 | 1.01 | 0.000 |
| seedstone-always | 4 629 630 | 1.230 | 0.250 | 1.490 | 6.90 | 1.01 | 0.000 |

- seedstone-always vs seedstone: indistinguishable on throughput; more expensive per operation 1.03x (spreads 0.46 % / 2.01 %)

#### SET at depth 1

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op |
|---|---|---|---|---|---|---|---|
| seedstone | 142 592 | 5.270 | 5.870 | 11.130 | 1.59 | 1.00 | 0.000 |
| redis-iot1 | 138 198 | 1.620 | 4.650 | 6.270 | 0.87 | 1.00 | 0.000 |
| seedstone-never | 141 323 | 8.560 | 10.130 | 18.550 | 2.62 | 0.99 | 0.000 |
| redis-aof-no | 146 284 | 1.920 | 4.670 | 6.590 | 0.96 | 0.99 | 0.000 |
| seedstone-interval | 142 755 | 8.770 | 10.490 | 19.280 | 2.74 | 0.99 | 0.000 |
| redis-aof-everysec | 146 477 | 1.910 | 4.710 | 6.630 | 0.97 | 1.00 | 0.000 |
| seedstone-always | 14 004 | 10.390 | 11.210 | 21.600 | 0.30 | 0.10 | 0.000 |
| redis-aof-always | 13 847 | 1.680 | 5.070 | 6.700 | 0.09 | 0.10 | 0.000 |

- seedstone vs redis-iot1: ahead 1.03x on throughput; more expensive per operation 1.78x (spreads 0.99 % / 1.44 %)
- seedstone-never vs redis-aof-no: behind 0.97x on throughput; more expensive per operation 2.81x (spreads 1.15 % / 1.67 %)
- seedstone-interval vs redis-aof-everysec: behind 0.97x on throughput; more expensive per operation 2.91x (spreads 2.05 % / 1.92 %)
- seedstone-always vs redis-aof-always: indistinguishable on throughput; more expensive per operation 3.22x (spreads 22.16 % / 1.44 %)

#### SET at depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op |
|---|---|---|---|---|---|---|---|
| seedstone | 4 608 295 | 1.350 | 0.260 | 1.610 | 7.42 | 1.01 | 0.000 |
| redis-iot1 | 2 087 683 | 0.410 | 0.070 | 0.480 | 1.00 | 0.43 | 0.000 |
| seedstone-never | 2 192 982 | 1.830 | 0.230 | 2.060 | 4.56 | 0.47 | 0.000 |
| redis-aof-no | 1 200 480 | 0.690 | 0.130 | 0.820 | 0.98 | 0.25 | 0.000 |
| seedstone-interval | 2 132 196 | 1.850 | 0.220 | 2.080 | 4.48 | 0.45 | 0.000 |
| redis-aof-everysec | 1 199 041 | 0.680 | 0.130 | 0.820 | 0.98 | 0.25 | 0.000 |
| seedstone-always | 597 015 | 2.010 | 0.340 | 2.350 | 1.39 | 0.13 | 0.000 |
| redis-aof-always | 516 529 | 0.710 | 0.160 | 0.870 | 0.45 | 0.11 | 0.000 |

- seedstone vs redis-iot1: ahead 2.21x on throughput; more expensive per operation 3.35x (spreads 0.93 % / 0.62 %)
- seedstone-never vs redis-aof-no: ahead 1.83x on throughput; more expensive per operation 2.51x (spreads 24.14 % / 2.43 %)
- seedstone-interval vs redis-aof-everysec: ahead 1.78x on throughput; more expensive per operation 2.54x (spreads 30.49 % / 2.44 %)
- seedstone-always vs redis-aof-always: indistinguishable on throughput; more expensive per operation 2.70x (spreads 23.07 % / 2.30 %)

The `GET` row is the read path with the log synced on every write: against no
log it is indistinguishable on throughput and 1.03× the CPU per operation.

At depth 1 the two quantities point opposite ways only on the line with no
log, where this server is ahead on throughput and more expensive per
operation. With a log, no pair is a trade: `--fsync never` and `interval` are
*behind* 0.97× their Redis settings and `always` is *indistinguishable*, and
this server is more expensive per operation on every line, by 1.78× to 3.22×.
Under `always` both engines answer about a tenth of the writes per second they
answer with no log: 14 004 and 13 847, against 142 592 and 138 198.

At depth 64 the two quantities point opposite ways on three lines — no log,
`never` and `interval`: this server is ahead on throughput and more expensive
per operation against each of those Redis arms, and both readings are of the
same runs. Under `always` it is *indistinguishable* on throughput and 2.70×
the CPU per operation.

**The spreads on the logged arms are wide, and on the depth-64 row they decide
a word.** This server's `never`, `interval` and `always` arms spread 24.14 %,
30.49 % and 23.07 % over their three kept runs at depth 64, and Redis's `no`
arm 19.03 %, against 0.96 % and 5.98 % for Redis's `everysec` and `always` and
under 1 % for the two engines with no log. The `never` and `interval` ratios
clear the spread their lines print. The `always` pair does not: its medians,
597 015 against 516 529 writes per second, are 1.16× apart, inside this
server's own 23.07 %, so the pair reads *indistinguishable*, and three kept
runs do not resolve it. At depth 1 both engines' `always` arms are wide too,
18.17 % and 22.16 %. Each arm's three kept runs are in the log.

**What this cell does not say.** One disk class and one provisioning: a cloud
block device at 3 240 IOPS, not a local NVMe drive, and a different disk is a
different table. Redis rewrote its log in the background during each of its
logged arms — 12, 12 and 13 times under `no`, `everysec` and `always`, counted
from Redis's own log and printed per arm in `08-durability.log` with the
rewrite thresholds it ran at, Redis 8.10.0's defaults; the cell carries those
rewrites and does not separate them out. No latency percentiles, so nothing
here says how long an acknowledged write waited for its sync. No crash was
induced: the cell measures what each promise costs, not whether it is kept.

## What these numbers do not say

- The client and the server share one machine over loopback; there is no
  network.
- One machine class — ARM Neoverse-V2 (Google Axion), 16 cores, one thread
  per core. A re-run on x86, or on a machine with SMT, is a different table.
- `redis-benchmark` is the only load generator, and it is the load generator
  of the Redis version measured.
- No latency percentiles. No memory footprint, resident or accounted.
  Persistence only in the durability cell, and only on the two engines it
  pairs.
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
- **The two runs compared on this page ran on different kernels, in different
  zones.** This run stands on `7.0.0-1014-gcp` and the `v0.2.0` run whose
  figures are quoted under the changed pairs on `7.0.0-1011-gcp`, on the same
  machine class; each kernel is echoed at the head of its run's logs. This
  run's machine was in `us-central1-b`, the `v0.2.0` run's in `us-central1-a`.
  The canary is the only instrument this page has for deciding whether two
  runs are comparable at all, and it passed here at `+3.70 %` against the same
  reference the `v0.2.0` run read `+4.26 %` against, both inside the ±5 % fixed
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
WARMUP=<W> DATA_ROOT=<dir> bash bench/campaign.sh durability > 08-durability.log
python3 bench/report.py 0[3-8]-*.log
```

`DATA_ROOT` is the directory the durability cell's arms write into, one
directory each; put it on the disk that is meant to be measured, not on a
`tmpfs`.

`W` was 1 for this run, and the calibration derives it again rather than
assuming it. A calibration in which an arm never settles gives no `W`, and is
run again rather than read without that arm. The canary
decides whether a re-run is comparable to the tables on this page. A re-run on
other hardware is a different table, not a correction of this one, and is read
on its own terms.

## Re-measurement

The same harness, durability stage included, runs at every minor release. The
new tables replace these, and this run's raw logs stay under
`bench/results/v0.3.2/`.
