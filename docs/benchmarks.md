# Benchmarks

**Release measured:** `v0.1.0` (commit `91f3a70`). **Date:** 2026-09-05.
**Machine:** GCP `c4a-standard-16`. **Engines:** Redis 8.10.0, Valkey 9.1.1,
Dragonfly `df-v1.40.2`, Garnet 2.1.5 — each as the engine itself reports its
version, not as its release archive is named. **Warm-up runs discarded per arm
per cell (`W`):** 3, derived by the calibration below.

The method below was committed before the run; the tables were added after
it. The raw logs the tables are computed from are in
[`bench/results/v0.1.0/`](../bench/results/v0.1.0/), and
`python3 bench/report.py bench/results/v0.1.0/0[3-6]-*.log` regenerates every
table and every reading on this page. A number here without its method beside it
would be a marketing number, so the method comes first.

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
than suspected. Its highest reading across the 204 kept runs in the tables below
is 1.01 cores, and no row reaches higher; if a re-run shows more, that row is
about the client.

**The engines, and how each was configured.** Each engine receives the
configuration that matches the hardware it is given, where it has a knob for
that, and nothing else — no allocator, hugepage or affinity tuning, for any
of them, this server included.

| arm | version and provenance | started as |
|---|---|---|
| seedstone | `v0.1.0`, built on the machine with `cargo build --release --locked -p seedstone` | `--bind 127.0.0.1:6390 --max-clients 2000 --no-auth` |
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
read was already known to be a shape this server is behind Redis on, and it is
here for that reason; the tables below say where else it is behind, and that
this is the one cell where it is behind every comparator.

## How

**One run** is a million operations of `redis-benchmark` against a server
already up, with 50 connections, keys spread uniformly over 100 000 keys
(`-r 100000`), and the server's CPU read once from `/proc/<pid>/stat`
immediately before and after — user and system time over every thread, at the
kernel's clock tick. Read once around a million operations the window is
thousands of ticks wide; read around a single operation it would be quantised
to nothing. `bench/cell.sh` is one run.

**Populated, and declared.** Before every read cell the keyspace is written
by the same step (`-t set -n 300000 -c 50 -P 64 -d 64 -r 100000`) and probed
by reading keys back; the probe's hit count is in the log. A `GET` against an
empty keyspace measures the miss path, which is a different and faster path.
The eviction cell is the exception: it starts empty, fills past its ceiling,
and says so.

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
rewrite of the script that produced the canary's reference figure. Before
this run it was measured against that script on one Redis lifetime, the two
alternating, and agreed within 2 % on throughput and on CPU per operation.
The new instrument measures what the old one measured.

**What was discarded, and where it is.** The calibration runs (all of them),
the warm-up runs (`W` per arm per cell), and the fill that drives the
eviction cell past its ceiling. Every one is in the raw logs, marked.

## The tables

Every table and every pair line below is the output of

```sh
python3 bench/report.py bench/results/v0.1.0/0[3-6]-*.log
```

over the raw logs in [`bench/results/v0.1.0/`](../bench/results/v0.1.0/),
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
| seedstone | 140 786 | 4.540 | 5.630 | 10.090 | 1.42 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 131 596 | 1.520 | 4.620 | 6.210 | 0.82 | 1.00 | 0.000 | 1.070 |
| redis-iot4 | 121 936 | 4.530 | 11.200 | 16.000 | 1.95 | 1.00 | 0.000 | 1.155 |
| valkey-iot1 | 131 354 | 1.740 | 4.360 | 6.100 | 0.80 | 1.00 | 0.000 | 1.072 |
| valkey-iot4 | 135 851 | 9.760 | 4.890 | 14.430 | 1.94 | 1.00 | 0.000 | 1.036 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 140 786 | 4.540 | 5.630 | 10.090 | 1.42 | 1.00 | 0.000 | 1.000 |
| dragonfly | 127 405 | 19.940 | 11.200 | 31.130 | 3.97 | 1.00 | 0.000 | 1.105 |
| garnet | 121 300 | 30.640 | 20.180 | 50.060 | 6.02 | 1.00 | - | 1.161 |

- seedstone vs redis-iot1: ahead 1.07x on throughput; more expensive per operation 1.62x (spreads 4.23 % / 2.58 %)
- seedstone vs redis-iot4: ahead 1.15x on throughput; cheaper per operation 0.63x (spreads 14.66 % / 3.94 %)
- seedstone vs valkey-iot1: ahead 1.07x on throughput; more expensive per operation 1.65x (spreads 3.63 % / 4.92 %)
- seedstone vs valkey-iot4: ahead 1.04x on throughput; cheaper per operation 0.70x (spreads 2.53 % / 2.84 %)
- seedstone vs dragonfly: ahead 1.11x on throughput; cheaper per operation 0.32x (spreads 4.49 % / 2.08 %)
- seedstone vs garnet: ahead 1.16x on throughput; cheaper per operation 0.20x (spreads 2.78 % / 2.38 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

#### Depth 4

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 498 008 | 3.960 | 2.700 | 6.660 | 3.34 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 523 560 | 0.690 | 1.060 | 1.760 | 0.92 | 1.00 | 0.000 | 0.951 |
| redis-iot4 | 484 731 | 1.410 | 2.730 | 4.170 | 2.03 | 1.00 | 0.000 | 1.027 |
| valkey-iot1 | 519 481 | 0.680 | 1.100 | 1.780 | 0.93 | 1.00 | 0.000 | 0.959 |
| valkey-iot4 | 525 486 | 2.610 | 1.180 | 3.790 | 1.99 | 1.00 | 0.000 | 0.948 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 498 008 | 3.960 | 2.700 | 6.660 | 3.34 | 1.00 | 0.000 | 1.000 |
| dragonfly | 504 286 | 8.050 | 3.690 | 11.740 | 5.92 | 1.00 | 0.000 | 0.988 |
| garnet | 463 822 | 8.260 | 5.200 | 13.410 | 6.34 | 1.00 | - | 1.074 |

- seedstone vs redis-iot1: behind 0.95x on throughput; more expensive per operation 3.78x (spreads 3.21 % / 1.70 %)
- seedstone vs redis-iot4: indistinguishable on throughput; more expensive per operation 1.60x (spreads 4.01 % / 2.64 %)
- seedstone vs valkey-iot1: indistinguishable on throughput; more expensive per operation 3.74x (spreads 7.36 % / 1.12 %)
- seedstone vs valkey-iot4: behind 0.95x on throughput; more expensive per operation 1.76x (spreads 1.46 % / 1.32 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.57x (spreads 13.73 % / 8.69 %)
- seedstone vs garnet: ahead 1.07x on throughput; cheaper per operation 0.50x (spreads 3.22 % / 3.06 %)

Tracked as #32.

#### Depth 16

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 805 054 | 2.680 | 0.860 | 3.530 | 6.35 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 700 680 | 0.320 | 0.270 | 0.590 | 1.00 | 0.89 | 0.000 | 1.061 |
| redis-iot4 | 1 680 672 | 0.620 | 0.630 | 1.240 | 2.08 | 1.01 | 0.000 | 1.074 |
| valkey-iot1 | 1 543 210 | 0.410 | 0.240 | 0.650 | 1.00 | 0.84 | 0.000 | 1.170 |
| valkey-iot4 | 1 824 818 | 0.820 | 0.270 | 1.100 | 2.01 | 1.00 | 0.000 | 0.989 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 805 054 | 2.680 | 0.860 | 3.530 | 6.35 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 686 341 | 2.900 | 1.080 | 3.980 | 6.75 | 1.00 | 0.000 | 1.070 |
| garnet | 1 602 564 | 2.380 | 1.520 | 3.980 | 6.50 | 1.00 | - | 1.126 |

- seedstone vs redis-iot1: ahead 1.06x on throughput; more expensive per operation 5.98x (spreads 3.90 % / 1.70 %)
- seedstone vs redis-iot4: ahead 1.07x on throughput; more expensive per operation 2.85x (spreads 6.72 % / 4.84 %)
- seedstone vs valkey-iot1: ahead 1.17x on throughput; more expensive per operation 5.43x (spreads 3.90 % / 3.08 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 3.21x (spreads 3.90 % / 2.73 %)
- seedstone vs dragonfly: ahead 1.07x on throughput; cheaper per operation 0.89x (spreads 4.36 % / 2.51 %)
- seedstone vs garnet: ahead 1.13x on throughput; indistinguishable (spreads 4.17 % / 12.31 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

#### Depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 201 680 | 1.880 | 0.330 | 2.210 | 9.29 | 0.90 | 0.000 | 1.000 |
| redis-iot1 | 2 564 103 | 0.290 | 0.100 | 0.390 | 1.00 | 0.53 | 0.000 | 1.639 |
| redis-iot4 | 3 184 713 | 0.410 | 0.080 | 0.490 | 1.56 | 0.76 | 0.000 | 1.319 |
| valkey-iot1 | 2 247 191 | 0.380 | 0.070 | 0.440 | 0.99 | 0.46 | 0.000 | 1.870 |
| valkey-iot4 | 3 558 719 | 1.050 | 0.080 | 1.150 | 4.09 | 0.79 | 0.000 | 1.181 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 201 680 | 1.880 | 0.330 | 2.210 | 9.29 | 0.90 | 0.000 | 1.000 |
| dragonfly | 4 608 295 | 1.240 | 0.330 | 1.570 | 7.24 | 1.01 | 0.000 | 0.912 |
| garnet | 4 329 004 | 1.010 | 0.510 | 1.480 | 6.43 | 1.01 | - | 0.971 |

- seedstone vs redis-iot1: ahead 1.64x on throughput; more expensive per operation 5.67x (spreads 0.76 % / 2.56 %)
- seedstone vs redis-iot4: ahead 1.32x on throughput; more expensive per operation 4.51x (spreads 0.42 % / 0.45 %)
- seedstone vs valkey-iot1: ahead 1.87x on throughput; more expensive per operation 5.02x (spreads 0.42 % / 2.27 %)
- seedstone vs valkey-iot4: ahead 1.18x on throughput; more expensive per operation 1.92x (spreads 0.42 % / 1.74 %)
- seedstone vs dragonfly: behind 0.91x on throughput; more expensive per operation 1.41x (spreads 1.83 % / 1.27 %)
- seedstone vs garnet: behind 0.97x on throughput; more expensive per operation 1.49x (spreads 1.29 % / 8.11 %)

Tracked as #33.

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`,
`valkey-iot1` and `valkey-iot4`: this server is ahead on throughput and more
expensive per operation against each of them, and both readings are of the
same runs.

Against `dragonfly` and `garnet` on this row the two quantities agree rather
than disagree — behind on throughput and more expensive per operation is one
direction, and this row is not a trade against either of them.

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
| seedstone | 4 310 345 | 1.890 | 0.280 | 2.170 | 9.32 | 0.93 | 0.000 | 1.000 |
| redis-iot1 | 2 012 072 | 0.420 | 0.080 | 0.500 | 1.01 | 0.43 | 0.000 | 2.142 |
| redis-iot4 | 2 364 066 | 0.510 | 0.100 | 0.610 | 1.44 | 0.57 | 0.000 | 1.823 |
| valkey-iot1 | 1 751 314 | 0.490 | 0.080 | 0.570 | 1.00 | 0.37 | 0.000 | 2.461 |
| valkey-iot4 | 2 557 545 | 1.050 | 0.140 | 1.180 | 3.03 | 0.59 | 0.000 | 1.685 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 310 345 | 1.890 | 0.280 | 2.170 | 9.32 | 0.93 | 0.000 | 1.000 |
| dragonfly | 4 504 504 | 1.350 | 0.240 | 1.590 | 7.16 | 1.01 | 0.000 | 0.957 |
| garnet | 4 329 004 | 1.120 | 0.520 | 1.640 | 7.10 | 1.01 | - | 0.996 |

- seedstone vs redis-iot1: ahead 2.14x on throughput; more expensive per operation 4.34x (spreads 1.29 % / 4.00 %)
- seedstone vs redis-iot4: ahead 1.82x on throughput; more expensive per operation 3.56x (spreads 1.29 % / 4.92 %)
- seedstone vs valkey-iot1: ahead 2.46x on throughput; more expensive per operation 3.81x (spreads 1.29 % / 1.38 %)
- seedstone vs valkey-iot4: ahead 1.69x on throughput; more expensive per operation 1.84x (spreads 1.29 % / 1.38 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.36x (spreads 5.51 % / 2.52 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.32x (spreads 1.73 % / 10.98 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`,
`valkey-iot1` and `valkey-iot4`: this server is ahead on throughput and more
expensive per operation against each of them, and both readings are of the
same runs.

#### With `EX 60`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 2 840 909 | 2.600 | 0.620 | 3.220 | 9.15 | 0.65 | 0.000 | 1.000 |
| redis-iot1 | 1 522 070 | 0.580 | 0.080 | 0.660 | 1.00 | 0.33 | 0.000 | 1.866 |
| redis-iot4 | 1 736 111 | 0.670 | 0.120 | 0.790 | 1.37 | 0.41 | 0.000 | 1.636 |
| valkey-iot1 | 1 101 322 | 0.810 | 0.100 | 0.910 | 1.00 | 0.24 | 0.000 | 2.580 |
| valkey-iot4 | 1 550 388 | 2.470 | 0.130 | 2.600 | 4.03 | 0.38 | 0.000 | 1.832 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 2 840 909 | 2.600 | 0.620 | 3.220 | 9.15 | 0.65 | 0.000 | 1.000 |
| dragonfly | 4 524 887 | 1.560 | 0.190 | 1.740 | 7.87 | 1.01 | 0.000 | 0.628 |
| garnet | 4 310 345 | 1.140 | 0.550 | 1.670 | 7.19 | 1.01 | - | 0.659 |

- seedstone vs redis-iot1: ahead 1.87x on throughput; more expensive per operation 4.88x (spreads 11.34 % / 9.94 %)
- seedstone vs redis-iot4: ahead 1.64x on throughput; more expensive per operation 4.08x (spreads 11.34 % / 9.94 %)
- seedstone vs valkey-iot1: ahead 2.58x on throughput; more expensive per operation 3.54x (spreads 11.34 % / 9.94 %)
- seedstone vs valkey-iot4: ahead 1.83x on throughput; more expensive per operation 1.24x (spreads 11.34 % / 9.94 %)
- seedstone vs dragonfly: behind 0.63x on throughput; more expensive per operation 1.85x (spreads 11.34 % / 9.94 %)
- seedstone vs garnet: behind 0.66x on throughput; more expensive per operation 1.93x (spreads 11.34 % / 12.57 %)

Tracked as #34.

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`,
`valkey-iot1` and `valkey-iot4`: this server is ahead on throughput and more
expensive per operation against each of them, and both readings are of the
same runs.

Against `dragonfly` and `garnet` on this row, as at depth 64 above, the two
quantities agree — behind on throughput and more expensive per operation — and
the row is not a trade against either of them.

**Every pair line on this row prints the same two spreads, 11.34 % and 9.94 %,
and both of them are this server's own.** Its three kept runs here are
2 717 391, 2 840 909 and 3 039 514 operations per second: a spread of 11.34 %,
the widest of any arm anywhere in this run and wider on this row than any
comparator's, so it is the one the rule takes six times out of six. What decides
the words on this row is therefore this server's own run-to-run variation under
`SET … EX`, and neither a property of the machine nor a property of the
comparators.

### SET 10 240 B past a 384 MB ceiling — the write path under eviction

`SET` of a 10 240-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, against a server started with a 384 MB memory
ceiling. Unlike every other cell this one starts empty: the keyspace is filled
past the ceiling first and that fill is discarded, then `W` = 3 discarded runs,
then three kept — so every kept run is in steady-state eviction rather than
still filling. The `evicted/op` column is the cell's own declaration that this
held: about 0.61 keys evicted per operation, in every arm.

**The ceiling and the policy, per arm.** This server, Redis and Valkey each run
`--maxmemory 384mb --maxmemory-policy allkeys-lru`; the start line of every arm
is in `05-eviction.log`.

**Two engines are absent from this table, and each absence is that engine's
property, not this cell's.** Garnet bounds memory by a log size with tail
reclamation rather than by a ceiling with LRU eviction, so the comparable cell
does not exist for it. Dragonfly requires 256 MiB of `maxmemory` per proactor
thread and refuses to start below that: at the ten proactor threads this
hardware gives it, the smallest ceiling it accepts is 2.50 GiB, which is above
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
| seedstone | 585 823 | 5.390 | 3.190 | 8.570 | 5.02 | 1.01 | 0.612 | 1.000 |
| redis-iot1 | 233 209 | 1.910 | 2.380 | 4.290 | 1.00 | 0.48 | 0.677 | 2.512 |
| redis-iot4 | 610 128 | 2.950 | 3.110 | 6.060 | 3.77 | 1.01 | 0.678 | 0.960 |
| valkey-iot1 | 230 680 | 1.920 | 2.420 | 4.340 | 1.00 | 0.48 | 0.678 | 2.540 |
| valkey-iot4 | 586 854 | 4.930 | 1.880 | 6.720 | 3.94 | 1.01 | 0.679 | 0.998 |

- seedstone vs redis-iot1: ahead 2.51x on throughput; more expensive per operation 2.00x (spreads 3.46 % / 0.47 %)
- seedstone vs redis-iot4: behind 0.96x on throughput; more expensive per operation 1.41x (spreads 3.46 % / 3.63 %)
- seedstone vs valkey-iot1: ahead 2.54x on throughput; more expensive per operation 1.97x (spreads 3.46 % / 0.92 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.28x (spreads 18.25 % / 15.03 %)

Tracked as #35.

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

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
| seedstone | 2 770 083 | 3.420 | 0.190 | 3.610 | 10.00 | 0.75 | 0.000 | 1.000 |
| redis-iot1 | 2 493 766 | 0.330 | 0.070 | 0.400 | 1.00 | 0.63 | 0.000 | 1.111 |
| redis-iot4 | 3 039 514 | 0.410 | 0.090 | 0.500 | 1.52 | 0.85 | 0.000 | 0.911 |
| valkey-iot1 | 2 188 184 | 0.380 | 0.070 | 0.460 | 1.01 | 0.56 | 0.000 | 1.266 |
| valkey-iot4 | 3 412 969 | 1.120 | 0.080 | 1.200 | 4.10 | 0.91 | 0.000 | 0.812 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 2 770 083 | 3.420 | 0.190 | 3.610 | 10.00 | 0.75 | 0.000 | 1.000 |
| dragonfly | 3 802 281 | 1.810 | 0.290 | 2.100 | 7.98 | 1.01 | 0.000 | 0.729 |
| garnet | 3 623 188 | 1.140 | 0.570 | 1.710 | 6.13 | 1.01 | - | 0.765 |

- seedstone vs redis-iot1: ahead 1.11x on throughput; more expensive per operation 9.02x (spreads 0.99 % / 0.28 %)
- seedstone vs redis-iot4: behind 0.91x on throughput; more expensive per operation 7.22x (spreads 0.00 % / 2.00 %)
- seedstone vs valkey-iot1: ahead 1.27x on throughput; more expensive per operation 7.85x (spreads 0.65 % / 4.35 %)
- seedstone vs valkey-iot4: behind 0.81x on throughput; more expensive per operation 3.01x (spreads 0.34 % / 0.28 %)
- seedstone vs dragonfly: behind 0.73x on throughput; more expensive per operation 1.72x (spreads 1.14 % / 0.28 %)
- seedstone vs garnet: behind 0.76x on throughput; more expensive per operation 2.11x (spreads 1.08 % / 3.51 %)

Tracked as #36.

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**`s_ops` reads 0.00 % against `redis-iot4` on this row, and that is the load
generator's rate quantisation rather than a noiseless measurement.** Both arms
report three byte-identical kept runs — 2 770 083.00 three times, 3 039 513.75
three times — because `redis-benchmark` reports a rate that lands on the same
value at these magnitudes. The word on that line is decided against the rule's
2 % floor, not against a 0 % spread, which is what the floor exists for.

#### 4 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 805 153 | 11.270 | 0.890 | 12.170 | 9.80 | 0.45 | 0.000 | 1.000 |
| redis-iot1 | 1 027 749 | 0.870 | 0.100 | 0.980 | 1.01 | 0.53 | 0.000 | 0.783 |
| redis-iot4 | 1 136 364 | 1.010 | 0.130 | 1.140 | 1.30 | 0.64 | 0.000 | 0.709 |
| valkey-iot1 | 998 004 | 0.890 | 0.110 | 1.000 | 1.00 | 0.52 | 0.000 | 0.807 |
| valkey-iot4 | 1 430 615 | 2.690 | 0.130 | 2.820 | 4.03 | 0.79 | 0.000 | 0.563 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 805 153 | 11.270 | 0.890 | 12.170 | 9.80 | 0.45 | 0.000 | 1.000 |
| dragonfly | 1 160 093 | 6.470 | 1.010 | 7.480 | 8.68 | 0.64 | 0.000 | 0.694 |
| garnet | 1 811 594 | 2.160 | 0.660 | 2.820 | 5.11 | 1.01 | - | 0.444 |

- seedstone vs redis-iot1: behind 0.78x on throughput; more expensive per operation 12.42x (spreads 0.21 % / 1.02 %)
- seedstone vs redis-iot4: behind 0.71x on throughput; more expensive per operation 10.68x (spreads 0.23 % / 0.88 %)
- seedstone vs valkey-iot1: behind 0.81x on throughput; more expensive per operation 12.17x (spreads 0.30 % / 2.00 %)
- seedstone vs valkey-iot4: behind 0.56x on throughput; more expensive per operation 4.32x (spreads 0.14 % / 0.16 %)
- seedstone vs dragonfly: behind 0.69x on throughput; more expensive per operation 1.63x (spreads 0.35 % / 0.16 %)
- seedstone vs garnet: behind 0.44x on throughput; more expensive per operation 4.32x (spreads 1.26 % / 4.61 %)

Tracked as #37.

#### 16 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 247 097 | 33.890 | 4.510 | 38.400 | 9.49 | 0.41 | 0.000 | 1.000 |
| redis-iot1 | 320 821 | 2.790 | 0.330 | 3.120 | 1.00 | 0.51 | 0.000 | 0.770 |
| redis-iot4 | 372 995 | 4.970 | 5.240 | 10.190 | 3.81 | 0.60 | 0.000 | 0.662 |
| valkey-iot1 | 296 824 | 3.040 | 0.330 | 3.370 | 1.00 | 0.47 | 0.000 | 0.832 |
| valkey-iot4 | 421 408 | 9.210 | 0.320 | 9.520 | 4.01 | 0.68 | 0.000 | 0.586 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 247 097 | 33.890 | 4.510 | 38.400 | 9.49 | 0.41 | 0.000 | 1.000 |
| dragonfly | 429 369 | 16.360 | 2.530 | 18.880 | 8.10 | 0.72 | 0.000 | 0.575 |
| garnet | 611 621 | 6.120 | 0.910 | 7.050 | 4.31 | 1.00 | - | 0.404 |

- seedstone vs redis-iot1: behind 0.77x on throughput; more expensive per operation 12.31x (spreads 3.54 % / 2.68 %)
- seedstone vs redis-iot4: behind 0.66x on throughput; more expensive per operation 3.77x (spreads 3.54 % / 2.68 %)
- seedstone vs valkey-iot1: behind 0.83x on throughput; more expensive per operation 11.39x (spreads 3.54 % / 2.68 %)
- seedstone vs valkey-iot4: behind 0.59x on throughput; more expensive per operation 4.03x (spreads 3.54 % / 2.68 %)
- seedstone vs dragonfly: behind 0.58x on throughput; more expensive per operation 2.03x (spreads 3.54 % / 2.68 %)
- seedstone vs garnet: behind 0.40x on throughput; more expensive per operation 5.45x (spreads 3.54 % / 3.12 %)

Tracked as #38.

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
- Two payloads (64 B, 10 240 B), one key distribution (100 000 spread keys),
  pipeline depth at most 64, 50 connections throughout.
- Garnet and Dragonfly are both absent from the eviction table, each for the
  reason stated there: Garnet bounds memory by a log size with tail
  reclamation rather than a ceiling with LRU, and Dragonfly refuses to start
  below 256 MiB of `maxmemory` per proactor thread, which at ten threads puts
  the smallest ceiling it accepts above everything this cell's keyspace can
  hold. The ceiling was declared before the run and was not moved to
  accommodate an engine.
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
python3 bench/report.py 0*.log
```

The canary decides whether a re-run is comparable to the tables on this page.
A re-run on other hardware is a different table, not a correction of this
one, and is read on its own terms.

## Re-measurement

The same harness runs at every minor release. The new tables replace these,
and this run's raw logs stay under `bench/results/v0.1.0/`.
