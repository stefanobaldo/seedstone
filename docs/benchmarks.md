# Benchmarks

**Release measured:** `v0.3.0` (commit `292d1fa`). **Date:** 2026-10-10.
**Machine:** GCP `c4a-standard-16`. **Engines:** Redis 8.10.0, Valkey 9.1.1,
Dragonfly `df-v1.40.2`, Garnet 2.1.5 — each as the engine itself reports its
version, not as its release archive is named. **Warm-up runs discarded per arm
per cell (`W`):** 5, derived by the calibration below.

The method below was committed before the run; the tables were added after
it. The raw logs the tables are computed from are in
[`bench/results/v0.3.0/`](../bench/results/v0.3.0/), and
`python3 bench/report.py bench/results/v0.3.0/0[3-8]-*.log` regenerates every
table and every pair line on this page. The only readings it does not produce
are the `v0.2.0` ones quoted under the pairs whose word changed; the same
command over that run's logs produces those. A number here without its method
beside it would be a marketing number, so the method comes first.

**The runs these tables replace are still here.** The raw logs of the `v0.2.0`
and `v0.1.0` runs stay under [`bench/results/v0.2.0/`](../bench/results/v0.2.0/)
and [`bench/results/v0.1.0/`](../bench/results/v0.1.0/), the same command over
either regenerates that run's tables unchanged, and wherever a pair's reading
changed since `v0.2.0`, a line under that pair says what it read then.

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
| seedstone | `v0.3.0`, built on the machine with `cargo build --release --locked -p seedstone` | `--bind 127.0.0.1:6390 --max-clients 2000 --no-auth` |
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
python3 bench/report.py bench/results/v0.3.0/0[3-8]-*.log
```

over the raw logs in [`bench/results/v0.3.0/`](../bench/results/v0.3.0/),
spliced into this page unedited. `W` is **5** for every cell in this run,
derived by the calibration and not chosen: Garnet needed five discarded runs
before it settled, Dragonfly three, and every other arm settled on its first
or second (`02b-calibrate.log`). That is the run's second calibration. In the
first (`02-calibrate.log`) Garnet never settled within its twelve runs, so no
`W` could be derived from it, and the calibration was run again rather than
read without that arm. Both logs are committed.

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
durability cell gets nothing because `v0.2.0` never ran it. Twenty of the 64
pairs the two runs share changed. Every throughput word that moved, sixteen
of them, moved to *indistinguishable*; every CPU word that moved, six of them,
moved from *indistinguishable* to *more expensive per operation*.

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
before each arm's runs (the probe's hit count is in `03-field.log`). `W` = 5
discarded runs per arm per depth, then three kept.

#### Depth 1

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 141 423 | 4.960 | 5.900 | 10.860 | 1.54 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 117 813 | 1.480 | 4.740 | 6.260 | 0.75 | 1.00 | 0.000 | 1.200 |
| redis-iot4 | 115 687 | 3.540 | 13.370 | 16.860 | 1.96 | 1.00 | 0.000 | 1.222 |
| valkey-iot1 | 139 198 | 1.230 | 4.660 | 5.880 | 0.82 | 1.00 | 0.000 | 1.016 |
| valkey-iot4 | 145 751 | 8.710 | 4.510 | 13.220 | 1.94 | 1.00 | 0.000 | 0.970 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 141 423 | 4.960 | 5.900 | 10.860 | 1.54 | 1.00 | 0.000 | 1.000 |
| dragonfly | 133 887 | 20.210 | 9.900 | 30.070 | 4.02 | 1.00 | 0.000 | 1.056 |
| garnet | 124 984 | 26.290 | 19.100 | 45.390 | 5.67 | 1.00 | - | 1.132 |

- seedstone vs redis-iot1: ahead 1.20x on throughput; more expensive per operation 1.73x (spreads 18.35 % / 5.91 %)
- seedstone vs redis-iot4: ahead 1.22x on throughput; cheaper per operation 0.64x (spreads 13.20 % / 5.89 %)
- seedstone vs valkey-iot1: indistinguishable on throughput; more expensive per operation 1.85x (spreads 6.41 % / 5.89 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; cheaper per operation 0.82x (spreads 6.41 % / 5.89 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.36x (spreads 6.41 % / 5.89 %)
- seedstone vs garnet: indistinguishable on throughput; cheaper per operation 0.24x (spreads 19.71 % / 12.51 %)

The two quantities point opposite ways against `redis-iot1`: this server is
ahead on throughput and more expensive per operation, and both readings are of
the same runs.

**The spreads on this row and the next are the widest in the field cell.** At
depth 1 they reach 19.71 % (Garnet) and 18.35 % (`redis-iot1`), at depth 4
16.45 % (`valkey-iot1`), against at most 7.47 % at depths 16 and 64. The rule
reads a ratio inside the spread as *indistinguishable*, and each line prints
the spread it was read against.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot1`: in `v0.2.0` this pair read *ahead* 1.06× on throughput.
- `dragonfly`: in `v0.2.0` this pair read *ahead* 1.09× on throughput.
- `garnet`: in `v0.2.0` this pair read *ahead* 1.14× on throughput.

#### Depth 4

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 527 704 | 3.940 | 2.930 | 6.790 | 3.58 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 506 842 | 0.570 | 1.170 | 1.750 | 0.89 | 1.00 | 0.000 | 1.041 |
| redis-iot4 | 483 092 | 1.200 | 3.140 | 4.350 | 2.07 | 1.00 | 0.000 | 1.092 |
| valkey-iot1 | 551 572 | 0.590 | 1.090 | 1.700 | 0.94 | 1.00 | 0.000 | 0.957 |
| valkey-iot4 | 529 381 | 3.020 | 1.220 | 4.240 | 2.23 | 1.00 | 0.000 | 0.997 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 527 704 | 3.940 | 2.930 | 6.790 | 3.58 | 1.00 | 0.000 | 1.000 |
| dragonfly | 514 668 | 8.240 | 3.320 | 11.560 | 5.95 | 1.00 | 0.000 | 1.025 |
| garnet | 480 077 | 7.450 | 5.320 | 12.770 | 5.72 | 1.00 | - | 1.099 |

- seedstone vs redis-iot1: indistinguishable on throughput; more expensive per operation 3.88x (spreads 11.11 % / 4.00 %)
- seedstone vs redis-iot4: indistinguishable on throughput; more expensive per operation 1.56x (spreads 9.96 % / 5.06 %)
- seedstone vs valkey-iot1: indistinguishable on throughput; more expensive per operation 3.99x (spreads 16.45 % / 8.82 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.60x (spreads 3.43 % / 12.97 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.59x (spreads 12.85 % / 8.13 %)
- seedstone vs garnet: indistinguishable on throughput; cheaper per operation 0.53x (spreads 12.87 % / 10.34 %)

Every throughput word on this row is *indistinguishable*, so no pair on it is a
trade: against the four Redis and Valkey arms this server is level on
throughput and more expensive per operation, by 1.56× to 3.99×.

In `v0.2.0` this row carried three *behind* readings against those arms, the
only row on the page with more than one.
[#32](https://github.com/stefanobaldo/seedstone/issues/32) measured what
keeping the executor threads awake between requests did to that reading and
what it cost per operation, and why that trade was declined.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot1`: in `v0.2.0` this pair read *behind* 0.96× on throughput.
- `redis-iot4`: in `v0.2.0` this pair read *ahead* 1.07× on throughput.
- `valkey-iot1`: in `v0.2.0` this pair read *behind* 0.94× on throughput.
- `valkey-iot4`: in `v0.2.0` this pair read *behind* 0.95× on throughput.
- `garnet`: in `v0.2.0` this pair read *ahead* 1.07× on throughput.

#### Depth 16

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 763 668 | 2.210 | 0.860 | 3.070 | 5.41 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 724 138 | 0.320 | 0.260 | 0.590 | 1.01 | 0.91 | 0.000 | 1.023 |
| redis-iot4 | 1 626 016 | 0.550 | 0.700 | 1.250 | 2.03 | 1.00 | 0.000 | 1.085 |
| valkey-iot1 | 1 572 327 | 0.410 | 0.230 | 0.640 | 1.00 | 0.83 | 0.000 | 1.122 |
| valkey-iot4 | 1 872 659 | 0.790 | 0.260 | 1.070 | 2.00 | 1.00 | 0.000 | 0.942 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 763 668 | 2.210 | 0.860 | 3.070 | 5.41 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 763 668 | 2.890 | 1.000 | 3.890 | 6.86 | 1.01 | 0.000 | 1.000 |
| garnet | 1 626 016 | 2.240 | 1.570 | 3.810 | 6.18 | 1.00 | - | 1.085 |

- seedstone vs redis-iot1: indistinguishable on throughput; more expensive per operation 5.20x (spreads 6.83 % / 1.69 %)
- seedstone vs redis-iot4: ahead 1.08x on throughput; more expensive per operation 2.46x (spreads 7.47 % / 1.60 %)
- seedstone vs valkey-iot1: ahead 1.12x on throughput; more expensive per operation 4.80x (spreads 6.83 % / 1.56 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 2.87x (spreads 6.83 % / 3.74 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.79x (spreads 6.83 % / 3.60 %)
- seedstone vs garnet: ahead 1.08x on throughput; cheaper per operation 0.81x (spreads 6.83 % / 5.25 %)

The two quantities point opposite ways against `redis-iot4` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `redis-iot1`: in `v0.2.0` this pair read *ahead* 1.06× on throughput.
- `valkey-iot4`: in `v0.2.0` this pair read *behind* 0.97× on throughput.

#### Depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 484 305 | 1.210 | 0.270 | 1.480 | 6.64 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 624 672 | 0.310 | 0.070 | 0.380 | 1.00 | 0.54 | 0.000 | 1.709 |
| redis-iot4 | 3 322 259 | 0.370 | 0.090 | 0.470 | 1.56 | 0.78 | 0.000 | 1.350 |
| valkey-iot1 | 2 262 444 | 0.370 | 0.070 | 0.440 | 1.00 | 0.48 | 0.000 | 1.982 |
| valkey-iot4 | 3 649 635 | 1.040 | 0.080 | 1.110 | 4.05 | 0.80 | 0.000 | 1.229 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 484 305 | 1.210 | 0.270 | 1.480 | 6.64 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 629 630 | 1.230 | 0.330 | 1.560 | 7.22 | 1.01 | 0.000 | 0.969 |
| garnet | 4 291 846 | 0.920 | 0.560 | 1.480 | 6.44 | 1.01 | - | 1.045 |

- seedstone vs redis-iot1: ahead 1.71x on throughput; more expensive per operation 3.89x (spreads 3.56 % / 5.26 %)
- seedstone vs redis-iot4: ahead 1.35x on throughput; more expensive per operation 3.15x (spreads 3.56 % / 2.13 %)
- seedstone vs valkey-iot1: ahead 1.98x on throughput; more expensive per operation 3.36x (spreads 3.56 % / 0.00 %)
- seedstone vs valkey-iot4: ahead 1.23x on throughput; more expensive per operation 1.33x (spreads 3.56 % / 0.90 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.95x (spreads 3.56 % / 0.64 %)
- seedstone vs garnet: indistinguishable on throughput; indistinguishable (spreads 5.15 % / 5.41 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `garnet`: in `v0.2.0` this pair read *ahead* 1.05× on throughput.

### SET 64 B, without and with `EX 60` — a write, and the same write carrying a deadline

`SET` of a 64-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, run twice per arm: once plain, once as
`SET … EX 60`. `W` = 5 discarded runs per arm per row, then three kept. On the
`EX` row every one of the seven arms answers the `TTL` probe with
`ttl probe: 60 (a deadline reached the server)` (`04-expiry.log`), so the
deadline reached the server and was not dropped on the way.

#### Without `EX`

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 484 305 | 1.390 | 0.250 | 1.650 | 7.39 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 109 704 | 0.400 | 0.080 | 0.480 | 1.01 | 0.44 | 0.000 | 2.126 |
| redis-iot4 | 2 506 266 | 0.480 | 0.100 | 0.580 | 1.45 | 0.61 | 0.000 | 1.789 |
| valkey-iot1 | 1 788 909 | 0.490 | 0.070 | 0.560 | 1.00 | 0.37 | 0.000 | 2.507 |
| valkey-iot4 | 2 631 579 | 1.430 | 0.110 | 1.540 | 4.04 | 0.62 | 0.000 | 1.704 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 484 305 | 1.390 | 0.250 | 1.650 | 7.39 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 545 454 | 1.310 | 0.240 | 1.560 | 7.09 | 1.01 | 0.000 | 0.987 |
| garnet | 4 310 345 | 0.940 | 0.570 | 1.510 | 6.42 | 1.01 | - | 1.040 |

- seedstone vs redis-iot1: ahead 2.13x on throughput; more expensive per operation 3.44x (spreads 1.78 % / 2.08 %)
- seedstone vs redis-iot4: ahead 1.79x on throughput; more expensive per operation 2.84x (spreads 1.78 % / 1.72 %)
- seedstone vs valkey-iot1: ahead 2.51x on throughput; more expensive per operation 2.95x (spreads 1.78 % / 3.57 %)
- seedstone vs valkey-iot4: ahead 1.70x on throughput; more expensive per operation 1.07x (spreads 1.78 % / 1.21 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.06x (spreads 5.41 % / 1.28 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.09x (spreads 6.83 % / 3.31 %)

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
| seedstone | 4 629 630 | 1.520 | 0.230 | 1.760 | 8.10 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 1 582 278 | 0.560 | 0.080 | 0.640 | 1.01 | 0.33 | 0.000 | 2.926 |
| redis-iot4 | 1 828 154 | 0.650 | 0.110 | 0.750 | 1.37 | 0.42 | 0.000 | 2.532 |
| valkey-iot1 | 1 176 470 | 0.760 | 0.090 | 0.850 | 1.00 | 0.25 | 0.000 | 3.935 |
| valkey-iot4 | 1 680 672 | 2.280 | 0.110 | 2.400 | 4.03 | 0.41 | 0.000 | 2.755 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 4 629 630 | 1.520 | 0.230 | 1.760 | 8.10 | 1.01 | 0.000 | 1.000 |
| dragonfly | 4 444 444 | 1.480 | 0.210 | 1.690 | 7.56 | 1.01 | 0.000 | 1.042 |
| garnet | 4 255 319 | 1.050 | 0.570 | 1.590 | 6.77 | 1.01 | - | 1.088 |

- seedstone vs redis-iot1: ahead 2.93x on throughput; more expensive per operation 2.75x (spreads 6.67 % / 4.69 %)
- seedstone vs redis-iot4: ahead 2.53x on throughput; more expensive per operation 2.35x (spreads 6.67 % / 2.67 %)
- seedstone vs valkey-iot1: ahead 3.94x on throughput; more expensive per operation 2.07x (spreads 6.67 % / 2.35 %)
- seedstone vs valkey-iot4: ahead 2.75x on throughput; cheaper per operation 0.73x (spreads 6.67 % / 6.25 %)
- seedstone vs dragonfly: indistinguishable on throughput; more expensive per operation 1.04x (spreads 6.67 % / 1.70 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.11x (spreads 9.57 % / 3.14 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Five of the six pair lines on this row print the same throughput spread,
6.67 %, and it is this server's own.** Its three kept runs here are 4 385 965,
4 629 630 and 4 694 836 operations per second: a spread of 6.67 %, wider than
every comparator's on this row but Garnet's, whose 9.57 % is the one its own
line prints. What decides the throughput words on this row is therefore,
against every arm but Garnet, this server's own run-to-run variation under
`SET … EX`, and neither a property of the machine nor a property of the
comparators. The same was true in `v0.2.0`, where that spread was 2.70 % and
the rule took it six times out of six.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `dragonfly`: in `v0.2.0` this pair read *indistinguishable* on CPU per operation.
- `garnet`: in `v0.2.0` this pair read *ahead* 1.06× on throughput and *indistinguishable* on CPU per operation.

### SET 10 240 B past a 384 MB ceiling — the write path under eviction

`SET` of a 10 240-byte value at pipeline depth 64, 50 connections, keys spread
uniformly over 100 000 keys, against a server started with a 384 MB memory
ceiling. Unlike every other cell this one starts empty: the keyspace is filled
past the ceiling first and that fill is discarded, then `W` = 5 discarded runs,
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
| seedstone | 616 523 | 5.180 | 3.060 | 8.270 | 5.10 | 1.01 | 0.613 | 1.000 |
| redis-iot1 | 264 690 | 1.670 | 2.100 | 3.770 | 1.00 | 0.55 | 0.677 | 2.329 |
| redis-iot4 | 632 911 | 2.870 | 3.000 | 5.870 | 3.74 | 1.01 | 0.678 | 0.974 |
| valkey-iot1 | 264 271 | 1.750 | 2.030 | 3.790 | 1.00 | 0.53 | 0.677 | 2.333 |
| valkey-iot4 | 628 536 | 4.440 | 1.790 | 6.160 | 3.92 | 1.01 | 0.678 | 0.981 |

- seedstone vs redis-iot1: ahead 2.33x on throughput; more expensive per operation 2.19x (spreads 5.29 % / 0.60 %)
- seedstone vs redis-iot4: indistinguishable on throughput; more expensive per operation 1.41x (spreads 6.48 % / 4.09 %)
- seedstone vs valkey-iot1: ahead 2.33x on throughput; more expensive per operation 2.18x (spreads 5.29 % / 1.06 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.34x (spreads 5.29 % / 6.01 %)

The two quantities point opposite ways against `redis-iot1` and
`valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** No pair on this row changed its word.

### MGET of 1, 4 and 16 keys — the multi-key read

`MGET` of 1, 4 and 16 spread keys at pipeline depth 64, 50 connections, keys
spread uniformly over 100 000 keys, against a keyspace populated and probed
before each arm's runs. `W` = 5 discarded runs per arm per row, then three kept.

**`ops/s` on these rows is requests per second, not keys per second.** The
16-key row moves sixteen times the keys of the 1-key row at the same figure, so
the three rows are not comparable with each other on throughput. Each row is
comparable across arms, which is what these tables are for.

#### 1 key

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 676 470 | 1.550 | 0.290 | 1.830 | 6.79 | 1.01 | 0.000 | 1.000 |
| redis-iot1 | 2 525 252 | 0.310 | 0.080 | 0.400 | 1.00 | 0.63 | 0.000 | 1.456 |
| redis-iot4 | 3 174 603 | 0.380 | 0.100 | 0.480 | 1.52 | 0.89 | 0.000 | 1.158 |
| valkey-iot1 | 2 207 506 | 0.360 | 0.090 | 0.450 | 1.00 | 0.56 | 0.000 | 1.665 |
| valkey-iot4 | 3 558 719 | 1.070 | 0.080 | 1.140 | 4.06 | 0.94 | 0.000 | 1.033 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 3 676 470 | 1.550 | 0.290 | 1.830 | 6.79 | 1.01 | 0.000 | 1.000 |
| dragonfly | 3 787 879 | 1.690 | 0.340 | 2.040 | 7.74 | 1.01 | 0.000 | 0.971 |
| garnet | 3 508 772 | 1.010 | 0.610 | 1.620 | 5.68 | 1.01 | - | 1.048 |

- seedstone vs redis-iot1: ahead 1.46x on throughput; more expensive per operation 4.58x (spreads 3.76 % / 2.50 %)
- seedstone vs redis-iot4: ahead 1.16x on throughput; more expensive per operation 3.81x (spreads 3.76 % / 2.19 %)
- seedstone vs valkey-iot1: ahead 1.67x on throughput; more expensive per operation 4.07x (spreads 3.76 % / 2.22 %)
- seedstone vs valkey-iot4: indistinguishable on throughput; more expensive per operation 1.61x (spreads 3.76 % / 2.19 %)
- seedstone vs dragonfly: indistinguishable on throughput; cheaper per operation 0.90x (spreads 3.76 % / 2.19 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.13x (spreads 7.03 % / 11.73 %)

The two quantities point opposite ways against `redis-iot1`, `redis-iot4`
and `valkey-iot1`: this server is ahead on throughput and more expensive per
operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.2.0` this pair read *ahead* 1.06× on throughput.
- `dragonfly`: in `v0.2.0` this pair read *behind* 0.96× on throughput.
- `garnet`: in `v0.2.0` this pair read *indistinguishable* on CPU per operation.

#### 4 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 912 046 | 3.330 | 0.280 | 3.620 | 6.92 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 1 127 396 | 0.780 | 0.110 | 0.890 | 1.00 | 0.58 | 0.000 | 1.696 |
| redis-iot4 | 1 273 885 | 0.890 | 0.150 | 1.050 | 1.34 | 0.70 | 0.000 | 1.501 |
| valkey-iot1 | 1 022 495 | 0.870 | 0.110 | 0.980 | 1.00 | 0.53 | 0.000 | 1.870 |
| valkey-iot4 | 1 506 024 | 2.570 | 0.110 | 2.680 | 4.04 | 0.83 | 0.000 | 1.270 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 1 912 046 | 3.330 | 0.280 | 3.620 | 6.92 | 1.00 | 0.000 | 1.000 |
| dragonfly | 1 218 027 | 6.270 | 0.890 | 7.160 | 8.71 | 0.68 | 0.000 | 1.570 |
| garnet | 1 845 018 | 1.880 | 0.680 | 2.560 | 4.67 | 1.00 | - | 1.036 |

- seedstone vs redis-iot1: ahead 1.70x on throughput; more expensive per operation 4.07x (spreads 0.38 % / 1.12 %)
- seedstone vs redis-iot4: ahead 1.50x on throughput; more expensive per operation 3.45x (spreads 0.38 % / 3.81 %)
- seedstone vs valkey-iot1: ahead 1.87x on throughput; more expensive per operation 3.69x (spreads 0.41 % / 0.55 %)
- seedstone vs valkey-iot4: ahead 1.27x on throughput; more expensive per operation 1.35x (spreads 0.38 % / 0.75 %)
- seedstone vs dragonfly: ahead 1.57x on throughput; cheaper per operation 0.51x (spreads 0.38 % / 0.55 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.41x (spreads 3.74 % / 30.47 %)

The two quantities point opposite ways against all four Redis and Valkey
arms: this server is ahead on throughput and more expensive per operation
against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `garnet`: in `v0.2.0` this pair read *ahead* 1.03× on throughput.

#### 16 keys

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 615 764 | 11.710 | 1.040 | 12.750 | 7.85 | 1.00 | 0.000 | 1.000 |
| redis-iot1 | 340 599 | 2.660 | 0.270 | 2.940 | 1.00 | 0.53 | 0.000 | 1.808 |
| redis-iot4 | 395 413 | 4.610 | 4.870 | 9.600 | 3.79 | 0.63 | 0.000 | 1.557 |
| valkey-iot1 | 312 695 | 2.930 | 0.270 | 3.200 | 1.00 | 0.49 | 0.000 | 1.969 |
| valkey-iot4 | 444 444 | 8.720 | 0.290 | 9.010 | 4.01 | 0.71 | 0.000 | 1.385 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 615 764 | 11.710 | 1.040 | 12.750 | 7.85 | 1.00 | 0.000 | 1.000 |
| dragonfly | 471 476 | 14.990 | 2.310 | 17.300 | 8.15 | 0.77 | 0.000 | 1.306 |
| garnet | 615 385 | 5.200 | 1.250 | 6.420 | 3.97 | 1.00 | - | 1.001 |

- seedstone vs redis-iot1: ahead 1.81x on throughput; more expensive per operation 4.34x (spreads 4.33 % / 0.78 %)
- seedstone vs redis-iot4: ahead 1.56x on throughput; more expensive per operation 1.33x (spreads 4.33 % / 2.92 %)
- seedstone vs valkey-iot1: ahead 1.97x on throughput; more expensive per operation 3.98x (spreads 4.33 % / 0.78 %)
- seedstone vs valkey-iot4: ahead 1.39x on throughput; more expensive per operation 1.42x (spreads 4.33 % / 4.11 %)
- seedstone vs dragonfly: ahead 1.31x on throughput; cheaper per operation 0.74x (spreads 4.33 % / 0.78 %)
- seedstone vs garnet: indistinguishable on throughput; more expensive per operation 1.99x (spreads 4.33 % / 0.78 %)

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
shapes are sized from does not pipeline a `KEYS` call. `W` = 5 discarded runs
per arm, then three kept.

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 2 973 | 3253.000 | 85.500 | 3340.000 | 9.93 | 0.06 | 0.000 | 1.000 |
| redis-iot1 | 688 | 1446.000 | 8.500 | 1454.500 | 1.00 | 0.01 | 0.000 | 4.324 |
| redis-iot4 | 697 | 1443.500 | 30.500 | 1474.000 | 1.03 | 0.02 | 0.000 | 4.267 |
| valkey-iot1 | 701 | 1418.000 | 8.500 | 1426.000 | 1.00 | 0.02 | 0.000 | 4.240 |
| valkey-iot4 | 711 | 2801.000 | 10.500 | 2811.500 | 2.00 | 0.02 | 0.000 | 4.179 |

Other engines:

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op | ×seedstone |
|---|---|---|---|---|---|---|---|---|
| seedstone | 2 973 | 3253.000 | 85.500 | 3340.000 | 9.93 | 0.06 | 0.000 | 1.000 |
| dragonfly | 2 113 | 2263.500 | 87.000 | 2350.500 | 4.97 | 0.04 | 0.000 | 1.407 |
| garnet | 1 514 | 6486.000 | 107.000 | 6592.500 | 9.98 | 0.03 | - | 1.964 |

- seedstone vs redis-iot1: ahead 4.32x on throughput; more expensive per operation 2.30x (spreads 0.23 % / 0.31 %)
- seedstone vs redis-iot4: ahead 4.27x on throughput; more expensive per operation 2.27x (spreads 0.22 % / 0.44 %)
- seedstone vs valkey-iot1: ahead 4.24x on throughput; more expensive per operation 2.34x (spreads 0.22 % / 0.19 %)
- seedstone vs valkey-iot4: ahead 4.18x on throughput; more expensive per operation 1.19x (spreads 0.22 % / 0.19 %)
- seedstone vs dragonfly: ahead 1.41x on throughput; more expensive per operation 1.42x (spreads 1.41 % / 0.19 %)
- seedstone vs garnet: ahead 1.96x on throughput; cheaper per operation 0.51x (spreads 0.81 % / 0.87 %)

The two quantities point opposite ways against all four Redis and Valkey arms
and against `dragonfly`: this server is ahead on throughput and more expensive
per operation against each of them, and both readings are of the same runs.

**Since `v0.2.0`.** The pairs on this row whose word changed, and what
they read then:

- `valkey-iot4`: in `v0.2.0` this pair read *indistinguishable* on CPU per operation.

**This server's own figure on this row is lower than in `v0.2.0`, and the
comparators' are not.** It answered 3 579 calls per second at 2 783.5 µs of
CPU a call then and 2 973 at 3 340.0 µs now; `redis-iot1` answered 688 in
both runs. Every word on the row but one held, because the ratios are far from
1, and that is why only one line above records a change.

**This server answers 4.3× the `KEYS` calls per second of Redis 8.10.0 at one
I/O thread and spends 2.30× the CPU on each of them, and that is the shape of
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
the head of `08-durability.log`. `W` = 5 discarded runs per arm per row, then
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
| seedstone | 4 587 156 | 1.220 | 0.250 | 1.480 | 6.81 | 1.01 | 0.000 |
| seedstone-always | 4 524 887 | 1.270 | 0.250 | 1.530 | 6.92 | 1.01 | 0.000 |

- seedstone-always vs seedstone: indistinguishable on throughput; more expensive per operation 1.03x (spreads 1.38 % / 1.35 %)

#### SET at depth 1

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op |
|---|---|---|---|---|---|---|---|
| seedstone | 130 770 | 5.080 | 5.710 | 10.790 | 1.42 | 1.00 | 0.000 |
| redis-iot1 | 136 258 | 1.580 | 4.760 | 6.250 | 0.85 | 1.00 | 0.000 |
| seedstone-never | 138 850 | 8.690 | 10.080 | 18.710 | 2.63 | 1.00 | 0.000 |
| redis-aof-no | 139 451 | 1.900 | 4.900 | 6.810 | 0.95 | 0.98 | 0.000 |
| seedstone-interval | 138 485 | 8.800 | 10.230 | 19.090 | 2.66 | 1.00 | 0.000 |
| redis-aof-everysec | 143 082 | 1.880 | 4.860 | 6.740 | 0.96 | 1.00 | 0.000 |
| seedstone-always | 17 491 | 10.570 | 11.460 | 21.880 | 0.39 | 0.13 | 0.000 |
| redis-aof-always | 18 049 | 1.560 | 4.910 | 6.550 | 0.12 | 0.13 | 0.000 |

- seedstone vs redis-iot1: indistinguishable on throughput; more expensive per operation 1.73x (spreads 17.32 % / 8.16 %)
- seedstone-never vs redis-aof-no: indistinguishable on throughput; more expensive per operation 2.75x (spreads 4.99 % / 1.76 %)
- seedstone-interval vs redis-aof-everysec: indistinguishable on throughput; more expensive per operation 2.83x (spreads 6.42 % / 2.37 %)
- seedstone-always vs redis-aof-always: behind 0.97x on throughput; more expensive per operation 3.34x (spreads 1.95 % / 4.89 %)

#### SET at depth 64

| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores | evicted/op |
|---|---|---|---|---|---|---|---|
| seedstone | 4 484 305 | 1.380 | 0.280 | 1.660 | 7.44 | 1.01 | 0.000 |
| redis-iot1 | 2 096 436 | 0.400 | 0.080 | 0.480 | 1.01 | 0.44 | 0.000 |
| seedstone-never | 2 577 320 | 1.840 | 0.210 | 2.070 | 5.34 | 0.55 | 0.000 |
| redis-aof-no | 1 161 440 | 0.690 | 0.130 | 0.820 | 0.95 | 0.24 | 0.000 |
| seedstone-interval | 2 645 503 | 1.880 | 0.180 | 2.080 | 5.40 | 0.55 | 0.000 |
| redis-aof-everysec | 1 177 856 | 0.680 | 0.170 | 0.850 | 0.97 | 0.25 | 0.000 |
| seedstone-always | 725 163 | 2.050 | 0.340 | 2.420 | 1.71 | 0.16 | 0.000 |
| redis-aof-always | 514 668 | 0.710 | 0.150 | 0.860 | 0.44 | 0.11 | 0.000 |

- seedstone vs redis-iot1: ahead 2.14x on throughput; more expensive per operation 3.46x (spreads 3.59 % / 4.17 %)
- seedstone-never vs redis-aof-no: ahead 2.22x on throughput; more expensive per operation 2.52x (spreads 44.71 % / 2.42 %)
- seedstone-interval vs redis-aof-everysec: ahead 2.25x on throughput; more expensive per operation 2.45x (spreads 24.67 % / 7.06 %)
- seedstone-always vs redis-aof-always: ahead 1.41x on throughput; more expensive per operation 2.81x (spreads 18.64 % / 2.89 %)

The `GET` row is the read path with the log synced on every write: against no
log it is indistinguishable on throughput and 1.03× the CPU per operation.

At depth 1 no pair is a trade. Three lines are *indistinguishable* on
throughput and `--fsync always` is *behind* 0.97× `appendfsync always`, and
this server is more expensive per operation on every line, by 1.73× to 3.34×.
Under `always` both engines answer about 13 % of the writes per second they
answer with no log: 17 491 and 18 049, against 130 770 and 136 258.

At depth 64 the two quantities point opposite ways on all four lines: this
server is ahead on throughput and more expensive per operation against each
Redis arm, and both readings are of the same runs.

**The spreads on the depth-64 row are wide on both engines' logged arms**:
44.71 %, 24.67 % and 18.64 % on this server's `never`, `interval` and
`always`; 18.89 % and 19.14 % on Redis's `no` and `everysec`; against 3.59 %
and 0.42 % for the two engines with no log, and 1.72 % for Redis under
`always`. Every throughput ratio on the row clears the spread its line prints,
and each arm's three kept runs are in the log.

**What this cell does not say.** One disk class and one provisioning: a cloud
block device at 3 240 IOPS, not a local NVMe drive, and a different disk is a
different table. Redis rewrote its log in the background during each of its
logged arms — 22, 22 and 26 times under `no`, `everysec` and `always`, counted
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
- **The two runs compared on this page ran on the same kernel, in different
  zones.** This run and the `v0.2.0` run whose figures are quoted under the
  changed pairs both stand on `7.0.0-1011-gcp`, on the same machine class; each
  kernel is echoed at the head of its run's logs. This run's machine was in
  `us-east5-b`, the `v0.2.0` run's in `us-central1-a`. The canary is the only
  instrument this page has for deciding whether two runs are comparable at all,
  and it passed here at `+3.70 %` against the same reference the `v0.2.0` run
  read `+4.26 %` against, both inside the ±5 % fixed before either run. What
  sanctions the comparison is that reading and nothing else.
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

`W` was 5 for this run, and the calibration derives it again rather than
assuming it. A calibration in which an arm never settles gives no `W`, and is
run again rather than read without that arm: this run's `02-calibrate.log` is
one, and `02b-calibrate.log` is the calibration it was read from. The canary
decides whether a re-run is comparable to the tables on this page. A re-run on
other hardware is a different table, not a correction of this one, and is read
on its own terms.

## Re-measurement

The same harness, durability stage included, runs at every minor release. The
new tables replace these, and this run's raw logs stay under
`bench/results/v0.3.0/`.
