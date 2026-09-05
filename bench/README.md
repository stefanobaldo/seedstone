# bench

The benchmark harness. `docs/benchmarks.md` is the method and the results;
this directory is what produced them.

- `cell.sh` — one measurement run: a shape, a pipeline depth, a connection
  count, and one line out with throughput, the server's CPU per operation,
  its cores, the client's cores, and every declaration the run was made under.
- `campaign.sh` — the stages, in the only order that makes them readable:
  `canary` (a gate), `calibrate` (produces `W`), then `field`, `expiry`,
  `eviction`, `multikey`.
- `report.py` — turns the raw logs into the tables, applies the reading rule,
  and derives `W` from the calibration log. Standard library only.
- `results/<tag>/` — the raw logs of each published run, one per stage,
  unedited.

Requirements: Linux, `redis-benchmark` and `redis-cli`, `taskset`, Python 3.
Every path and cpuset in `campaign.sh` is an environment variable with the
reference machine's value as its default.
