#!/usr/bin/env python3
"""Turn the raw logs of a campaign into the tables the benchmark document
carries, and apply the reading rule mechanically.

The rule, fixed before any number existed, for each pair (seedstone,
comparator) on one row:

  r = seedstone's median / the comparator's median
  s = the larger of the two arms' within-arm spreads, (max - min) / median,
      over their kept runs
  indistinguishable   if |r - 1| <= max(s, 2 %)
  ahead / behind      otherwise (throughput), r to two decimals
  cheaper / more expensive per operation   (total CPU per operation)

No adjectives. Throughput and CPU are always printed side by side.

  report.py <log> [<log>...]      tables and pair readings, per log
  report.py --calibrate <log>     W, by the calibration rule
  report.py --selftest            the rule on fixed inputs

Standard library only.
"""
import re
import statistics
import sys
from collections import defaultdict

PRIMARY = ["seedstone", "redis-iot1", "redis-iot4", "valkey-iot1", "valkey-iot4"]
OTHER = ["dragonfly", "garnet"]
ORDER = PRIMARY + OTHER
TIE = 0.02

FIELD = re.compile(r"(\w+)=(\S+)")


def parse(path):
    """Every `cell ` line of a log, as a dict of its key=value fields."""
    rows = []
    with open(path) as f:
        for line in f:
            if not line.startswith("cell "):
                continue
            d = dict(FIELD.findall(line[5:]))
            for k in ("ops", "user_us", "sys_us", "total_us", "cores", "client_cores"):
                d[k] = float(d[k])
            rows.append(d)
    return rows


def rowkey(d):
    return (d["shape"], d["arg"], d["depth"], d["clients"], d["keyspace"], d["payload"])


def describe(key):
    shape, arg, depth, clients, keyspace, payload = key
    what = {
        "get": f"GET {payload} B",
        "set": f"SET {payload} B",
        "set-ex": f"SET {payload} B EX {arg}",
        "set-large": f"SET {payload} B",
        "mget": f"MGET {arg} keys",
    }[shape]
    return f"{what}, depth {depth}, {clients} clients, {keyspace} spread keys"


def median(xs):
    return statistics.median(xs)


def spread(xs):
    m = median(xs)
    return (max(xs) - min(xs)) / m if m else 0.0


def fmt(n, digits=0):
    s = f"{n:,.{digits}f}"
    return s.replace(",", " ")


def word(r, s, kind):
    if abs(r - 1) <= max(s, TIE):
        return "indistinguishable"
    if kind == "ops":
        return f"ahead {r:.2f}x" if r > 1 else f"behind {r:.2f}x"
    return f"more expensive per operation {r:.2f}x" if r > 1 else f"cheaper per operation {r:.2f}x"


def summarise(rows):
    """{rowkey: {arm: {col: median, ...; 'spread_ops', 'spread_cpu', 'evicted_per_op'}}}."""
    kept = defaultdict(lambda: defaultdict(list))
    for d in rows:
        if d["kind"] != "kept":
            continue
        kept[rowkey(d)][d["arm"]].append(d)
    out = {}
    for key, arms in kept.items():
        out[key] = {}
        for arm, runs in arms.items():
            cols = {c: median([r[c] for r in runs])
                    for c in ("ops", "user_us", "sys_us", "total_us", "cores", "client_cores")}
            cols["spread_ops"] = spread([r["ops"] for r in runs])
            cols["spread_cpu"] = spread([r["total_us"] for r in runs])
            ev = [r["evicted_per_op"] for r in runs if r["evicted_per_op"] != "-"]
            cols["evicted_per_op"] = median([float(e) for e in ev]) if ev else None
            cols["n"] = len(runs)
            out[key][arm] = cols
    return out


def table(arms, present, base):
    evict = any(present[a]["evicted_per_op"] is not None for a in arms if a in present)
    head = "| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores |"
    sep = "|---|---|---|---|---|---|---|"
    if evict:
        head += " evicted/op |"; sep += "---|"
    head += " ×seedstone |"; sep += "---|"
    lines = [head, sep]
    for a in arms:
        if a not in present:
            continue
        c = present[a]
        line = (f"| {a} | {fmt(c['ops'])} | {c['user_us']:.3f} | {c['sys_us']:.3f} | "
                f"{c['total_us']:.3f} | {c['cores']:.2f} | {c['client_cores']:.2f} |")
        if evict:
            line += f" {c['evicted_per_op']:.3f} |" if c["evicted_per_op"] is not None else " - |"
        ratio = base["ops"] / c["ops"] if base and c["ops"] else 0.0
        line += f" {ratio:.3f} |"
        lines.append(line)
    return "\n".join(lines)


def pairs(present, base):
    out = []
    for a in ORDER:
        if a == "seedstone" or a not in present:
            continue
        c = present[a]
        r_ops = base["ops"] / c["ops"]
        r_cpu = base["total_us"] / c["total_us"]
        s_ops = max(base["spread_ops"], c["spread_ops"])
        s_cpu = max(base["spread_cpu"], c["spread_cpu"])
        out.append(f"- seedstone vs {a}: {word(r_ops, s_ops, 'ops')} on throughput; "
                   f"{word(r_cpu, s_cpu, 'cpu')} (spreads {100*s_ops:.2f} % / {100*s_cpu:.2f} %)")
    return "\n".join(out)


def report(path):
    summary = summarise(parse(path))
    print(f"## {path}\n")
    for key in sorted(summary, key=lambda k: (k[0], int(k[2]), k[1])):
        present = summary[key]
        base = present.get("seedstone")
        print(f"### {describe(key)}\n")
        print(table(PRIMARY, present, base))
        if any(a in present for a in OTHER):
            print("\nOther engines:\n")
            print(table(["seedstone"] + OTHER, present, base))
        if base:
            print("\n" + pairs(present, base))
        print()


def calibrate(path):
    runs = defaultdict(list)
    for d in parse(path):
        if d["kind"] == "cal":
            runs[d["arm"]].append(d["ops"])
    W = 0
    for arm in ORDER:
        xs = runs.get(arm)
        if not xs:
            continue
        settle = None
        for i in range(len(xs) - 2):
            trio = xs[i:i + 3]
            if max(trio) - min(trio) <= TIE * median(trio):
                settle = i + 1  # 1-based
                break
        if settle is None:
            print(f"{arm}: never settled within {len(xs)} runs — a finding, not a number to round")
            continue
        need = settle - 1
        W = max(W, need)
        print(f"{arm}: settles at run {settle}, needs {need} discarded")
    print(f"W={W}")
    return W


def selftest():
    assert word(1.009, 0.005, "ops") == "indistinguishable"
    assert word(1.469, 0.01, "ops") == "ahead 1.47x"
    assert word(0.717, 0.01, "ops") == "behind 0.72x"
    assert word(1.03, 0.05, "ops") == "indistinguishable"      # inside the arm's own spread
    assert word(4.234, 0.01, "cpu") == "more expensive per operation 4.23x"
    assert word(0.6185, 0.01, "cpu") == "cheaper per operation 0.62x"
    assert abs(spread([100, 102, 98]) - 0.04) < 1e-9
    line = ("cell arm=redis-iot1 kind=kept shape=get arg=- depth=64 clients=50 keyspace=100000 "
            "payload=64 n=1000000 ops=2583979.25 user_us=0.310 sys_us=0.080 total_us=0.380 "
            "cores=0.98 client_cores=0.40 evicted=- evicted_per_op=-")
    d = dict(FIELD.findall(line[5:]))
    assert d["arm"] == "redis-iot1" and d["ops"] == "2583979.25" and d["evicted_per_op"] == "-"
    print("selftest ok")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    if args[0] == "--selftest":
        selftest()
    elif args[0] == "--calibrate":
        calibrate(args[1])
    else:
        for p in args:
            report(p)
