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
  report.py --svg <out> [--theme light|dark] [--release vX.Y.Z]
            [--engines name:ver,...] [--machine type] <log>...
                                  the README's chart, from the logs

Standard library only.
"""
import re
import statistics
import sys
import xml.dom.minidom
from collections import defaultdict

PRIMARY = ["seedstone", "redis-iot1", "redis-iot4", "valkey-iot1", "valkey-iot4"]
OTHER = ["dragonfly", "garnet"]
ORDER = PRIMARY + OTHER
# The durability stage's pairs, each seedstone setting against Redis's AOF at
# the matching one; the first pair is both engines with no log at all.
DURABILITY = [("seedstone", "redis-iot1"),
              ("seedstone-never", "redis-aof-no"),
              ("seedstone-interval", "redis-aof-everysec"),
              ("seedstone-always", "redis-aof-always")]
DURABILITY_ARMS = [arm for pair in DURABILITY for arm in pair]
# A row Redis does not take part in is a read under seedstone's log synced on
# every write, against no log: what holding replies costs the replies it holds
# none of.
READ_PAIR = ("seedstone-always", "seedstone")
TIE = 0.02

# The README's chart: four cells, one panel each, throughput only. A reader
# who stops here sees throughput and not the CPU per operation; the caption
# names the page that carries the rest. Each panel's arms, in order, and the
# arm whose presence on a row identifies the cell (Garnet is absent from
# eviction; the durability panel's arms exist in no other stage).
CHART = [
    ("GET 64 B · depth 1", ("get", "-", "1"), PRIMARY + OTHER, "seedstone"),
    ("GET 64 B · depth 64", ("get", "-", "64"), PRIMARY + OTHER, "seedstone"),
    ("SET 10 KB · depth 64 · under a 384 MB ceiling, LRU", ("set-large", "10240", "64"),
     PRIMARY + OTHER, "seedstone"),
    ("SET 64 B · depth 1 · synced on every write", ("set", "-", "1"),
     ["seedstone-always", "redis-aof-always"], "seedstone-always"),
]
LABEL = {"seedstone": "seedstone", "redis-iot1": "redis, io-threads 1",
         "redis-iot4": "redis, io-threads 4", "valkey-iot1": "valkey, io-threads 1",
         "valkey-iot4": "valkey, io-threads 4", "dragonfly": "dragonfly", "garnet": "garnet",
         "seedstone-always": "seedstone --fsync always",
         "redis-aof-always": "redis appendfsync always"}
# GitHub's own light and dark palettes, so the chart sits on the README as
# if it were part of the page.
THEMES = {
    "light": dict(bg="#ffffff", text="#1f2328", muted="#59636e", bar="#afb8c1",
                  ours="#1a7f5a", grid="#d1d9e0"),
    "dark": dict(bg="#0d1117", text="#e6edf3", muted="#9198a1", bar="#3d444d",
                 ours="#3fb950", grid="#30363d"),
}

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
    size = fmt(int(payload))
    if shape == "keys":
        # The loader's keyspace: `payload` is the values' size and `keyspace`
        # the key count; `arg` is which prefix every call matched.
        return (f"KEYS over {fmt(int(keyspace))} keys of {size} B, one prefix of 64 "
                f"matched per call, depth {depth}, {clients} clients")
    what = {
        "get": f"GET {size} B",
        "set": f"SET {size} B",
        "set-ex": f"SET {size} B EX {arg}",
        "set-large": f"SET {size} B",
        "mget": f"MGET {arg} keys",
    }[shape]
    return f"{what}, depth {depth}, {clients} clients, {fmt(int(keyspace))} spread keys"


def sortkey(key):
    """Shape, then depth, then the shape argument — all three as numbers where
    they are numbers. Sorted as text, MGET's 1, 4 and 16 keys come out 1, 16, 4."""
    shape, arg, depth = key[0], key[1], key[2]
    return (shape, int(depth), int(arg) if arg.isdigit() else 0, arg)


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


def table(arms, present, base, ratio="seedstone"):
    evict = any(present[a]["evicted_per_op"] is not None for a in arms if a in present)
    head = "| arm | ops/s | user µs/op | sys µs/op | total µs/op | server cores | client cores |"
    sep = "|---|---|---|---|---|---|---|"
    if evict:
        head += " evicted/op |"; sep += "---|"
    if ratio:
        head += f" ×{ratio} |"; sep += "---|"
    lines = [head, sep]
    for a in arms:
        if a not in present:
            continue
        c = present[a]
        line = (f"| {a} | {fmt(c['ops'])} | {c['user_us']:.3f} | {c['sys_us']:.3f} | "
                f"{c['total_us']:.3f} | {c['cores']:.2f} | {c['client_cores']:.2f} |")
        if evict:
            line += f" {c['evicted_per_op']:.3f} |" if c["evicted_per_op"] is not None else " - |"
        # No seedstone row, or a median of zero, is a missing ratio and says so.
        # Printed as 0.000 it would read as a measured hundredfold gap.
        if ratio:
            line += f" {base['ops'] / c['ops']:.3f} |" if base and c["ops"] else " - |"
        lines.append(line)
    return "\n".join(lines)


def reading(ours, base, a, c):
    """One pair's line: `ours` (median columns `base`) against `a` (`c`)."""
    s_ops = max(base["spread_ops"], c["spread_ops"])
    s_cpu = max(base["spread_cpu"], c["spread_cpu"])
    # A median of zero has no ratio. CPU per operation reads zero whenever a
    # run is short enough that the clock tick swallows it, and dividing by it
    # ends the report; the pair is unreadable, which is what is printed.
    if not (base["ops"] and c["ops"] and base["total_us"] and c["total_us"]):
        return (f"- {ours} vs {a}: no reading — a median is zero "
                f"({ours} {base['ops']:.2f} ops/s at {base['total_us']:.3f} µs/op; "
                f"{a} {c['ops']:.2f} ops/s at {c['total_us']:.3f} µs/op)")
    r_ops = base["ops"] / c["ops"]
    r_cpu = base["total_us"] / c["total_us"]
    return (f"- {ours} vs {a}: {word(r_ops, s_ops, 'ops')} on throughput; "
            f"{word(r_cpu, s_cpu, 'cpu')} (spreads {100*s_ops:.2f} % / {100*s_cpu:.2f} %)")


def pairs(present, base):
    return "\n".join(reading("seedstone", base, a, present[a])
                     for a in ORDER if a != "seedstone" and a in present)


def durability_pairs(present):
    """Each setting's pair present on the row; a pair missing an arm says so."""
    if not any(a.startswith("redis") for a in present):
        ours, theirs = READ_PAIR
        if ours in present and theirs in present:
            return reading(ours, present[ours], theirs, present[theirs])
        return f"- {ours} vs {theirs}: no reading — the row lacks one of them"
    out = []
    for ours, theirs in DURABILITY:
        if ours in present and theirs in present:
            out.append(reading(ours, present[ours], theirs, present[theirs]))
        elif ours in present or theirs in present:
            out.append(f"- {ours} vs {theirs}: no reading — "
                       f"{theirs if ours in present else ours} has no row here")
    return "\n".join(out)


def report(path):
    summary = summarise(parse(path))
    print(f"## {path}\n")
    for key in sorted(summary, key=sortkey):
        present = summary[key]
        base = present.get("seedstone")
        print(f"### {describe(key)}\n")
        # The durability stage's rows hold arms no other stage has; they are
        # read by setting, each against its own pair, not against seedstone.
        if any(a in present for a in DURABILITY_ARMS if a not in ORDER):
            print(table(DURABILITY_ARMS, present, None, ratio=None))
            print("\n" + durability_pairs(present) + "\n")
            continue
        if key[0] == "keys":
            print("`ops/s` on these rows is `KEYS` calls per second; every call answers "
                  "the same prefix's keys, so the rows are comparable across arms.\n")
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
    unsettled = []
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
            unsettled.append(arm)
            continue
        need = settle - 1
        W = max(W, need)
        print(f"{arm}: settles at run {settle}, needs {need} discarded")
    # W is the largest need across arms; an arm with no need contributes zero.
    # An arm that never settled contributes nothing at all, so printing W here
    # would answer the question with the unsettled arm left out of it — the
    # finding reported as a number, which is what the rule forbids.
    if unsettled:
        print(f"W is not derivable: {', '.join(unsettled)} never settled. "
              "Record the finding; do not run the cells on a W taken from the rest.")
        return None
    print(f"W={W}")
    return W


def header(path):
    """The run's date (the stage's start, from the log's first line) and the
    kernel line, so the caption states what the log states and nothing the
    clock says."""
    date = machine = ""
    with open(path) as f:
        for line in f:
            if line.startswith("### stage ") and " start " in line:
                date = line.split(" start ", 1)[1].strip()[:10]
            elif line.startswith("### kernel "):
                machine = line[4:].strip()
            if date and machine:
                break
    return date, machine


def find_cell(summaries, prefix, anchor):
    """The first log whose row matches the cell's shape, argument and depth and
    carries the anchor arm; None when no log ran the cell."""
    for s in summaries:
        for key, present in s.items():
            if key[:3] == prefix and anchor in present:
                return present
    return None


def short(n):
    if n >= 1e6:
        return f"{n / 1e6:.2f} M"
    if n >= 1e3:
        return f"{n / 1e3:.0f} k"
    return f"{n:.0f}"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def svg_from_summaries(summaries, date, machine_line, release, engines, theme,
                       machine="c4a-standard-16"):
    t = THEMES[theme]
    W, PW, PH, ROW, LEFT, GAP = 960, 460, 200, 22, 200, 20
    arch = machine_line.split()[2] if len(machine_line.split()) > 2 else ""
    body = []
    for i, (title, prefix, arms, anchor) in enumerate(CHART):
        x0 = GAP + (i % 2) * (PW + GAP)
        y0 = 44 + (i // 2) * (PH + GAP)
        body.append(f'<text x="{x0}" y="{y0}" class="t">{esc(title)}</text>')
        present = find_cell(summaries, prefix, anchor)
        if present is None:
            body.append(f'<text x="{x0}" y="{y0 + 28}" class="m">not measured in this run</text>')
            continue
        rows = [a for a in arms if a in present]
        top = max(present[a]["ops"] for a in rows)
        scale = (PW - LEFT - 64) / top
        for j, a in enumerate(rows):
            y = y0 + 12 + j * ROW
            w = present[a]["ops"] * scale
            fill = t["ours"] if a.startswith("seedstone") else t["bar"]
            body.append(f'<text x="{x0 + LEFT - 8}" y="{y + 13}" class="l" text-anchor="end">'
                        f'{esc(LABEL.get(a, a))}</text>')
            body.append(f'<rect x="{x0 + LEFT}" y="{y}" width="{w:.1f}" height="16" rx="2" fill="{fill}"/>')
            body.append(f'<text x="{x0 + LEFT + w + 6:.1f}" y="{y + 13}" class="v">'
                        f'{short(present[a]["ops"])}</text>')
    H = 44 + 2 * (PH + GAP) + 50
    names = ", ".join(f"{n.capitalize()} {v}" for n, v in
                      (e.split(":", 1) for e in engines.split(",")))
    cap1 = (f"Throughput, operations per second, the median of three kept runs. "
            f"seedstone {release} against {names}.")
    cap2 = (f"GCP {machine} ({arch}), server on 10 cores, redis-benchmark on 6, over "
            f"loopback, {date}.")
    cap3 = "Every table, the CPU per operation and what the numbers do not say: docs/benchmarks.md"
    style = (f".t{{font:600 13px ui-sans-serif,system-ui,sans-serif;fill:{t['text']}}}"
             f".l,.v,.m{{font:12px ui-sans-serif,system-ui,sans-serif;fill:{t['text']}}}"
             f".m,.c{{fill:{t['muted']}}}.c{{font:11px ui-sans-serif,system-ui,sans-serif}}")
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" role="img" aria-label="Benchmark throughput, seedstone {release}">',
        f"<style>{style}</style>",
        f'<rect width="{W}" height="{H}" fill="{t["bg"]}"/>',
        *body,
        f'<text x="{GAP}" y="{H - 36}" class="c">{esc(cap1)}</text>',
        f'<text x="{GAP}" y="{H - 22}" class="c">{esc(cap2)}</text>',
        f'<text x="{GAP}" y="{H - 8}" class="c">{esc(cap3)}</text>',
        "</svg>", ""])


def svg_args(argv):
    """The options and logs of `--svg`, from `--key value` or `key=value`: the
    file's first line writes the second form, since an XML comment may not
    hold "--", and the check passes that line back as it reads it."""
    opts = {"theme": "light", "release": "", "engines": "", "machine": "c4a-standard-16"}
    logs = []
    it = iter(argv)
    for a in it:
        key, eq, value = a.partition("=")
        if a.startswith("--") and a[2:] in opts:
            opts[a[2:]] = next(it)
        elif eq and key in opts:
            opts[key] = value
        else:
            logs.append(a)
    for v in [*opts.values(), *logs]:
        if " " in v or "--" in v:
            sys.exit(f"an --svg input may not contain a space or a double hyphen: {v!r}")
    return opts, logs


def inputs_comment(opts, logs):
    return "<!-- inputs: " + " ".join([f"{k}={v}" for k, v in opts.items()] + logs) + " -->"


def svg_chart(out, argv):
    """`--svg <out> [--theme T] [--release R] [--engines E] [--machine M] <log>...`.
    The first line of the file names every input, so the file says what it
    was rendered from and a check can regenerate it."""
    opts, logs = svg_args(argv)
    if not logs:
        sys.exit("--svg needs at least one log")
    date, machine = header(logs[0])
    summaries = [summarise(parse(p)) for p in logs]
    svg = svg_from_summaries(summaries, date, machine, opts["release"], opts["engines"],
                             opts["theme"], opts["machine"])
    with open(out, "w") as f:
        f.write(f"{inputs_comment(opts, logs)}\n{svg}")

def selftest():
    assert word(1.009, 0.005, "ops") == "indistinguishable"
    assert word(1.469, 0.01, "ops") == "ahead 1.47x"
    assert word(0.717, 0.01, "ops") == "behind 0.72x"
    assert word(1.03, 0.05, "ops") == "indistinguishable"      # inside the arm's own spread
    assert word(4.234, 0.01, "cpu") == "more expensive per operation 4.23x"
    assert word(0.6185, 0.01, "cpu") == "cheaper per operation 0.62x"
    assert abs(spread([100, 102, 98]) - 0.04) < 1e-9
    assert fmt(10240) == "10\u202f240" and fmt(64) == "64"
    mget = [("mget", a, "64", "50", "100000", "64") for a in ("16", "1", "4")]
    assert [k[1] for k in sorted(mget, key=sortkey)] == ["1", "4", "16"]
    assert describe(("set-large", "10240", "64", "50", "100000", "10240")) == (
        "SET 10\u202f240 B, depth 64, 50 clients, 100\u202f000 spread keys")
    line = ("cell arm=redis-iot1 kind=kept shape=get arg=- depth=64 clients=50 keyspace=100000 "
            "payload=64 n=1000000 ops=2583979.25 user_us=0.310 sys_us=0.080 total_us=0.380 "
            "cores=0.98 client_cores=0.40 evicted=- evicted_per_op=-")
    d = dict(FIELD.findall(line[5:]))
    assert d["arm"] == "redis-iot1" and d["ops"] == "2583979.25" and d["evicted_per_op"] == "-"
    assert describe(("keys", "7", "1", "50", "7000", "10240")) == (
        "KEYS over 7\u202f000 keys of 10\u202f240 B, one prefix of 64 matched per call, "
        "depth 1, 50 clients")
    assert sortkey(("keys", "7", "1", "50", "7000", "10240")) == ("keys", 1, 7, "7")
    row = {"ops": 100.0, "user_us": 0.5, "sys_us": 0.5, "total_us": 1.0, "cores": 1.0,
           "client_cores": 0.5, "spread_ops": 0.01, "spread_cpu": 0.01, "evicted_per_op": None}
    slow = dict(row, ops=50.0, total_us=2.0)
    present = {"seedstone-always": slow, "redis-aof-always": row, "seedstone-never": row}
    assert durability_pairs(present) == (
        "- seedstone-never vs redis-aof-no: no reading — redis-aof-no has no row here\n"
        "- seedstone-always vs redis-aof-always: behind 0.50x on throughput; "
        "more expensive per operation 2.00x (spreads 1.00 % / 1.00 %)")
    assert "×" not in table(DURABILITY_ARMS, present, None, ratio=None)
    reads = {"seedstone-always": slow, "seedstone": row}
    assert durability_pairs(reads) == (
        "- seedstone-always vs seedstone: behind 0.50x on throughput; "
        "more expensive per operation 2.00x (spreads 1.00 % / 1.00 %)")
    # --svg: the chart is a function of the summaries and the header, nothing else.
    assert short(4587156) == "4.59 M" and short(143906) == "144 k" and short(950) == "950"
    base_row = {"ops": 100.0, "user_us": 0.5, "sys_us": 0.5, "total_us": 1.0, "cores": 1.0,
                "client_cores": 0.5, "spread_ops": 0.01, "spread_cpu": 0.01, "evicted_per_op": None}
    field = {("get", "-", "1", "50", "100000", "64"): {"seedstone": dict(base_row, ops=140000.0),
                                                        "redis-iot1": dict(base_row, ops=130000.0)},
             ("get", "-", "64", "50", "100000", "64"): {"seedstone": dict(base_row, ops=4500000.0),
                                                         "redis-iot1": dict(base_row, ops=2600000.0)}}
    assert find_cell([field], ("get", "-", "64"), "seedstone")["seedstone"]["ops"] == 4500000.0
    assert find_cell([field], ("set-large", "10240", "64"), "seedstone") is None
    svg = svg_from_summaries([field], "2026-09-23", "kernel 7.0.0-1011-gcp aarch64",
                             "v0.2.0", "redis:8.10.0,valkey:9.1.1", "light")
    assert svg.startswith("<svg "), svg[:40]
    assert "4.50 M" in svg and "144 k" not in svg and "140 k" in svg
    assert svg.count("not measured in this run") == 2          # eviction and durability panels
    assert "Redis 8.10.0, Valkey 9.1.1" in svg and "2026-09-23" in svg and "v0.2.0" in svg
    assert svg == svg_from_summaries([field], "2026-09-23", "kernel 7.0.0-1011-gcp aarch64",
                                     "v0.2.0", "redis:8.10.0,valkey:9.1.1", "light")
    dark = svg_from_summaries([field], "2026-09-23", "kernel 7.0.0-1011-gcp aarch64",
                              "v0.2.0", "redis:8.10.0,valkey:9.1.1", "dark")
    assert dark != svg and THEMES["dark"]["ours"] in dark
    # The first line names the inputs in a comment, which XML forbids to hold
    # "--": the options are written key=value, and read back in either form.
    opts, logs = svg_args(["--theme", "dark", "--release", "v0.2.0", "a.log", "b.log"])
    line = inputs_comment(opts, logs)
    assert "--" not in line[4:-3], line
    assert svg_args(line[len("<!-- inputs: "):-len(" -->")].split()) == (opts, logs)
    xml.dom.minidom.parseString(line + "\n" + dark)
    print("selftest ok")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    if args[0] == "--selftest":
        selftest()
    elif args[0] == "--calibrate":
        sys.exit(0 if calibrate(args[1]) is not None else 1)
    elif args[0] == "--svg":
        svg_chart(args[1], args[2:])
    else:
        for p in args:
            report(p)
