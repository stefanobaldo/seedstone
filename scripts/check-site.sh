#!/usr/bin/env bash
# The site under `site/` holds together: every link on the page resolves —
# to a file this repository tracks, or to a heading on one of the
# documentation pages — the page carries the metadata a crawler and a link
# preview read, it names no `http://` URL, the mark it shows is the mark
# `brand.py` holds, every benchmark figure it shows is the benchmark page's
# for that cell and arm, and the page with everything it references stays
# under its byte budget. Each check prints one `ok:` or `FAIL:` line.
#
# Usage: check-site.sh            (from the repository root)
#
# The budget counts the first load: the page and what the browser fetches
# to show it. A link the reader may follow and the image a link preview
# fetches are not in it.
set -euo pipefail

python3 -I - <<'PY'
import os, re, subprocess, sys

sys.path.insert(0, "scripts")
import brand

SITE = "site"
# A fixture may stand in for the page (the checker's own tests do that).
INDEX = os.environ.get("CHECK_SITE_INDEX", os.path.join(SITE, "index.html"))
REPO = "https://github.com/stefanobaldo/seedstone/blob/main/"
ORIGIN = "https://seedstone.io/"
BUDGET = 200 * 1024
# Paths the page links before the file lands beside it; each entry leaves
# with the change that adds its file.
PENDING = {"install.sh"}

status = 0
def ok(check, detail=""):
    print(f"ok: {check}" + (f": {detail}" if detail else ""))
def fail(check, why):
    global status
    status = 1
    print(f"FAIL: {check}: {why}")

if not os.path.exists(INDEX):
    fail("page", f"{INDEX} missing")
    sys.exit(status)

html = open(INDEX, encoding="utf-8").read()
tracked = set(subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout.split())

FENCE = re.compile(r"^(```|~~~).*?^\1", re.M | re.S)
anchors = {}
def anchors_of(path):
    """The fragments GitHub renders for a Markdown file: one per heading,
    slugged its way (lower case, punctuation dropped, spaces to hyphens,
    repeats suffixed -1, -2 ...), plus explicit HTML ids and names."""
    if path not in anchors:
        text = FENCE.sub("", open(path, encoding="utf-8").read())
        seen, found = {}, set()
        for h in re.findall(r"^#{1,6}\s+(.*?)\s*#*\s*$", text, re.M):
            slug = re.sub(r"[^\w\- ]", "", h.lower()).replace(" ", "-")
            n = seen.get(slug, 0)
            seen[slug] = n + 1
            found.add(slug if n == 0 else f"{slug}-{n}")
        found.update(re.findall(r"""(?:id|name)=["']([^"']+)["']""", text))
        anchors[path] = found
    return anchors[path]

ATTR = re.compile(r"""\b(?:href|src|srcset)=["']([^"']+)["']""")
OG_IMAGE = re.compile(r"""<meta property="og:image" content="([^"]+)">""")
URLS = sorted({u.strip() for m in ATTR.finditer(html) for u in m.group(1).split(",")}
              | set(OG_IMAGE.findall(html)))
# What a browser fetches to show the page: `src`, `srcset` and `<link href>`
# (stylesheet, icons, preloads). An `<a href>` is a navigation and the
# `og:image` a crawler's fetch; neither is in the first load the budget bounds.
LOADED = re.compile(r"""<(?:link|img|source|script)\b[^>]*?\b(?:href|src|srcset)=["']([^"']+)["']""")
LOADED_URLS = {u.strip() for m in LOADED.finditer(html) for u in m.group(1).split(",")}

def site_file(url):
    """A URL on this site -> its path under site/, or None if it is not one."""
    if url.startswith(ORIGIN):
        url = "/" + url[len(ORIGIN):]
    if url.startswith("/") or (not re.match(r"^[a-z][a-z0-9+.-]*:", url) and not url.startswith("#")):
        path = url.partition("#")[0].partition("?")[0].lstrip("/") or "index.html"
        if path.endswith("/"):
            path += "index.html"
        return os.path.join(SITE, path)
    return None

def check_links():
    referenced, bad = [], []
    for url in URLS:
        path = site_file(url)
        if path is not None:
            if path in tracked:
                if url in LOADED_URLS:
                    referenced.append(path)
            elif path[len(SITE) + 1:] in PENDING:
                continue  # not yet; checked below that it stays not-yet
            else:
                bad.append(f"{url} -> {path} is not tracked")
        elif url.startswith(REPO):
            target, _, frag = url[len(REPO):].partition("#")
            if target not in tracked:
                bad.append(f"{url}: {target} is not tracked")
            elif frag and target.endswith(".md") and frag not in anchors_of(target):
                bad.append(f"{url}: no heading for #{frag} in {target}")
        elif url.startswith("#") and f'id="{url[1:]}"' not in html:
            bad.append(f"{url}: no element with that id")
    landed = sorted(p for p in PENDING if os.path.join(SITE, p) in tracked)
    if landed:
        bad.append("tracked now, so no longer pending: " + ", ".join(landed))
    if bad:
        fail("links", "; ".join(bad))
    else:
        ok("links", f"{len(URLS)} resolve, {len(PENDING)} pending")
    return referenced

def check_metadata():
    want = {
        "canonical": r'<link rel="canonical" href="https://seedstone\.io/">',
        "title": r"<title>[^<]+</title>",
        "description": r'<meta name="description" content="[^"]{20,155}">',
        "og:title": r'<meta property="og:title" content="[^"]+">',
        "og:description": r'<meta property="og:description" content="[^"]+">',
        "og:image": r'<meta property="og:image" content="https://seedstone\.io/assets/social\.png">',
    }
    missing = [name for name, rx in want.items() if not re.search(rx, html)]
    if missing:
        fail("metadata", "missing " + ", ".join(missing))
    else:
        ok("metadata")

def check_landmarks():
    """The headline and the install line are inside <main>: a reader who
    jumps to the main landmark lands on them, not past them."""
    main = html.find("<main>")
    h1 = html.find("<h1")
    if 0 <= main < h1 and html.find("</main>") > h1:
        ok("landmarks", "the h1 is inside main")
    else:
        fail("landmarks", "the h1 is not inside <main>")

def check_no_http():
    hits = sorted(set(re.findall(r"http://[^\s\"'<)]+", html)))
    if hits:
        fail("https only", ", ".join(hits))
    else:
        ok("https only")

def check_mark():
    if all(shape in html for shape in brand.SHAPES):
        ok("mark", "the four shapes, verbatim")
    else:
        fail("mark", "the page does not carry the mark's four shapes verbatim")

BENCH = os.path.join("docs", "benchmarks.md")
# Each cell the page may name -> the heading(s) its tables sit under on the
# benchmark page: a `###` heading, then optionally a `####` one, each
# matched by prefix with the backticks dropped. Under the innermost heading
# every Markdown table until the next heading of the same or a higher level
# counts (the main table and the *Other engines* table both), and so do the
# `- seedstone vs <arm>:` pair lines.
CELLS = {
    "keys-7000": ("KEYS over 7 000 keys",),
    "set-64-ex60-d64": ("SET 64 B, without and with", "With EX 60"),
    "set-64-d64": ("SET 64 B, without and with", "Without EX"),
    "mget-16-d64": ("MGET of 1, 4 and 16 keys", "16 keys"),
    "get-64-d64": ("GET 64 B", "Depth 64"),
}

def abbreviate(n):
    """The README chart's abbreviation: three significant digits with `M`
    and `k`; below ten thousand the table's own string."""
    if n >= 1_000_000:
        return f"{n / 1e6:.2f} M"
    if n >= 10_000:
        return f"{round(n / 1e3):.0f} k"
    return f"{n:,}".replace(",", " ")

def bench_section(headings):
    """The text of docs/benchmarks.md under the named heading(s)."""
    text = open(BENCH, encoding="utf-8").read()
    for level, want in zip((3, 4), headings):
        lines, out, inside = text.split("\n"), [], False
        for line in lines:
            m = re.match(r"^(#{1,6})\s+(.*)$", line)
            if m and len(m.group(1)) <= level:
                if inside:
                    break
                inside = m.group(2).replace("`", "").startswith(want) and len(m.group(1)) == level
                continue
            if inside:
                out.append(line)
        if not inside and not out:
            return None
        text = "\n".join(out)
    return text

def bench_cell(cell):
    """(ops/s by arm, factor text by arm) for a cell, read from the page."""
    text = bench_section(CELLS[cell])
    if text is None:
        return None, None
    ops, factors = {}, {}
    for line in text.split("\n"):
        cols = [c.strip() for c in line.strip().strip("|").split("|")] if line.startswith("|") else None
        if cols and len(cols) >= 2 and re.fullmatch(r"[\d\s]+", cols[1]):
            ops[cols[0]] = int(re.sub(r"\D", "", cols[1]))
        m = re.match(r"- seedstone vs ([\w-]+): (?:ahead|behind) ([\d.]+)x on throughput", line)
        if m:
            factors[m.group(1)] = m.group(2) + "\u00d7"
    return ops, factors

ELEMENT = re.compile(r"<(\w+)\b([^>]*\bdata-cell=\"[^\"]*\"[^>]*)>(.*?)</\1>", re.S)
def attrs_of(s):
    return dict(re.findall(r"""\b([\w-]+)=["']([^"']*)["']""", s))
def text_of(s):
    """Tags dropped, whitespace folded: the table's thousands separator is a
    narrow no-break space (U+202F), and the page's may be any space that
    keeps a figure from wrapping; both read as one plain space here."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s)).strip()

def check_figures():
    """Every benchmark figure the page shows is bound to its cell and its
    arm (`data-cell`, `data-arm`), every factor to its cell and the arm it
    is against (`data-pair`), and each matches the benchmark page's table
    for exactly that cell and arm, abbreviated as the README's chart does.
    A bar's width (`--w`) is its figure's share of the cell's largest shown
    figure, so a bar cannot say one thing and its text another."""
    bad, figures, factors = [], 0, 0
    if re.search(r"benchmarks(-dark)?\.svg", html):
        bad.append("the page references the README's chart")
    per_cell = {}
    for el in ELEMENT.finditer(html):
        attrs, inner = attrs_of(el.group(2)), el.group(3)
        cell = attrs["data-cell"]
        if cell not in CELLS:
            bad.append(f"data-cell={cell!r} is not a cell this check knows ({', '.join(CELLS)})")
            continue
        if cell not in per_cell:
            per_cell[cell] = bench_cell(cell)
        ops, pairs = per_cell[cell]
        if ops is None:
            bad.append(f"{cell}: no heading {CELLS[cell]} in {BENCH}")
            continue
        fig = re.search(r"<(\w+)\b[^>]*\bclass=\"[^\"]*\bfigure\b[^\"]*\"[^>]*>(.*?)</\1>", inner, re.S)
        shown = text_of(fig.group(2) if fig else inner)
        if "data-arm" in attrs:
            arm = attrs["data-arm"]
            if arm not in ops:
                bad.append(f"{cell}: no row for arm {arm!r}")
                continue
            want = abbreviate(ops[arm])
            if shown != want:
                bad.append(f"{cell} {arm}: the page says {shown!r}, the table {want!r}")
            m = re.search(r"--w:\s*([\d.]+)%", attrs.get("style", ""))
            if m:
                shown_arms = [a for _, s2, _ in ELEMENT.findall(html) for a in [attrs_of(s2)]
                              if a.get("data-cell") == cell and "data-arm" in a and "--w" in a.get("style", "")]
                top = max(ops[a["data-arm"]] for a in shown_arms if a["data-arm"] in ops)
                want_w = 100 * ops[arm] / top
                if abs(float(m.group(1)) - want_w) > 0.5:
                    bad.append(f"{cell} {arm}: bar width {m.group(1)}%, the figure's share is {want_w:.1f}%")
            figures += 1
        elif "data-pair" in attrs:
            arm = attrs["data-pair"]
            if arm not in pairs:
                bad.append(f"{cell}: no pair line with a factor against {arm!r}")
                continue
            if shown != pairs[arm]:
                bad.append(f"{cell} vs {arm}: the page says {shown!r}, the pair line {pairs[arm]!r}")
            factors += 1
        else:
            bad.append(f"{cell}: an element with data-cell but neither data-arm nor data-pair")
    if bad:
        fail("figures", "; ".join(bad))
    else:
        ok("figures", f"{figures} figures, {factors} factors")

def check_no_version_literal():
    """The install rows name no version: the page is not pinned to a commit
    the way the README is, and a literal here goes stale at the next tag.
    The benchmark section names the release it measured, which is a fact."""
    start = html.index('<section id="install">')
    install = html[start:html.index("<section", start + 1)]
    hits = sorted(set(re.findall(r"(?<![\d.])\d+\.\d+\.\d+(?![\d.])", install)))
    if hits:
        fail("no version literal", "in the install section: " + ", ".join(hits))
    else:
        ok("no version literal", "in the install section")

ANIM_BUDGET = 30 * 1024
ANIM_SVG = re.compile(r'<svg[^>]*\baria-labelledby="anim-title"[^>]*>.*?</svg>', re.S)
ANIM_CSS = re.compile(r"/\* anim \*/.*?/\* /anim \*/", re.S)

def check_animation():
    """The hero animation — the inline SVG, its driver and its CSS block —
    carries an accessible name and stays under its own budget."""
    svg = ANIM_SVG.search(html)
    if not svg:
        fail("animation", 'no inline <svg> with aria-labelledby="anim-title" on the page')
        return
    if 'role="img"' not in svg.group(0).split(">", 1)[0]:
        fail("animation", 'the SVG lacks role="img"')
        return
    css_path, js_path = os.path.join(SITE, "assets", "site.css"), os.path.join(SITE, "assets", "site.js")
    block = ANIM_CSS.search(open(css_path, encoding="utf-8").read())
    if not block:
        fail("animation", f"no /* anim */ ... /* /anim */ block in {css_path}")
        return
    total = (len(svg.group(0).encode()) + len(block.group(0).encode())
             + (os.path.getsize(js_path) if os.path.exists(js_path) else 0))
    if total < ANIM_BUDGET:
        ok("animation", f"{total} bytes (svg, css block, site.js), budget {ANIM_BUDGET}")
    else:
        fail("animation", f"{total} bytes (svg, css block, site.js), budget {ANIM_BUDGET}")

def check_budget(referenced):
    files = {INDEX, *referenced}
    for path in list(files):
        if path.endswith(".css"):
            css = open(path, encoding="utf-8").read()
            for u in re.findall(r"url\(\s*[\"']?([^\"')]+)[\"']?\s*\)", css):
                if not re.match(r"^(data:|[a-z]+:)", u):
                    files.add(os.path.normpath(os.path.join(os.path.dirname(path), u.partition("#")[0])))
    total = sum(os.path.getsize(p) for p in files if os.path.exists(p))
    if total < BUDGET:
        ok("budget", f"{total} bytes over {len(files)} files, budget {BUDGET}")
    else:
        fail("budget", f"{total} bytes over {len(files)} files, budget {BUDGET}")

referenced = check_links()
check_metadata()
check_landmarks()
check_no_http()
check_mark()
check_figures()
check_no_version_literal()
check_animation()
check_budget(referenced)
sys.exit(status)
PY
