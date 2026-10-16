#!/usr/bin/env bash
# The site under `site/` holds together: every link on the page resolves —
# to a file this repository tracks, or to a heading on one of the
# documentation pages — it loads nothing from another origin but the
# analytics beacon, and `_headers`' policy names that host alone for
# scripts, the page carries the metadata a crawler and a link
# preview read, it names no `http://` URL, the mark it shows is the mark
# `brand.py` holds, every benchmark figure it shows is the benchmark page's
# for that cell and arm, the installer it serves is POSIX sh that talks to
# the two hosts it may, and the page with everything it references stays
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
# A fixture may stand in for the page or the installer (the checker's own
# tests do that).
INDEX = os.environ.get("CHECK_SITE_INDEX", os.path.join(SITE, "index.html"))
INSTALLER = os.environ.get("CHECK_SITE_INSTALLER", os.path.join(SITE, "install.sh"))
REPO = "https://github.com/stefanobaldo/seedstone/blob/main/"
ORIGIN = "https://seedstone.io/"
BUDGET = 200 * 1024
# Paths the page links before the file lands beside it; each entry leaves
# with the change that adds its file.
PENDING = set()
# The one thing the page loads from another origin: Cloudflare's analytics
# beacon. `_headers`' Content-Security-Policy admits its host and no other.
BEACON = "https://static.cloudflareinsights.com/beacon.min.js"
HEADERS = os.environ.get("CHECK_SITE_HEADERS", os.path.join(SITE, "_headers"))

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

def page_policy(headers):
    """The Content-Security-Policy lines `_headers` gives the page: those in
    the block whose path line is `/*`, which Pages applies to every path."""
    lines, block = [], None
    for line in headers.splitlines():
        if line.strip() == "" or line.lstrip().startswith("#"):
            continue
        if not line[0].isspace():
            block = line.strip()
        elif block == "/*":
            name, _, value = line.strip().partition(":")
            if name.lower() == "content-security-policy":
                lines.append(value.strip())
    return lines

def check_origins():
    """Everything the browser fetches to show the page comes from this site,
    but the beacon; the page runs no inline script; and the policy in
    `_headers` lets the beacon's script run and names no other script
    source, so a script the policy would block cannot pass here."""
    foreign = sorted(u for u in LOADED_URLS if site_file(u) is None and u != BEACON)
    inline = re.findall(r"<script\b(?![^>]*\bsrc=)[^>]*>|\son[a-z]+=", html, re.I)
    headers = open(HEADERS, encoding="utf-8").read() if os.path.exists(HEADERS) else ""
    policies = page_policy(headers)
    want = ["'self'", BEACON.split("/beacon")[0]] if BEACON in LOADED_URLS else ["'self'"]
    if foreign:
        fail("origins", "loaded from another origin: " + ", ".join(foreign))
        return
    if inline:
        fail("origins", "an inline script or event handler, which the policy blocks: " + ", ".join(sorted(set(inline))))
        return
    if len(policies) != 1:
        fail("origins", f"{len(policies)} Content-Security-Policy lines under /* in {HEADERS}, want one")
        return
    directives = {d.split()[0]: d.split()[1:] for d in policies[0].split(";") if d.strip()}
    extra = sorted(d for d in directives if d.startswith("script-src-"))
    script_src = directives.get("script-src", directives.get("default-src"))
    if extra:
        fail("origins", "the policy sets " + ", ".join(extra) + ", which overrides script-src")
    elif script_src is None or sorted(script_src) != sorted(want):
        fail("origins", f"script-src is {script_src}, want {want}")
    else:
        ok("origins", "this site" + (" and the analytics beacon" if BEACON in LOADED_URLS else ""))

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
# benchmark page: a `###` heading, then optionally a `####` one, each the
# heading's text before its " — " with the backticks dropped. Under the
# innermost heading every Markdown table until the next heading of the same
# or a higher level counts (the main table and the *Other engines* table
# both), and so do the `- seedstone vs <arm>:` pair lines.
CELLS = {
    "keys-7000": ("KEYS over 7 000 keys",),
    "set-64-ex60-d64": ("SET 64 B, without and with EX 60", "With EX 60"),
    "set-64-d64": ("SET 64 B, without and with EX 60", "Without EX"),
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

def heading_name(text):
    return re.sub(r"\s+", " ", text.replace("`", "")).split(" — ")[0].strip()

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
                inside = len(m.group(1)) == level and heading_name(m.group(2)) == want
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
        m = re.match(r"- seedstone vs ([\w-]+): (ahead|behind) ([\d.]+)x on throughput", line)
        if m:
            factors[m.group(1)] = (m.group(2), m.group(3) + "\u00d7")
    return ops, factors

# The name the page shows for each arm of the table: a figure is bound to
# its arm by `data-arm`, and the name a reader sees beside it must be that
# arm's, or the right number stands under the wrong engine.
ARM_NAME = {
    "seedstone": "seedstone", "dragonfly": "Dragonfly", "garnet": "Garnet",
    "redis-iot1": "Redis", "redis-iot4": "Redis \u00d74",
    "valkey-iot1": "Valkey", "valkey-iot4": "Valkey \u00d74",
}

from html.parser import HTMLParser
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}

class Elements(HTMLParser):
    """Every element on the page with its attributes, its ancestors and its
    text (entities decoded), so a bound element is read as the browser
    reads it: a nested element is seen, a quote style is irrelevant."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.all = [], []
    def handle_starttag(self, tag, attrs):
        el = {"tag": tag, "attrs": dict(attrs), "ancestors": list(self.stack), "text": [], "classes": set()}
        el["classes"] = set((el["attrs"].get("class") or "").split())
        self.all.append(el)
        if tag not in VOID:
            self.stack.append(el)
    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i]["tag"] == tag:
                del self.stack[i:]
                break
    def handle_data(self, data):
        for el in self.stack:
            el["text"].append(data)
        self.loose(data)
    def loose(self, data):
        pass

def text_of(el):
    """The element's text, whitespace folded: the table's thousands
    separator is a narrow no-break space (U+202F), and the page's may be
    any space that keeps a figure from wrapping; both read as one space."""
    return re.sub(r"\s+", " ", "".join(el["text"])).strip()

def check_figures():
    """Every benchmark figure the page shows is bound to its cell and its
    arm (`data-cell`, `data-arm`), every factor to its cell and the arm it
    is against (`data-pair`), and each matches the benchmark page's table
    for exactly that cell and arm, abbreviated as the README's chart does.
    A bound element carries the figure in a `.figure` child (`.factor` for
    a factor) and the engine's name in an `.arm` child, and the name must
    be the arm's, so the right number cannot stand under the wrong engine.
    A bar's width (`--w`) is its figure's share of the cell's largest shown
    figure, so a bar cannot say one thing and its text another, and a bar
    without a width fails rather than drawing an empty track. A factor the
    table records as behind fails: the page shows leads only. An element
    that looks like one of these (`.bar`, `.figure`, `.factor`) and is not
    bound fails: a figure may not opt out of the check by forgetting its
    attributes."""
    bad, figures, factors = [], 0, 0
    if re.search(r"benchmarks(-dark)?\.svg", html):
        bad.append("the page references the README's chart")
    def bound(el):
        return "data-cell" in el["attrs"]
    # A figure in the benchmark sections that no bound element carries — a
    # `4.65 M` or a `2.96×` typed into prose — is unchecked, so it fails.
    FIGURE = re.compile(r"\b\d+(?:\.\d+)?\s?[kM]\b|\b\d\.\d\d\u00d7")
    loose = []
    class Page(Elements):
        def loose(self, data):
            chart = any(a["attrs"].get("id") == "fast" or "proof" in a["classes"] for a in self.stack)
            if chart and not any(bound(a) for a in self.stack):
                loose.extend(FIGURE.findall(data))
    parser = Page()
    parser.feed(html)
    for text in loose:
        bad.append(f"{text!r} is a figure in a benchmark section and is not bound to a cell")
    for el in parser.all:
        if el["classes"] & {"bar", "figure", "factor"} and not bound(el) and not any(bound(a) for a in el["ancestors"]):
            bad.append(f"<{el['tag']} class=\"{' '.join(sorted(el['classes']))}\"> showing {text_of(el)!r} is not bound to a cell")
    per_cell = {}
    for el in parser.all:
        if not bound(el):
            continue
        attrs, cell = el["attrs"], el["attrs"]["data-cell"]
        if any(bound(a) for a in el["ancestors"]):
            bad.append(f"{cell}: a bound element inside another bound element ({text_of(el)!r})")
            continue
        if cell not in CELLS:
            bad.append(f"data-cell={cell!r} is not a cell this check knows ({', '.join(CELLS)})")
            continue
        if cell not in per_cell:
            per_cell[cell] = bench_cell(cell)
        ops, pairs = per_cell[cell]
        if ops is None:
            bad.append(f"{cell}: no heading {CELLS[cell]} in {BENCH}")
            continue
        child = lambda cls: next((c for c in parser.all if el in c["ancestors"] and cls in c["classes"]), None)
        arm = attrs.get("data-arm", attrs.get("data-pair"))
        if arm not in ARM_NAME:
            bad.append(f"{cell}: arm {arm!r} has no name this check knows ({', '.join(ARM_NAME)})")
            continue
        name = child("arm")
        if name is None:
            bad.append(f"{cell} {arm}: no .arm child names the engine")
        elif text_of(name) != ARM_NAME[arm]:
            bad.append(f"{cell} {arm}: the page names {text_of(name)!r}, the arm is {ARM_NAME[arm]!r}")
        if "data-arm" in attrs:
            figure_child = child("figure")
            if figure_child is None:
                bad.append(f"{cell} {arm}: no .figure child carries the figure")
                continue
            shown = text_of(figure_child)
            if arm not in ops:
                bad.append(f"{cell}: no row for arm {arm!r}")
                continue
            want = abbreviate(ops[arm])
            if shown != want:
                bad.append(f"{cell} {arm}: the page says {shown!r}, the table {want!r}")
            style = attrs.get("style", "")
            if "bar" in el["classes"] and "--w" not in style:
                bad.append(f"{cell} {arm}: a bar without --w draws an empty track")
            if "--w" in style:
                m = re.search(r"--w:\s*([\d.]+)%", style)
                if not m:
                    bad.append(f"{cell} {arm}: --w is not a percentage: {style!r}")
                else:
                    shown_arms = [e["attrs"]["data-arm"] for e in parser.all if bound(e) and e["attrs"]["data-cell"] == cell
                                  and "data-arm" in e["attrs"] and "--w" in e["attrs"].get("style", "")]
                    top = max(ops[a] for a in shown_arms if a in ops)
                    want_w = 100 * ops[arm] / top
                    if abs(float(m.group(1)) - want_w) > 0.5:
                        bad.append(f"{cell} {arm}: bar width {m.group(1)}%, the figure's share is {want_w:.1f}%")
            figures += 1
        elif "data-pair" in attrs:
            factor_child = child("factor")
            if factor_child is None:
                bad.append(f"{cell} vs {arm}: no .factor child carries the factor")
                continue
            shown = text_of(factor_child)
            if arm not in pairs:
                bad.append(f"{cell}: no pair line with a factor against {arm!r}")
                continue
            direction, want = pairs[arm]
            if direction != "ahead":
                bad.append(f"{cell} vs {arm}: the table says behind, and the page shows leads only")
            if shown != want:
                bad.append(f"{cell} vs {arm}: the page says {shown!r}, the pair line {want!r}")
            factors += 1
        else:
            bad.append(f"{cell}: an element with data-cell but neither data-arm nor data-pair")
    if bad:
        fail("figures", "; ".join(bad))
    else:
        ok("figures", f"{figures} figures, {factors} factor" + ("s" if factors != 1 else ""))

def check_no_version_literal():
    """The install rows name no version: the page is not pinned to a commit
    the way the README is, and a literal here goes stale at the next tag.
    The benchmark section names the release it measured, which is a fact."""
    start = html.find('<section id="install">')
    if start < 0:
        fail("no version literal", 'no <section id="install"> on the page')
        return
    # The section ends where the next top-level section starts; the install
    # card nests a <section> per channel, so the next bare "<section" is
    # not the end.
    install = html[start:html.index('<section id="', start + 1)]
    hits = sorted(set(re.findall(r"(?<![\d.])\d+\.\d+\.\d+(?![\d.])", install)))
    if hits:
        fail("no version literal", "in the install section: " + ", ".join(hits))
    else:
        ok("no version literal", "in the install section")

ANIM_BUDGET = 30 * 1024
ANIM_SVG = re.compile(r'<svg[^>]*\baria-labelledby="anim-title"[^>]*>.*?</svg>', re.S)
ANIM_CSS = re.compile(r"/\* anim \*/.*?/\* /anim \*/", re.S)
MOTION_CSS = re.compile(r"/\* motion \*/.*?/\* /motion \*/", re.S)

def check_animation():
    """Everything on the page that moves — the hero's inline SVG, the
    script (its driver, the tabs and the scroll-triggered motions) and the
    two CSS blocks — carries an accessible name and stays under one budget."""
    svg = ANIM_SVG.search(html)
    if not svg:
        fail("animation", 'no inline <svg> with aria-labelledby="anim-title" on the page')
        return
    if 'role="img"' not in svg.group(0).split(">", 1)[0]:
        fail("animation", 'the SVG lacks role="img"')
        return
    css_path, js_path = os.path.join(SITE, "assets", "site.css"), os.path.join(SITE, "assets", "site.js")
    css = open(css_path, encoding="utf-8").read()
    blocks = []
    for name, pattern in (("anim", ANIM_CSS), ("motion", MOTION_CSS)):
        block = pattern.search(css)
        if not block:
            fail("animation", f"no /* {name} */ ... /* /{name} */ block in {css_path}")
            return
        blocks.append(block.group(0))
    total = (len(svg.group(0).encode()) + sum(len(b.encode()) for b in blocks)
             + (os.path.getsize(js_path) if os.path.exists(js_path) else 0))
    if total < ANIM_BUDGET:
        ok("animation", f"{total} bytes (svg, two css blocks, site.js), budget {ANIM_BUDGET}")
    else:
        fail("animation", f"{total} bytes (svg, two css blocks, site.js), budget {ANIM_BUDGET}")

# The installer may talk to the repository and to the site, and to nothing
# else; a URL on any other host is a request the reader did not sign up for.
INSTALLER_HOSTS = ("https://github.com/stefanobaldo/seedstone", "https://seedstone.io")

def check_installer():
    """The installer is POSIX sh by two linters, every URL in it is under
    the two allowed hosts, and `main "$@"` is its last line."""
    if not os.path.exists(INSTALLER):
        fail("installer", f"{INSTALLER} missing")
        return
    bad = []
    for cmd in (["shellcheck", "--shell=sh", INSTALLER], ["checkbashisms", INSTALLER]):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True)
        except FileNotFoundError:
            bad.append(f"{cmd[0]} is not on the PATH")
            continue
        if r.returncode != 0:
            said = (r.stdout + r.stderr).strip().splitlines()
            said = [l.strip() for l in said if re.search(r"SC\d+|bashism", l)] or said or [f"exit {r.returncode}"]
            bad.append(f"{cmd[0]}: {said[0]}")
    text = open(INSTALLER, encoding="utf-8").read()
    urls = re.findall(r"https?://[^\s\"'`)>}]+", text)
    for url in urls:
        if not any(url == h or url.startswith(h + "/") for h in INSTALLER_HOSTS):
            bad.append(f"{url} is not under {' or '.join(INSTALLER_HOSTS)}")
    if text.rstrip("\n").rsplit("\n", 1)[-1] != 'main "$@"':
        bad.append('the last line is not main "$@"')
    if bad:
        fail("installer", "; ".join(bad))
    else:
        ok("installer", f"shellcheck and checkbashisms clean, {len(urls)} URLs under the two hosts")

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
check_origins()
check_metadata()
check_landmarks()
check_no_http()
check_mark()
check_figures()
check_no_version_literal()
check_animation()
check_installer()
check_budget(referenced)
sys.exit(status)
PY
