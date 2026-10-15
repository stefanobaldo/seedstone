#!/usr/bin/env bash
# The site under `site/` holds together: every link on the page resolves —
# to a file this repository tracks, or to a heading on one of the
# documentation pages — the page carries the metadata a crawler and a link
# preview read, it names no `http://` URL, the mark it shows is the mark
# `brand.py` holds, and the page with everything it references stays
# under its byte budget. Each check prints one `ok:` or `FAIL:` line.
#
# Usage: check-site.sh            (from the repository root)
set -euo pipefail

python3 -I - <<'PY'
import os, re, subprocess, sys

sys.path.insert(0, "scripts")
import brand

SITE = "site"
INDEX = os.path.join(SITE, "index.html")
REPO = "https://github.com/stefanobaldo/seedstone/blob/main/"
ORIGIN = "https://seedstone.io/"
BUDGET = 200 * 1024
# Paths the page links before the file lands beside it; each entry leaves
# with the change that adds its file.
PENDING = {"install.sh", "assets/social.png"}

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

FAST = ("Against Redis 8.10.0 at one I/O thread on a 16-core machine, v0.3.2 reads ahead on "
        "throughput in 12 of 13 measured pairs, by 1.03\u00d7 to 5.16\u00d7, and more expensive per "
        "operation in all 13.")

def check_charts():
    """The charts on the page are the README's, byte for byte."""
    bad = []
    for name in ("benchmarks.svg", "benchmarks-dark.svg"):
        site_copy, source = os.path.join(SITE, "assets", name), os.path.join("docs", "assets", name)
        try:
            same = open(site_copy, "rb").read() == open(source, "rb").read()
        except FileNotFoundError:
            same = False
        if not same:
            bad.append(f"{site_copy} is not {source}: cp {source} {site_copy}")
    if bad:
        fail("charts", "; ".join(bad))
    else:
        ok("charts", "both copies identical to docs/assets")

def check_fast_sentence():
    """The benchmark verdict is the benchmark page's sentence, not a paraphrase."""
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html))
    if FAST in text:
        ok("fast sentence", "verbatim")
    else:
        fail("fast sentence", "the page does not carry the pair verdict verbatim")

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
    css_path, js_path = os.path.join(SITE, "assets", "site.css"), os.path.join(SITE, "assets", "hero.js")
    block = ANIM_CSS.search(open(css_path, encoding="utf-8").read())
    if not block:
        fail("animation", f"no /* anim */ ... /* /anim */ block in {css_path}")
        return
    total = (len(svg.group(0).encode()) + len(block.group(0).encode())
             + (os.path.getsize(js_path) if os.path.exists(js_path) else 0))
    if total < ANIM_BUDGET:
        ok("animation", f"{total} bytes (svg, css block, hero.js), budget {ANIM_BUDGET}")
    else:
        fail("animation", f"{total} bytes (svg, css block, hero.js), budget {ANIM_BUDGET}")

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
check_no_http()
check_mark()
check_charts()
check_fast_sentence()
check_no_version_literal()
check_animation()
check_budget(referenced)
sys.exit(status)
PY
