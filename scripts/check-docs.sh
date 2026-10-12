#!/usr/bin/env bash
# Every relative link in every tracked Markdown file resolves — Markdown
# inline and reference links and HTML href/src alike, down to the heading a
# `#fragment` names — and the README's Rust badge says what
# rust-toolchain.toml says. A moved paragraph or a renamed heading leaves a
# dead link that no other gate sees; a toolchain bump leaves a badge that
# lies.
#
# Usage: check-docs.sh            (from the repository root)
set -euo pipefail

python3 <<'PY'
import os, re, subprocess, sys

FENCE = re.compile(r"^(```|~~~).*?^\1", re.M | re.S)
LINKS = [
    re.compile(r"\]\(\s*<([^>]+)>"),                       # [a](<path>)
    re.compile(r"\]\(\s*([^)\s<][^)\s]*)"),                # [a](path "title")
    re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?([^>\s]+)", re.M),  # [r]: path
    re.compile(r"""(?:href|src)=["']([^"']+)["']"""),      # HTML
]

def prose(path):
    with open(path, encoding="utf-8") as f:
        return FENCE.sub("", f.read())

anchors = {}
def anchors_of(path):
    """The fragments GitHub renders for a Markdown file: one per heading,
    slugged its way (lower case, punctuation dropped, spaces to hyphens,
    repeats suffixed -1, -2 …), plus explicit HTML ids and names."""
    if path not in anchors:
        text, seen, found = prose(path), {}, set()
        for h in re.findall(r"^#{1,6}\s+(.*?)\s*#*\s*$", text, re.M):
            slug = re.sub(r"[^\w\- ]", "", h.lower()).replace(" ", "-")
            n = seen.get(slug, 0)
            seen[slug] = n + 1
            found.add(slug if n == 0 else f"{slug}-{n}")
        found.update(re.findall(r"""(?:id|name)=["']([^"']+)["']""", text))
        anchors[path] = found
    return anchors[path]

files = subprocess.run(["git", "ls-files", "--", "*.md"], capture_output=True, text=True, check=True).stdout.split()
status = 0
for f in files:
    text = prose(f)
    targets = {m.group(1) for rx in LINKS for m in rx.finditer(text)}
    for target in sorted(targets):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("//"):
            continue
        path, _, frag = target.partition("#")
        dest = os.path.normpath(os.path.join(os.path.dirname(f), path)) if path else f
        if not os.path.exists(dest):
            print(f"{f}: dead link {target}")
            status = 1
        elif frag and dest.endswith(".md") and frag not in anchors_of(dest):
            print(f"{f}: no heading for #{frag} in {dest}")
            status = 1

toolchain = re.search(r'channel\s*=\s*"([0-9][0-9.]*)"', open("rust-toolchain.toml").read()).group(1)
if f"rust-{toolchain}-" not in open("README.md", encoding="utf-8").read():
    print(f"README.md: the Rust badge does not say {toolchain}")
    status = 1
sys.exit(status)
PY
