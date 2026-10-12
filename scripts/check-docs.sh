#!/usr/bin/env bash
# Every relative link in every tracked Markdown file resolves, Markdown
# links and HTML href/src alike, and the README's Rust badge says what
# rust-toolchain.toml says. A moved paragraph leaves a dead link that no
# other gate sees; a toolchain bump leaves a badge that lies.
#
# Usage: check-docs.sh            (from the repository root)
set -euo pipefail

python3 - "$@" <<'PY'
import os, re, subprocess, sys
files = subprocess.run(["git", "ls-files", "--", "*.md"], capture_output=True, text=True, check=True).stdout.split()
link = re.compile(r"\]\(([^)\s]+)\)|(?:href|src)=\"([^\"]+)\"")
status = 0
for f in files:
    base = os.path.dirname(f)
    for m in link.finditer(open(f, encoding="utf-8").read()):
        target = m.group(1) or m.group(2)
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        path = target.split("#", 1)[0]
        if not os.path.exists(os.path.normpath(os.path.join(base, path))):
            print(f"{f}: dead link {target}")
            status = 1
toolchain = re.search(r'channel\s*=\s*"([0-9][0-9.]*)"', open("rust-toolchain.toml").read()).group(1)
readme = open("README.md", encoding="utf-8").read()
if f"rust-{toolchain}-" not in readme:
    print(f"README.md: the Rust badge does not say {toolchain}")
    status = 1
sys.exit(status)
PY
