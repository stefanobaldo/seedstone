#!/usr/bin/env python3
"""The SeedStone mark, as data: four flat shapes on a 64x64 grid, in the
brand's four colours. Every file derived from it deterministically is
emitted by this script and never edited by hand; CI regenerates each one
and refuses a byte of difference.

Usage (from the repository root):
    python3 -I scripts/brand.py emit         write every derivative
    python3 -I scripts/brand.py --check      exit 1 naming each one that differs
    python3 -I scripts/brand.py --selftest   the mark's own invariants
    python3 -I scripts/brand.py --png <target> <out.png>
        target: avatar (512, on the rim colour), touch (180, on the rim
        colour), favicon-16, favicon-32 (transparent). Needs ImageMagick 7
        (`magick`); the rasters are made by hand, once per change of the
        mark, and are not byte-checked.

The installer's sprite is the one derivative that is drawn, not computed:
SPRITE below is the mark at 16 by 14 pixels, one character per pixel,
drawn by hand in the palette. `emit` writes its seven half-block lines
between the sprite markers in site/install.sh, and `--check` holds them.

To regenerate after a change to the mark: python3 -I scripts/brand.py emit
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

RIM = "#1f2831"
FACE = "#3a4756"
AMBER = "#f0a030"
HIGHLIGHT = "#ffc45c"
PALETTE = (RIM, FACE, AMBER, HIGHLIGHT)

# The silhouette is the rim alone, so the mark is exactly round; the face
# sits two units up so the rim shows as a crescent at the bottom.
SHAPES = [
    f'<circle cx="32" cy="32" r="28" fill="{RIM}"/>',
    f'<circle cx="32" cy="30" r="26" fill="{FACE}"/>',
    f'<path d="M32 15 C26 25 21 30 21 37 A11 11 0 0 0 43 37 C43 30 38 25 32 15 Z" fill="{AMBER}"/>',
    f'<path d="M32 15 C26 25 21 30 21 37 A11 11 0 0 0 29 47 C24 42 24 36 32 26 Z" fill="{HIGHLIGHT}"/>',
]

# At 16 px the seed above is four pixels; the favicon draws a larger seed
# and no facet.
FAVICON_SEED = "M32 11 C25 23 18 29 18 38 A14 14 0 0 0 46 38 C46 29 39 23 32 11 Z"
SHAPES_FAVICON = SHAPES[:2] + [f'<path d="{FAVICON_SEED}" fill="{AMBER}"/>']


WORDMARKS = ("docs/assets/wordmark-light.svg", "docs/assets/wordmark-dark.svg")

# The sprite: `.` transparent, `r` rim, `f` face, `a` amber, `h` highlight.
# Sixteen pixels wide and fourteen tall, because a terminal cell is about
# two and a half times as tall as it is wide and a half-block pixel is half
# a cell: a disc that is round on screen is wider than it is tall in pixels.
# The rim is one pixel at the sides and two below, none above (the face sits
# up, as it does in the mark); the seed is two pixels wide at its tip and
# six at its widest, the highlight its left edge.
SPRITE = """\
.....rffffr.....
...rffffffffr...
..rffffffffffr..
.rfffffaafffffr.
.ffffffhaffffff.
rfffffhaaafffffr
rfffffhaaafffffr
rffffhhaaaaffffr
rffffhhaaaaffffr
.rfffhaaaaafffr.
.rrfffhaaafffrr.
..rrfffaafffrr..
...rrrffffrrr...
.....rrrrrr.....
"""
SPRITE_COLOURS = {"r": RIM, "f": FACE, "a": AMBER, "h": HIGHLIGHT}
INSTALLER = "site/install.sh"
SPRITE_MARKERS = ("# --- sprite ---\n", "# --- /sprite ---\n")
# Two columns of margin before the sprite, three after it; the installer's
# lines start at the column after that, beside it.
SPRITE_MARGIN = (2, 3)


def svg(shapes, title):
    return "\n".join([
        '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64" role="img">',
        f"<title>{title}</title>",
        *shapes,
        "</svg>",
        "",
    ])


def rgb(colour):
    return ";".join(str(int(colour[i:i + 2], 16)) for i in (1, 3, 5))


def sprite():
    """The sprite as seven lines of half-block characters with 24-bit
    colour sequences, each 16 + margins columns wide: a row pair becomes
    one line, `▀` in the upper pixel's colour over the lower pixel's, `▄`
    where only the lower one is drawn, `█` where both share a colour, a
    space where neither is."""
    rows = SPRITE.splitlines()
    lines = []
    for top, bottom in zip(rows[::2], rows[1::2]):
        line, current = "", None
        for t, b in zip(top, bottom):
            if t == "." and b == ".":
                glyph, colours = " ", None
            elif b == ".":
                glyph, colours = "▀", f"38;2;{rgb(SPRITE_COLOURS[t])}"
            elif t == ".":
                glyph, colours = "▄", f"38;2;{rgb(SPRITE_COLOURS[b])}"
            elif t == b:
                glyph, colours = "█", f"38;2;{rgb(SPRITE_COLOURS[t])}"
            else:
                glyph, colours = "▀", f"38;2;{rgb(SPRITE_COLOURS[t])};48;2;{rgb(SPRITE_COLOURS[b])}"
            if colours != current:
                line += "\\033[0m" if colours is None else f"\\033[{colours}m"
                current = colours
            line += glyph
        if current is not None:
            line += "\\033[0m"
        lines.append(" " * SPRITE_MARGIN[0] + line + " " * SPRITE_MARGIN[1])
    return lines


def sprite_block():
    """What sits between the markers in the installer: a function printing
    the sprite's Nth line, and the sprite's height and width as constants."""
    width = SPRITE_MARGIN[0] + len(SPRITE.splitlines()[0]) + SPRITE_MARGIN[1]
    lines = sprite()
    out = [
        "# The mark, 16 by 14 pixels in half-block characters and 24-bit colour,",
        "# one line per call. Emitted by scripts/brand.py from the grid it holds;",
        "# edit the grid there, never these lines.",
        f"SPRITE_LINES={len(lines)}",
        f"SPRITE_WIDTH={width}",
        "sprite_line() {",
        '  case "$1" in',
    ]
    for n, line in enumerate(lines, 1):
        out.append(f"  {n}) printf '{line}' ;;")
    out.append("  esac")
    out.append("}")
    return "\n".join(out) + "\n"


def installer_text():
    """The installer as it must read: the file on disk with the span between
    the sprite markers replaced by what `sprite_block` emits. None when the
    file or a marker is missing."""
    try:
        with open(INSTALLER, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        return None
    start, end = SPRITE_MARKERS
    i, j = text.find(start), text.find(end)
    if i < 0 or j < i:
        return None
    return text[:i + len(start)] + sprite_block() + text[j:]


def targets():
    """Every deterministic derivative: path -> the bytes it must hold."""
    return {
        "docs/assets/logo.svg": svg(SHAPES, "SeedStone"),
        "site/favicon.svg": svg(SHAPES_FAVICON, "SeedStone"),
        INSTALLER: installer_text(),
    }


def emit():
    for path, text in targets().items():
        if text is None:
            print(f"{path}: not written, the file or its sprite markers are missing", file=sys.stderr)
            continue
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print(path)


def check():
    status = 0
    for path, text in targets().items():
        try:
            with open(path, "rb") as f:
                current = f.read()
        except FileNotFoundError:
            current = None
        if text is None:
            print(f"{path}: the file or its sprite markers are missing", file=sys.stderr)
            status = 1
        elif current != text.encode("utf-8"):
            print(f"{path}: regenerate with: python3 -I scripts/brand.py emit", file=sys.stderr)
            status = 1
    # The wordmarks' letter outlines come from a font and are not emitted
    # here; only the mark inside them is held, shape by shape.
    for path in WORDMARKS:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if not all(shape in text for shape in SHAPES):
            print(f"{path}: does not carry the mark's shapes verbatim", file=sys.stderr)
            status = 1
    return status


# target -> (shapes, size in px, background)
RASTERS = {
    "avatar": (SHAPES, 512, RIM),
    "touch": (SHAPES, 180, RIM),
    "favicon-16": (SHAPES_FAVICON, 16, "none"),
    "favicon-32": (SHAPES_FAVICON, 32, "none"),
}


def png(target, out):
    if target not in RASTERS:
        print(f"unknown target {target}; one of: {', '.join(RASTERS)}", file=sys.stderr)
        return 2
    if shutil.which("magick") is None:
        print("--png needs ImageMagick 7 (`magick`) on the PATH", file=sys.stderr)
        return 1
    shapes, size, background = RASTERS[target]
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "mark.svg")
        with open(src, "w", encoding="utf-8") as f:
            f.write(svg(shapes, "SeedStone"))
        # Rendered at twice the target and scaled down, never up. The grid's
        # own 4-unit margin keeps the rim whole under a circle crop.
        density = str(72 * size * 2 // 64)
        subprocess.run(["magick", "-background", background, "-density", density, src,
                        "-resize", f"{size}x{size}", "-gravity", "center",
                        "-extent", f"{size}x{size}", "-depth", "8", "-strip", out], check=True)
    subprocess.run(["magick", "identify", out], check=True)
    return 0


def selftest():
    rim = re.fullmatch(r'<circle cx="(\d+)" cy="(\d+)" r="(\d+)" fill="[^"]+"/>', SHAPES[0])
    assert rim, SHAPES[0]
    cx, cy, r = map(int, rim.groups())
    assert (cx, cy, r) == (32, 32, 28), (cx, cy, r)
    left, top, right, bottom = cx - r, cy - r, cx + r, cy + r
    assert right - left == bottom - top, "the silhouette is not square"
    assert 0 <= left and right <= 64 and 0 <= top and bottom <= 64, "the silhouette leaves the grid"
    for shape in SHAPES + SHAPES_FAVICON:
        for fill in re.findall(r'fill="([^"]*)"', shape):
            assert fill in PALETTE, f"{fill} is not a brand colour"
    master = svg(SHAPES, "SeedStone")
    for banned in ("<text", "gradient", "filter"):
        assert banned not in master, f"the mark carries {banned}"
    assert f'd="{FAVICON_SEED}"' in SHAPES_FAVICON[2]
    assert FAVICON_SEED == "M32 11 C25 23 18 29 18 38 A14 14 0 0 0 46 38 C46 29 39 23 32 11 Z"
    rows = SPRITE.splitlines()
    assert len(rows) == 14 and all(len(r) == 16 for r in rows), "the sprite is not 16 by 14"
    assert set("".join(rows)) <= set(".rfah"), "the sprite uses a character outside .rfah"
    assert all(r == r[::-1] for r in rows if "a" not in r), "the disc is not symmetric"
    lines = sprite()
    assert len(lines) == 7, len(lines)
    width = SPRITE_MARGIN[0] + 16 + SPRITE_MARGIN[1]
    for line in lines:
        visible = re.sub(r"\\033\[[0-9;]*m", "", line)
        assert len(visible) == width, (len(visible), visible)
        assert "38;2;" in line, "a sprite line without a pixel"
    assert "%" not in "".join(lines), "a % would be read by printf"
    print("brand selftest: ok")


def main(argv):
    if argv == ["emit"]:
        emit()
        return 0
    if argv == ["--check"]:
        return check()
    if argv == ["--selftest"]:
        selftest()
        return 0
    if len(argv) == 3 and argv[0] == "--png":
        return png(argv[1], argv[2])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
