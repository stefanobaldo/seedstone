#!/usr/bin/env bash
# The GitHub social preview: the light wordmark on white, 1280×640, the
# size GitHub asks for. Written to the path given; uploaded by hand in the
# repository's settings (Social preview), which no API sets.
#
# Usage: social-preview.sh <out.png>        (needs ImageMagick 7: `magick`)
set -euo pipefail
out=${1:?out.png}
magick -background white -density 384 docs/assets/wordmark-light.svg \
  -resize 900x -gravity center -extent 1280x640 -depth 8 -strip "$out"
magick identify "$out"
