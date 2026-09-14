#!/usr/bin/env bash
# Loads the keyspace the KEYS cell walks: 7 000 keys at the shape of a page
# cache's keyspace — a four-segment prefix, one of 64 prefixes, a fixed-width
# tail, 199 bytes a key — with 10 240-byte values, written as one RESP stream
# through `redis-cli --pipe`. Nothing here is random: key i is a function of i
# alone, so every run loads the same keyspace and two runs can be compared.
# No key or value here comes from anywhere real; only the shape is a cache's.
#
# Usage: keys-load.sh <port>            loads the keyspace, prints what landed
#        keys-load.sh --pattern <i>     prints the glob matching prefix i
#        keys-load.sh --resp            prints the RESP stream and loads nothing
# Env: KEYS (7000) PREFIXES (64) VALUE_BYTES (10240) CLI (redis-cli)
set -uo pipefail
export LC_ALL=C

KEYS=${KEYS:-7000}
PREFIXES=${PREFIXES:-64}
VALUE_BYTES=${VALUE_BYTES:-10240}
CLI=${CLI:-redis-cli}
# 128 bytes of filler in the second segment bring a key to 199 bytes:
# 3 + 128 + 1 + 2 + 1 + 64.
PAD=$(head -c 128 /dev/zero | tr '\0' p)

# One RESP array per SET. awk rather than a shell loop: 7 000 printf calls in
# bash cost seconds; here they cost nothing measurable.
resp() {
  awk -v n="$KEYS" -v prefixes="$PREFIXES" -v pad="$PAD" -v vb="$VALUE_BYTES" 'BEGIN {
    v = "x"; while (length(v) < vb) v = v v; v = substr(v, 1, vb)
    for (i = 0; i < n; i++) {
      k = sprintf(":1:%s:%02d:%064d", pad, i % prefixes, i)
      printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vb, v
    }
  }'
}

case ${1:-} in
  --pattern) printf ':1:%s:%02d:*\n' "$PAD" "${2:?usage: keys-load.sh --pattern <i>}"; exit 0;;
  --resp) resp; exit 0;;
esac
PORT=${1:?usage: keys-load.sh <port> | --pattern <i> | --resp}

PIPE=$(resp | "$CLI" -p "$PORT" --pipe 2>&1)
KEY_BYTES=$(printf ':1:%s:%02d:%064d' "$PAD" 0 0 | wc -c | tr -d ' ')
echo "    keyspace: $KEYS keys of $VALUE_BYTES B over $PREFIXES prefixes, $KEY_BYTES B a key; dbsize=$("$CLI" -p "$PORT" dbsize 2>/dev/null | tr -d '\r')"
echo "    pipe: $(grep -o 'errors: [0-9]*, replies: [0-9]*' <<<"$PIPE" || echo "unreadable: $PIPE")"
