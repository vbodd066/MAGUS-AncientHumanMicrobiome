#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./count_id.sh file1 [file2 ...]
# Counts unique IDs in the FIRST column, for either CSV (comma) or TSV (tab).
# Skips the header row.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 file1 [file2 ...]" >&2
  exit 2
fi

for f in "$@"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: file not found: $f" >&2
    continue
  fi

  first_line="$(head -n 1 "$f")"

  # Detect delimiter from header line
  if [[ "$first_line" == *$'\t'* ]]; then
    delim=$'\t'
  else
    delim=','  # default
  fi

  count="$(
    awk -F"$delim" 'NR>1 && $1!="" {print $1}' "$f" \
      | sort -u \
      | wc -l \
      | tr -d ' '
  )"

  echo "$f: $count unique IDs in column 1"
done
