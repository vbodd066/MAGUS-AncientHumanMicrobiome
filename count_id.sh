#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./count_id.sh file.csv
# Counts unique values in the FIRST column (CSV), skipping the header row.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 file.csv [more.csv ...]" >&2
  exit 2
fi

for f in "$@"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: file not found: $f" >&2
    continue
  fi

  count="$(
    awk -F',' 'NR>1 && $1!="" {print $1}' "$f" \
      | sort -u \
      | wc -l \
      | tr -d ' '
  )"

  echo "$f: $count unique IDs in column 1"
done
