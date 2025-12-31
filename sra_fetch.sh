#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./sra_fetch_runinfo.sh [queries.tsv] [out_dir]
#
# Optional environment variables:
#   SLEEP_SEC=0.8     # delay between queries (default 0.8)
#   MAX_HITS=5000     # skip queries with more than this many hits (default 5000)

QUERIES_TSV="${1:-queries_SRA.tsv}"
OUTDIR="${2:-sra_out_runinfo}"
mkdir -p "$OUTDIR" "$OUTDIR/tmp"

# Dependency checks (EDirect)
command -v esearch >/dev/null || { echo "ERROR: esearch not found (EDirect not active)."; exit 1; }
command -v efetch  >/dev/null || { echo "ERROR: efetch not found (EDirect not active)."; exit 1; }
command -v xtract  >/dev/null || { echo "ERROR: xtract not found (EDirect not active)."; exit 1; }

SLEEP_SEC="${SLEEP_SEC:-0.8}"
MAX_HITS="${MAX_HITS:-5000}"

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }

while IFS=$'\t' read -r tag query; do
  [[ -z "${tag// }" ]] && continue
  [[ "$tag" =~ ^# ]] && continue

  safe_tag="$(echo "$tag" | tr -cd 'A-Za-z0-9._-')"
  out_csv="$OUTDIR/${safe_tag}.runinfo.csv"
  tmp_csv="$OUTDIR/tmp/${safe_tag}.tmp.csv"

  echo "[$(timestamp)] [$tag] $query"

  # Best-effort hit count (does not stop the script if it fails)
  count="$(
    esearch -db sra -query "$query" </dev/null \
    | xtract -pattern ENTREZ_DIRECT -element Count 2>/dev/null \
    || true
  )"
  count="${count//[^0-9]/}"
  count="${count:-0}"
  echo "  hits (best-effort): $count"

  # Skip if too many hits (prevents huge downloads)
  if [[ "$count" -gt "$MAX_HITS" ]]; then
    echo "  (warning) skipping: too many hits (>$MAX_HITS). Refine/split this query."
    sleep "$SLEEP_SEC"
    continue
  fi

  # Fetch RunInfo (redirect stdin so the loop keeps reading queries.tsv)
  if ! esearch -db sra -query "$query" </dev/null | efetch -format runinfo > "$tmp_csv"; then
    echo "  (warning) fetch failed"
    rm -f "$tmp_csv"
    sleep "$SLEEP_SEC"
    continue
  fi

  # Treat header-only as no data
  if [[ "$(wc -l < "$tmp_csv")" -le 1 ]]; then
    echo "  (warning) no rows (header-only)"
    rm -f "$tmp_csv"
    sleep "$SLEEP_SEC"
    continue
  fi

  mv "$tmp_csv" "$out_csv"
  echo "  -> $out_csv"

  sleep "$SLEEP_SEC"
done < "$QUERIES_TSV"

echo "Done. RunInfo CSVs are in: $OUTDIR/"
