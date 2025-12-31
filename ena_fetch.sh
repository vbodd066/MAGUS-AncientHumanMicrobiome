#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./ena_fetch.sh [queries.tsv] [out_dir]
#
# Defaults:
#   queries.tsv = queries/queries_ENA.tsv
#   out_dir     = raw_data/ena
#
# queries.tsv must have headers:
#   query_id <TAB> query
#
# Output:
#   raw_data/ena/per_query/<query_id>.read_run.tsv

QUERIES_TSV="${1:-queries/queries_ENA.tsv}"
OUTDIR="${2:-raw_data/ena}"

ENA_URL="https://www.ebi.ac.uk/ena/portal/api/search"
SLEEP_SEC="${SLEEP_SEC:-0.80}"

# ENA Portal API fields (result=read_run) + organism/host fields for later filtering
FIELDS="run_accession,study_accession,secondary_study_accession,experiment_accession,sample_accession,tax_id,scientific_name,host_tax_id,host_scientific_name,sample_title,sample_description,library_strategy,library_source,library_selection,library_layout,instrument_platform,instrument_model,read_count,base_count,first_public,last_updated,fastq_ftp,fastq_md5"

mkdir -p "${OUTDIR}/per_query"

tail -n +2 "${QUERIES_TSV}" | while IFS=$'\t' read -r QUERY_ID QUERY; do
  # Skip empty lines
  [[ -z "${QUERY_ID// }" || -z "${QUERY// }" ]] && continue

  OUTFILE="${OUTDIR}/per_query/${QUERY_ID}.read_run.tsv"
  TMPFILE="${OUTFILE}.tmp"

  echo "Fetching ${QUERY_ID} ..."

  # Fetch into temp file then move on success
  if ! curl -sS --fail \
      --retry 8 --retry-delay 2 --retry-all-errors \
      -H "User-Agent: ena-fetch-script (academic use)" \
      --get \
      --data-urlencode "result=read_run" \
      --data-urlencode "query=${QUERY}" \
      --data-urlencode "fields=${FIELDS}" \
      --data-urlencode "format=tsv" \
      --data-urlencode "limit=0" \
      "${ENA_URL}" > "${TMPFILE}"; then
    echo "  (warning) fetch failed for ${QUERY_ID}"
    rm -f "${TMPFILE}"
    sleep "${SLEEP_SEC}"
    continue
  fi

  # Treat header-only as no data
  if [[ "$(wc -l < "${TMPFILE}" | tr -d ' ')" -le 1 ]]; then
    echo "  (warning) no rows (header-only) for ${QUERY_ID}"
    rm -f "${TMPFILE}"
    sleep "${SLEEP_SEC}"
    continue
  fi

  mv "${TMPFILE}" "${OUTFILE}"
  echo "  -> ${OUTFILE} ($(($(wc -l < "${OUTFILE}") - 1)) rows)"

  sleep "${SLEEP_SEC}"
done

echo "Done. Wrote per-query TSVs to ${OUTDIR}/per_query/"
