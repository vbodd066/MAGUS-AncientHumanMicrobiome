#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./ena_fetch.sh queries.tsv ena_out
#
# queries.tsv must have headers:
#   query_id <TAB> query
#
# Output:
#   ena_out/per_query/<query_id>.read_run.tsv

QUERIES_TSV="${1:?Provide queries_ENA.tsv}"
OUTDIR="${2:-ena_out}"

ENA_URL="https://www.ebi.ac.uk/ena/portal/api/search"

# ENA Portal API fields (result=read_run):
FIELDS="run_accession,study_accession,secondary_study_accession,experiment_accession,sample_accession,library_strategy,library_source,library_selection,library_layout,instrument_platform,instrument_model,read_count,base_count,first_public,last_updated,fastq_ftp,fastq_md5"

mkdir -p "${OUTDIR}/per_query"

# Sequential loop (no parallelism) + fixed sleep after each query
tail -n +2 "${QUERIES_TSV}" | while IFS=$'\t' read -r QUERY_ID QUERY; do
  # Skip empty lines
  [[ -z "${QUERY_ID// }" || -z "${QUERY// }" ]] && continue

  OUTFILE="${OUTDIR}/per_query/${QUERY_ID}.read_run.tsv"

  echo "Fetching ${QUERY_ID} ..."
  curl -sS --fail \
    --retry 6 --retry-delay 2 --retry-all-errors \
    -H "User-Agent: ena-fetch-script (academic use)" \
    --get \
    --data-urlencode "result=read_run" \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "fields=${FIELDS}" \
    --data-urlencode "format=tsv" \
    --data-urlencode "limit=0" \
    "${ENA_URL}" > "${OUTFILE}"

  sleep 0.80
done

echo "Done. Wrote per-query TSVs to ${OUTDIR}/per_query/"

