#!/usr/bin/env python3
import argparse
import csv
import glob
import os
from collections import defaultdict

def main():
    ap = argparse.ArgumentParser(
        description="Merge ENA per-query read_run TSVs and deduplicate by run_accession."
    )
    ap.add_argument(
        "--per_query_glob", required=True,
        help='Glob for per-query TSVs, e.g. "raw_data/ena/per_query/*.read_run.tsv"'
    )
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument(
        "--dedup_key", default="run_accession",
        help="Column to deduplicate on (default: run_accession)"
    )
    args = ap.parse_args()

    paths = sorted(glob.glob(args.per_query_glob))
    if not paths:
        raise SystemExit("No files matched --per_query_glob")

    os.makedirs(args.outdir, exist_ok=True)

    merged_runs_path = os.path.join(args.outdir, "merged_runs.tsv")
    dedup_runs_path = os.path.join(args.outdir, "dedup_runs.tsv")
    summary_path = os.path.join(args.outdir, "summary.tsv")

    header_ref = None

    total_input_rows = 0
    total_files = 0

    # dedup_key_value -> set(query_ids)  (so you can see which queries produced the run)
    key_to_queries = defaultdict(set)

    # set of runs already written to merged output
    seen = set()

    # per-query summary stats
    per_query_stats = []

    with open(merged_runs_path, "w", newline="", encoding="utf-8") as merged_f:
        merged_writer = None

        for p in paths:
            total_files += 1
            query_id = os.path.basename(p).replace(".read_run.tsv", "")

            with open(p, "r", newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t")
                if not reader.fieldnames:
                    continue

                # Enforce consistent headers across all files
                if header_ref is None:
                    header_ref = reader.fieldnames[:]
                    merged_writer = csv.DictWriter(
                        merged_f,
                        fieldnames=["query_id"] + header_ref,
                        delimiter="\t",
                    )
                    merged_writer.writeheader()
                else:
                    if reader.fieldnames != header_ref:
                        raise SystemExit(
                            f"Header mismatch in {p}\n"
                            f"Expected: {header_ref}\n"
                            f"Found:    {reader.fieldnames}\n"
                            "Make sure all per-query TSVs were fetched with the same fields."
                        )

                raw_rows = 0
                written_rows = 0
                unique_keys_in_file = set()

                for row in reader:
                    raw_rows += 1
                    total_input_rows += 1

                    key_val = (row.get(args.dedup_key) or "").strip()
                    if key_val:
                        key_to_queries[key_val].add(query_id)
                        unique_keys_in_file.add(key_val)

                    # Deduplicate what we write to merged output
                    if not key_val:
                        continue  # skip rows missing run_accession
                    if key_val in seen:
                        continue

                    seen.add(key_val)
                    merged_writer.writerow({"query_id": query_id, **row})
                    written_rows += 1

                per_query_stats.append({
                    "query_id": query_id,
                    "input_rows": raw_rows,
                    "written_rows": written_rows,
                    f"unique_{args.dedup_key}_in_query": len(unique_keys_in_file),
                })

    # Write deduplicated list of run_accession values + which queries they appeared in
    with open(dedup_runs_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([args.dedup_key, "query_ids"])
        for key_val in sorted(key_to_queries.keys()):
            w.writerow([key_val, ",".join(sorted(key_to_queries[key_val]))])

    # Write summary
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["query_id", "input_rows", "written_rows", f"unique_{args.dedup_key}_in_query"]
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(per_query_stats)

    print("DONE")
    print(f"Files processed: {total_files}")
    print(f"Total input rows read: {total_input_rows}")
    print(f"Unique {args.dedup_key} values: {len(key_to_queries)}")
    print(f"Unique rows written (deduped by {args.dedup_key}): {len(seen)}")
    print(f"Wrote: {merged_runs_path}")
    print(f"Wrote: {dedup_runs_path}")
    print(f"Wrote: {summary_path}")

if __name__ == "__main__":
    main()
