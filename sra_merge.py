#!/usr/bin/env python3
import csv
import glob
import os
import sys

# Usage:
#   python3 merge_runs.py sra_out_runinfo sra_merged_runs.csv
#
# Deduplicates by Run accession (keeps the first row seen per Run).
# Keeps ALL runs associated with each BioProject (i.e., does NOT dedup by BioProject).
# Ignores any columns that are not in the first file's header.

def main():
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "sra_out_runinfo"
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "sra_merged_runs.csv"

    files = sorted(glob.glob(os.path.join(input_dir, "*.runinfo.csv")))

    if not files:
        print(f"ERROR: no *.runinfo.csv files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    header = None
    seen_runs = set()
    wrote = 0
    total_rows_read = 0
    skipped_no_run = 0

    with open(output_csv, "w", newline="", encoding="utf-8") as out_f:
        writer = None

        for fp in files:
            with open(fp, "r", newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue

                if writer is None:
                    header = reader.fieldnames
                    if "Run" not in header:
                        print(f"ERROR: Run column not found in {fp}", file=sys.stderr)
                        sys.exit(1)
                    # BioProject isn't required for dedup anymore, but it's still expected to exist in RunInfo
                    writer = csv.DictWriter(out_f, fieldnames=header)
                    writer.writeheader()

                for row in reader:
                    total_rows_read += 1
                    run = (row.get("Run") or "").strip()
                    if not run:
                        skipped_no_run += 1
                        continue
                    if run in seen_runs:
                        continue

                    seen_runs.add(run)
                    writer.writerow(row)
                    wrote += 1

    print(f"total rows read: {total_rows_read}")
    print(f"Input files: {len(files)}")
    print(f"Unique Runs written: {wrote}")
    if skipped_no_run:
        print(f"Rows skipped (missing Run): {skipped_no_run}")
    print(f"Output: {output_csv}")

if __name__ == "__main__":
    main()
