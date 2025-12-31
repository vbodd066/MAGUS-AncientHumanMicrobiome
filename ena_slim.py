#!/usr/bin/env python3
import csv
import gzip
import sys
from pathlib import Path

# Keep only WGS/WGA-like data (exclude amplicon/targeted/WXS),
# require >= MIN_READS, and require library_selection == RANDOM
MIN_READS = 100_000
KEEP_LIBRARY_SELECTION = "RANDOM"
EXCLUDE_STRATEGY_TERMS = (
    "amplicon",
    "wxs",
    "targeted-capture",
    "targeted_capture",
    "targeted",
)

IN_COLS = [
    "study_accession",
    "run_accession",
    "read_count",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "instrument_platform",
    "instrument_model",
]

OUT_COLS = [
    "study_accession",
    "run_accession",
    "read_count",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "instrument_platform",
    "instrument_model",
]

def smart_open(path: Path, mode="rt"):
    p = str(path)
    if p.endswith(".gz"):
        return gzip.open(p, mode, newline="", encoding="utf-8")
    return open(p, mode, newline="", encoding="utf-8")

def sniff_delimiter(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "," in first_line:
        return ","
    return "\t"

def parse_int(x) -> int:
    try:
        x = (x or "").strip()
        if x == "":
            return 0
        return int(float(x))
    except Exception:
        return 0

def norm(s: str) -> str:
    return (s or "").strip().lower()

def is_wgs_or_wga_strategy(strategy: str) -> bool:
    s = norm(strategy)
    if not s:
        return False
    if any(term in s for term in EXCLUDE_STRATEGY_TERMS):
        return False
    return s == "wgs" or s.startswith("wgs") or s == "wga" or s.startswith("wga")

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} ena_input.tsv[.gz] ena_filtered.csv[.gz]", file=sys.stderr)
        sys.exit(2)

    in_path = Path(sys.argv[1]).expanduser().resolve()
    out_path = Path(sys.argv[2]).expanduser().resolve()

    if not in_path.exists():
        print(f"ERROR: Input file not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_in = 0
    rows_out = 0
    skipped_low_reads = 0
    skipped_non_wgs_wga = 0
    skipped_missing_fields = 0
    skipped_non_random = 0

    with smart_open(in_path, "rt") as fin:
        first = fin.readline()
        if not first:
            print("ERROR: Input file is empty.", file=sys.stderr)
            sys.exit(1)
        delim = sniff_delimiter(first)
        fin.seek(0)

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            print("ERROR: Could not read header row.", file=sys.stderr)
            sys.exit(1)

        header_set = set(reader.fieldnames)
        missing = [c for c in IN_COLS if c not in header_set]
        if missing:
            print("ERROR: Missing required columns:", file=sys.stderr)
            for c in missing:
                print(f"  - {c}", file=sys.stderr)
            print("\nFound columns:", file=sys.stderr)
            print(", ".join(reader.fieldnames), file=sys.stderr)
            sys.exit(1)

        with smart_open(out_path, "wt") as fout:
            writer = csv.DictWriter(fout, fieldnames=OUT_COLS)
            writer.writeheader()

            for row in reader:
                rows_in += 1

                study = (row.get("study_accession") or "").strip()
                run = (row.get("run_accession") or "").strip()
                if not study or not run:
                    skipped_missing_fields += 1
                    continue

                read_count = parse_int(row.get("read_count"))
                if read_count < MIN_READS:
                    skipped_low_reads += 1
                    continue

                strategy = (row.get("library_strategy") or "").strip()
                if not is_wgs_or_wga_strategy(strategy):
                    skipped_non_wgs_wga += 1
                    continue

                selection = (row.get("library_selection") or "").strip()
                if norm(selection) != norm(KEEP_LIBRARY_SELECTION):
                    skipped_non_random += 1
                    continue

                writer.writerow({c: (row.get(c) or "").strip() for c in OUT_COLS})
                rows_out += 1

    print(f"Done. Read {rows_in} rows, wrote {rows_out} rows to {out_path}")
    print(f"Skipped (read_count < {MIN_READS}): {skipped_low_reads}")
    print(f"Skipped (not WGS/WGA or excluded strategy terms): {skipped_non_wgs_wga}")
    print(f"Skipped (library_selection != {KEEP_LIBRARY_SELECTION}): {skipped_non_random}")
    if skipped_missing_fields:
        print(f"Skipped (missing study_accession or run_accession): {skipped_missing_fields}")

if __name__ == "__main__":
    main()
