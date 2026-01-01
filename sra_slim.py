#!/usr/bin/env python3
import csv
import gzip
import sys
from pathlib import Path

MIN_SPOTS = 100_000
KEEP_LIBRARY_COMBINED = "WGS/RANDOM"
KEEP_LIBRARY_SOURCE = "METAGENOMIC"
KEEP_SCIENTIFIC_NAME = "Homo sapiens"

def smart_open(path: Path, mode="rt"):
    p = str(path)
    if p.endswith(".gz"):
        return gzip.open(p, mode, newline="", encoding="utf-8")
    return open(p, mode, newline="", encoding="utf-8")

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

def combined_strategy(row: dict) -> str:
    """
    If LibraryStrategy already looks like 'WGS/RANDOM', keep it.
    Otherwise combine LibraryStrategy + LibrarySelection -> 'WGS/RANDOM'
    """
    strat = (row.get("LibraryStrategy") or "").strip()
    sel = (row.get("LibrarySelection") or "").strip()

    if "/" in strat:
        return strat
    if sel:
        return f"{strat}/{sel}" if strat else sel
    return strat

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} input.csv output.csv", file=sys.stderr)
        return 2

    in_path = Path(sys.argv[1]).expanduser().resolve()
    out_path = Path(sys.argv[2]).expanduser().resolve()

    print(f"[INFO] Input:  {in_path}")
    print(f"[INFO] Output: {out_path}")

    if not in_path.exists():
        print(f"[ERROR] Input file not found: {in_path}", file=sys.stderr)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_in = 0
    rows_out = 0

    skipped_missing_run = 0
    skipped_missing_bioproject = 0
    skipped_low_spots = 0
    skipped_non_hsapiens = 0
    skipped_non_matching_strategy = 0
    skipped_non_metagenomic = 0

    with smart_open(in_path, "rt") as fin:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            print("[ERROR] Could not read CSV header (empty file?)", file=sys.stderr)
            return 1

        required = {
            "Run", "BioProject", "spots",
            "LibraryStrategy", "LibrarySelection", "LibrarySource",
            "ScientificName"
        }
        missing_cols = sorted(required - set(reader.fieldnames))
        if missing_cols:
            print("[ERROR] Input CSV is missing required column(s): " + ", ".join(missing_cols), file=sys.stderr)
            print("[INFO] Found columns:", ", ".join(reader.fieldnames), file=sys.stderr)
            return 1

        with smart_open(out_path, "wt") as fout:
            out_fields = [
                "BioProject",
                "Run",
                "spots",
                "spots_with_mates",
                "SeqType",
                "SequencingMachine",
                "ScientificName",
                "LibraryStrategy",
                "LibrarySource",
            ]
            writer = csv.DictWriter(fout, fieldnames=out_fields)
            writer.writeheader()

            for row in reader:
                rows_in += 1

                run_id = (row.get("Run") or "").strip()
                if not run_id:
                    skipped_missing_run += 1
                    continue

                bioproject = (row.get("BioProject") or "").strip()
                if not bioproject:
                    skipped_missing_bioproject += 1
                    continue

                spots_val = parse_int(row.get("spots"))
                if spots_val < MIN_SPOTS:
                    skipped_low_spots += 1
                    continue

                sci_name = (row.get("ScientificName") or "").strip()
                if norm(sci_name) != norm(KEEP_SCIENTIFIC_NAME):
                    skipped_non_hsapiens += 1
                    continue

                lib_source = (row.get("LibrarySource") or "").strip()
                if norm(lib_source) != norm(KEEP_LIBRARY_SOURCE):
                    skipped_non_metagenomic += 1
                    continue

                lib_combined = combined_strategy(row)
                if norm(lib_combined) != norm(KEEP_LIBRARY_COMBINED):
                    skipped_non_matching_strategy += 1
                    continue

                spots = (row.get("spots") or "").strip()
                spots_with_mates = (row.get("spots_with_mates") or "").strip()

                layout_raw = (row.get("LibraryLayout") or "").strip().upper()
                seq_type = "paired" if layout_raw == "PAIRED" else "single"

                model = (row.get("Model") or "").strip()
                platform = (row.get("Platform") or "").strip()
                machine = model if model else platform

                writer.writerow({
                    "BioProject": bioproject,
                    "Run": run_id,
                    "spots": spots,
                    "spots_with_mates": spots_with_mates,
                    "SeqType": seq_type,
                    "SequencingMachine": machine,
                    "ScientificName": sci_name,
                    "LibraryStrategy": lib_combined,   # e.g., WGS/RANDOM
                    "LibrarySource": lib_source,       # METAGENOMIC
                })
                rows_out += 1

    print(f"[DONE] Read {rows_in} rows, wrote {rows_out} rows.")
    print(f"[DONE] Skipped missing BioProject: {skipped_missing_bioproject}")
    print(f"[DONE] Skipped missing Run: {skipped_missing_run}")
    print(f"[DONE] Skipped spots < {MIN_SPOTS}: {skipped_low_spots}")
    print(f"[DONE] Skipped ScientificName != '{KEEP_SCIENTIFIC_NAME}': {skipped_non_hsapiens}")
    print(f"[DONE] Skipped LibrarySource != '{KEEP_LIBRARY_SOURCE}': {skipped_non_metagenomic}")
    print(f"[DONE] Skipped Library != '{KEEP_LIBRARY_COMBINED}': {skipped_non_matching_strategy}")
    print(f"[DONE] Output exists? {out_path.exists()} (size={out_path.stat().st_size if out_path.exists() else 'NA'})")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
