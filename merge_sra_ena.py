#!/usr/bin/env python3
import csv
import gzip
import sys
from pathlib import Path

OUT_FIELDS = [
    "project_accession",
    "run_accession",
    "spots_or_reads",
    "spots_with_mates",
    "library_layout",
    "seq_type",
    "instrument_platform",
    "instrument_model",
    "sequencing_machine",
    "scientific_name",
    "library_strategy",
    "library_selection",
    "library_source",
    "source_db",
]

def smart_open(path: Path, mode="rt"):
    p = str(path)
    if p.endswith(".gz"):
        return gzip.open(p, mode, newline="", encoding="utf-8")
    return open(p, mode, newline="", encoding="utf-8")

def sniff_delimiter_from_header_line(line: str) -> str:
    if "\t" in line:
        return "\t"
    if "," in line:
        return ","
    # default for ENA-like exports
    return "\t"

def norm(s: str) -> str:
    return (s or "").strip()

def lower(s: str) -> str:
    return (s or "").strip().lower()

def split_strategy_selection(strategy: str):
    """
    If strategy looks like 'WGS/RANDOM' return ('WGS','RANDOM').
    Else return (strategy,'').
    """
    s = norm(strategy)
    if "/" in s:
        a, b = s.split("/", 1)
        return a.strip(), b.strip()
    return s, ""

def infer_seq_type_from_layout(layout: str) -> str:
    l = (layout or "").strip().upper()
    if l == "PAIRED":
        return "paired"
    if l == "SINGLE":
        return "single"
    return ""

def merge_files(ena_tsv: Path, sra_csv: Path, out_csv: Path, dedup_by_run: bool = True) -> int:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    seen_runs = set()
    written = 0
    dropped_dupes = 0

    with smart_open(out_csv, "wt") as fout:
        writer = csv.DictWriter(fout, fieldnames=OUT_FIELDS)
        writer.writeheader()

        # --- SRA ---
        with smart_open(sra_csv, "rt") as fin:
            header = fin.readline()
            if not header:
                raise SystemExit(f"ERROR: SRA file empty: {sra_csv}")
            delim = sniff_delimiter_from_header_line(header)
            fin.seek(0)

            reader = csv.DictReader(fin, delimiter=delim)
            if not reader.fieldnames:
                raise SystemExit(f"ERROR: Could not read SRA header: {sra_csv}")

            required = {"BioProject", "Run", "spots", "ScientificName", "LibraryStrategy", "LibrarySource"}
            missing = sorted(required - set(reader.fieldnames))
            if missing:
                raise SystemExit(f"ERROR: SRA missing columns {missing} in {sra_csv}")

            for row in reader:
                run = norm(row.get("Run"))
                if not run:
                    continue

                if dedup_by_run and run in seen_runs:
                    dropped_dupes += 1
                    continue

                strat_combined = norm(row.get("LibraryStrategy"))
                strat, sel = split_strategy_selection(strat_combined)

                out_row = {
                    "source_db": "SRA",
                    "project_accession": norm(row.get("BioProject")),
                    "run_accession": run,
                    "spots_or_reads": norm(row.get("spots")),
                    "spots_with_mates": norm(row.get("spots_with_mates")),
                    "library_layout": "",  # not present in your SRA-slim file
                    "seq_type": norm(row.get("SeqType")),
                    "instrument_platform": "",
                    "instrument_model": "",
                    "sequencing_machine": norm(row.get("SequencingMachine")),
                    "scientific_name": norm(row.get("ScientificName")),
                    "library_strategy": strat,
                    "library_selection": sel,
                    "library_source": norm(row.get("LibrarySource")),
                }

                writer.writerow(out_row)
                seen_runs.add(run)
                written += 1

        # --- ENA ---
        with smart_open(ena_tsv, "rt") as fin:
            header = fin.readline()
            if not header:
                raise SystemExit(f"ERROR: ENA file empty: {ena_tsv}")
            delim = sniff_delimiter_from_header_line(header)
            fin.seek(0)

            reader = csv.DictReader(fin, delimiter=delim)
            if not reader.fieldnames:
                raise SystemExit(f"ERROR: Could not read ENA header: {ena_tsv}")

            required = {"study_accession", "run_accession", "read_count", "library_strategy",
                        "library_source", "library_selection", "library_layout",
                        "instrument_platform", "instrument_model", "scientific_name"}
            missing = sorted(required - set(reader.fieldnames))
            if missing:
                raise SystemExit(f"ERROR: ENA missing columns {missing} in {ena_tsv}")

            for row in reader:
                run = norm(row.get("run_accession"))
                if not run:
                    continue

                if dedup_by_run and run in seen_runs:
                    dropped_dupes += 1
                    continue

                layout = norm(row.get("library_layout"))
                platform = norm(row.get("instrument_platform"))
                model = norm(row.get("instrument_model"))
                machine = model if model else platform

                out_row = {
                    "source_db": "ENA",
                    "project_accession": norm(row.get("study_accession")),
                    "run_accession": run,
                    "spots_or_reads": norm(row.get("read_count")),
                    "spots_with_mates": "",  # ENA minimal doesn’t include this
                    "library_layout": layout,
                    "seq_type": infer_seq_type_from_layout(layout),
                    "instrument_platform": platform,
                    "instrument_model": model,
                    "sequencing_machine": machine,
                    "scientific_name": norm(row.get("scientific_name")),
                    "library_strategy": norm(row.get("library_strategy")),
                    "library_selection": norm(row.get("library_selection")),
                    "library_source": norm(row.get("library_source")),
                }

                writer.writerow(out_row)
                seen_runs.add(run)
                written += 1

    print(f"[DONE] Wrote {written} rows -> {out_csv}")
    if dedup_by_run:
        print(f"[DONE] Dedup by run_accession: dropped {dropped_dupes} duplicate rows")
    return 0

def main():
    if len(sys.argv) < 4:
        print("Usage: merge_ena_sra.py ena_filtered.tsv sra_filtered.csv merged.csv", file=sys.stderr)
        return 2

    ena_path = Path(sys.argv[1]).expanduser().resolve()
    sra_path = Path(sys.argv[2]).expanduser().resolve()
    out_path = Path(sys.argv[3]).expanduser().resolve()

    if not ena_path.exists():
        print(f"ERROR: ENA input not found: {ena_path}", file=sys.stderr)
        return 1
    if not sra_path.exists():
        print(f"ERROR: SRA input not found: {sra_path}", file=sys.stderr)
        return 1

    return merge_files(ena_path, sra_path, out_path, dedup_by_run=True)

if __name__ == "__main__":
    raise SystemExit(main())
