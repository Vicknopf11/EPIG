import re
from pathlib import Path
import csv

DATASET_DIR = Path("/Users/ferrucciobottoni/Desktop/EPICOSO/downloads_dataset1")

# Seed che mi hai dato
NY_SHOOT1 = (1, 836)
NY_SHOOT2 = (837, 1499)
LSJ_START = 1500

EFTA_RE = re.compile(r"EFTA(\d{8})", re.IGNORECASE)

def compress_ranges(nums):
    """Converte una lista di interi in range compatti [(start,end), ...]."""
    if not nums:
        return []
    nums = sorted(nums)
    ranges = []
    s = e = nums[0]
    for x in nums[1:]:
        if x == e + 1:
            e = x
        else:
            ranges.append((s, e))
            s = e = x
    ranges.append((s, e))
    return ranges

def fmt_efta(n: int) -> str:
    return f"EFTA{n:08d}"

def main():
    pdfs = list(DATASET_DIR.glob("EFTA*.pdf"))
    ids = []
    bad = []

    for f in pdfs:
        m = EFTA_RE.search(f.name)
        if not m:
            bad.append(f.name)
            continue
        ids.append(int(m.group(1)))

    ids = sorted(set(ids))
    if not ids:
        print("Nessun file EFTA########.pdf trovato.")
        return

    s = set(ids)
    min_id, max_id = ids[0], ids[-1]

    # Buchi globali tra min e max
    missing_global = [i for i in range(min_id, max_id + 1) if i not in s]
    missing_global_ranges = compress_ranges(missing_global)

    # Buchi NY shoot1 e shoot2 (se min/max coprono)
    missing_ny1 = [i for i in range(NY_SHOOT1[0], NY_SHOOT1[1] + 1) if i not in s]
    missing_ny2 = [i for i in range(NY_SHOOT2[0], NY_SHOOT2[1] + 1) if i not in s]

    # LSJ: buchi solo nel tratto presente (da 1500 a max)
    if max_id >= LSJ_START:
        missing_lsj = [i for i in range(LSJ_START, max_id + 1) if i not in s]
    else:
        missing_lsj = []

    # Report console
    print("=== DATASET1 GAP REPORT ===")
    print(f"Cartella: {DATASET_DIR}")
    print(f"File PDF trovati (pattern EFTA*.pdf): {len(pdfs)}")
    print(f"ID EFTA validi: {len(ids)}")
    print(f"Min ID: {fmt_efta(min_id)}  Max ID: {fmt_efta(max_id)}")
    print(f"Buchi globali (count): {len(missing_global)}  Range mancanti: {len(missing_global_ranges)}")

    def show_block(name, missing):
        rngs = compress_ranges(missing)
        print(f"\n-- {name} --")
        print(f"Missing count: {len(missing)}  Missing ranges: {len(rngs)}")
        if rngs:
            preview = rngs[:10]
            for a,b in preview:
                if a == b:
                    print(f"  {fmt_efta(a)}")
                else:
                    print(f"  {fmt_efta(a)}–{fmt_efta(b)}")
            if len(rngs) > 10:
                print(f"  ... +{len(rngs)-10} altri range")

    show_block("NY shoot1 (1–836)", missing_ny1)
    show_block("NY shoot2 (837–1499)", missing_ny2)
    show_block(f"LSJ (1500–{max_id})", missing_lsj)

    if bad:
        print("\nNomi file non parsabili (non matchano EFTA########):")
        for x in bad[:20]:
            print(" ", x)
        if len(bad) > 20:
            print(f" ... +{len(bad)-20}")

    # CSV output
    out_csv = DATASET_DIR.parent / "dataset1_missing_ranges.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["block", "start_efta", "end_efta", "count_missing"])
        def write_block(block_name, miss):
            rngs = compress_ranges(miss)
            for a,b in rngs:
                w.writerow([block_name, fmt_efta(a), fmt_efta(b), (b - a + 1)])
        write_block("GLOBAL", missing_global)
        write_block("NY_SHOOT1", missing_ny1)
        write_block("NY_SHOOT2", missing_ny2)
        write_block("LSJ", missing_lsj)

    print(f"\nCSV scritto in: {out_csv}")

if __name__ == "__main__":
    main()
