import re
import csv
from pathlib import Path
import fitz

PDF_DIR = Path("downloads_dataset1")
OUT_CSV = Path("fields_200.csv")

date_re = re.compile(r"\bDATE\b[^\w]{0,10}([0-9A-Za-z\-/_. ]{3,20})", re.IGNORECASE)
case_re = re.compile(r"\bCASE\s*ID\b[^\w]{0,10}([0-9A-Za-z\-/_. ]{3,40})", re.IGNORECASE)
phot_re = re.compile(r"\bPHOTOGRAPHER\b[^\w]{0,10}([0-9A-Za-z\-/_. ]{2,60})", re.IGNORECASE)
loc_re  = re.compile(r"\bLOCATION\b[^\w]{0,10}([0-9A-Za-z\-/_. ]{2,60})", re.IGNORECASE)

def first_match(rx, text):
    m = rx.search(text)
    return m.group(1).strip() if m else ""

rows = []
pdfs = sorted(PDF_DIR.glob("EFTA*.pdf"))

for p in pdfs:
    doc = fitz.open(p)
    # prendi solo la prima pagina per ora (più veloce)
    txt = doc[0].get_text("text") or ""
    doc.close()

    clean = " ".join(txt.split())  # normalizza spazi
    rows.append({
        "file": p.name,
        "date": first_match(date_re, clean),
        "case_id": first_match(case_re, clean),
        "photographer": first_match(phot_re, clean),
        "location": first_match(loc_re, clean),
        "text_len": len(clean),
        "text_sample": clean[:120],
    })

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)

print("Saved:", OUT_CSV.resolve())

# mini statistiche
with_date = sum(1 for r in rows if r["date"])
with_case = sum(1 for r in rows if r["case_id"])
with_phot = sum(1 for r in rows if r["photographer"])
with_loc  = sum(1 for r in rows if r["location"])
print("Rows:", len(rows))
print("Has DATE:", with_date)
print("Has CASE ID:", with_case)
print("Has PHOTOGRAPHER:", with_phot)
print("Has LOCATION:", with_loc)
