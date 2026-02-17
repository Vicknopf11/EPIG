import fitz
from pathlib import Path

PDF_DIR = Path("downloads_dataset1")
OUT_DIR = Path("previews_200")
OUT_DIR.mkdir(exist_ok=True)

pdfs = sorted(PDF_DIR.glob("EFTA*.pdf"))[:200]

for i, p in enumerate(pdfs, start=1):
    doc = fitz.open(p)
    page = doc[0]
    pix = page.get_pixmap(dpi=220)  # leggero
    out = OUT_DIR / f"{p.stem}.png"
    pix.save(out)
    doc.close()

    if i % 25 == 0:
        print(f"Rendered {i}/200")

print("Done:", OUT_DIR.resolve())
